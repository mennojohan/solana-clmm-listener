"""WebSocket Listener for Orca Whirlpool Pools (Solana)

Monitors real-time pool account state updates and program logs for swaps and LP events,
persisting decoded pool states and swap deltas to PostgreSQL."""

import asyncio
import base64
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from solana.exceptions import SolanaRpcException
from solana.rpc.async_api import AsyncClient
from solana.rpc.websocket_api import connect
from solders.account import Account
from solders.pubkey import Pubkey
from solders.rpc.config import RpcTransactionLogsFilterMentions
from solders.rpc.responses import AccountNotification, SubscriptionResult, LogsNotification
from solders.signature import Signature

from database import get_database
from decoder import decode_pool_state, calculate_price_from_sqrt

load_dotenv()

MAX_CONCURRENT_RPC = int(os.getenv("MAX_CONCURRENT_RPC", "3"))
RPC_DELAY_SECONDS = float(os.getenv("RPC_DELAY_SECONDS", "0.1"))

# Configure standard logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Constants
RPC_ENDPOINT = os.getenv("RPC_ENDPOINT")
if not RPC_ENDPOINT:
    raise ValueError("RPC_ENDPOINT is not set! Please check your .env file.")

WHIRLPOOL_ADDRESS = Pubkey.from_string(
    os.getenv("WHIRLPOOL_ADDRESS", "HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ")
)
WHIRLPOOL_PROGRAM_ID = Pubkey.from_string("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc")  # Orca Whirlpool Program
COMMITMENT = "confirmed"
ENCODING = "base64"
SOL_DECIMALS = 9
USDC_DECIMALS = 6
MAX_RECONNECT_ATTEMPTS = 10
INITIAL_BACKOFF = 1  # seconds
MAX_BACKOFF = 60  # seconds

class WhirlpoolListener:
    """
    Streams account updates and program logs for an Orca Whirlpool via WebSocket,
    resolving swap vault deltas via RPC and storing state snapshots in PostgreSQL.
    """
    
    def __init__(self, db_instance):
        self.db = db_instance
        self.last_seen_slot: Optional[int] = None
        self.reconnect_attempts = 0
        self.running = False
        self.websocket = None
        self.subscription_slot: Optional[int] = None
    
        # Track subscription IDs for dual subscriptions
        self.account_subscription_id: Optional[int] = None
        self.logs_subscription_id: Optional[int] = None
    
        # Track background tasks for clean shutdown
        self._processing_tasks: set[asyncio.Task] = set()

        # HTTP RPC client for transaction fetching
        self.rpc_client: Optional[AsyncClient] = None

        # Limit concurrent HTTP RPC requests
        self._rpc_semaphore = asyncio.Semaphore(MAX_CONCURRENT_RPC)

        # Pool state caching (for swap metadata correlation)
        self.latest_pool_state: Dict[str, Any] = {}
        self.previous_pool_state: Dict[str, Any] = {}

    def set_database(self, db) -> None:
        
        self.db = db
        logger.info("✅ Database instance injected into listener")

    def _extract_pubkey(self, acc) -> str:
        return str(acc.pubkey) if hasattr(acc, "pubkey") else str(acc)

    def _parse_notification(self, raw_message: Any) -> Optional[Dict[str, Any]]:
        try:
            # Step 1: Unpack from transport list wrapper
            if isinstance(raw_message, list):
                if not raw_message:
                    logger.warning("Received empty WebSocket list.")
                    return None
                raw_message = raw_message[0]  

            # Step 2: Resilient type guard 
            is_account_notification = False
            
            # Primary check: isinstance with solders type
            if isinstance(raw_message, AccountNotification):
                is_account_notification = True
            # Fallback: duck-typing for cross-version compatibility
            elif hasattr(raw_message, 'result') and hasattr(raw_message.result, 'context'):
                # Check if it has account-like structure (has .value with .data)
                if hasattr(raw_message.result, 'value'):
                    value = raw_message.result.value
                    if hasattr(value, 'data') and not hasattr(value, 'logs'):
                        is_account_notification = True
            
            if not is_account_notification:
                return None
            
            # Step 3: Extract structured data
            notification_result = raw_message.result
            
            # Extract slot from context
            slot = notification_result.context.slot
            
            # Extract account data bytes from value
            account_obj = notification_result.value
            account_bytes = account_obj.data
            
            return {
                'slot': slot,
                'data': account_bytes  # Raw bytes for decoder
            }
            
        except AttributeError as e:
            logger.error(f"💥 Structure error in AccountNotification: {e}")
            logger.debug(f"Raw structure parsing failed: {raw_message}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing account notification: {e}")
            return None
    
    def _parse_log_notification(self, raw_message: Any) -> Optional[Dict[str, Any]]:
        try:
            # Step 1: Unpack from transport list wrapper
            if isinstance(raw_message, list):
                if not raw_message:
                    logger.warning("Received empty WebSocket list for logs.")
                    return None
                raw_message = raw_message[0]

            # Step 2: Resilient type guard - check LogsNotification
            is_logs_notification = False
            
            # Primary check: isinstance with solders type
            if isinstance(raw_message, LogsNotification):
                is_logs_notification = True
            # Fallback: duck-typing for cross-version compatibility
            elif hasattr(raw_message, 'result') and hasattr(raw_message.result, 'context'):
                # Check if it has logs-like structure (has .value with .logs)
                if hasattr(raw_message.result, 'value'):
                    value = raw_message.result.value
                    if hasattr(value, 'logs') or hasattr(value, 'signature'):
                        is_logs_notification = True
            
            if not is_logs_notification:
                # Not a logs notification - could be account or subscription response
                return None
            
            # Step 3: Extract structured data from LogsNotification
            notification_result = raw_message.result
            
            # Extract slot from context
            slot = notification_result.context.slot
            
            # Extract value (contains signature and logs)
            log_value = notification_result.value
            
            # Get signature - handle different possible formats
            signature = "unknown"
            if hasattr(log_value, 'signature'):
                signature = str(log_value.signature)
            elif isinstance(log_value, dict) and 'signature' in log_value:
                signature = str(log_value['signature'])
            
            # Get logs - handle different possible formats
            logs = []
            if hasattr(log_value, 'logs'):
                logs = log_value.logs
            elif isinstance(log_value, dict) and 'logs' in log_value:
                logs = log_value['logs']
            
            # Normalize logs to list of strings
            if not isinstance(logs, list):
                logs = [str(logs)] if logs else []
            else:
                logs = [str(log) for log in logs]
            
            # Filter for target instructions
            instruction_type = None
            for log_line in logs:
                if "Instruction: Swap" in log_line:
                    instruction_type = "Swap"
                    break
                elif "Instruction: OpenPosition" in log_line:
                    instruction_type = "OpenPosition"
                    break
                elif "Instruction: IncreaseLiquidity" in log_line:
                    instruction_type = "IncreaseLiquidity"
                    break
                elif "Instruction: DecreaseLiquidity" in log_line:
                    instruction_type = "DecreaseLiquidity"
                    break
            
            # Only return if we found a relevant instruction
            if instruction_type is None:
                return None
            
            return {
                'slot': slot,
                'signature': signature,
                'logs': logs,
                'instruction_type': instruction_type
            }
            
        except AttributeError as e:
            logger.error(f"💥 Structure error in LogsNotification: {e}")
            logger.debug(f"Raw logs structure parsing failed: {raw_message}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing logs notification: {e}")
            return None
    
    async def process_update(self, parsed_data: Dict[str, Any]) -> None:
        try:
            slot = parsed_data['slot']
        
            if self.last_seen_slot is not None:
                if slot < self.last_seen_slot:
                    logger.critical(f"Out-of-order slot detected! Current: {slot}, Last: {self.last_seen_slot}")
                    return
        
            # Update slot tracker after validation
            if slot > (self.last_seen_slot or 0):
                self.last_seen_slot = slot

            # ✅ STREAMLINED DATA EXTRACTION
            # The parsed_data['data'] should already be bytes from _parse_notification
            raw_data = parsed_data['data']
            
            # Handle different data formats
            if isinstance(raw_data, list):
                # Format: [base64_string, encoding]
                base64_data = raw_data[0]
                account_data = base64.b64decode(base64_data)
            elif isinstance(raw_data, str):
                # Raw base64 string
                account_data = base64.b64decode(raw_data)
            elif isinstance(raw_data, bytes):
                # ✅ Pre-decoded bytes (most common with solders.account.Account)
                account_data = raw_data
            else:
                logger.error(f"Unsupported data format: {type(raw_data)}")
                return
            
            # ✅ CLEAN HANDOFF TO DECODER
            # Pass raw bytes directly - decoder handles all binary deserialization
            pool_state = decode_pool_state(account_data)
            
            # Calculate human-readable price
            price = calculate_price_from_sqrt(
                pool_state["sqrt_price"],
                decimals_a=SOL_DECIMALS,
                decimals_b=USDC_DECIMALS
            )
            
            fee_growth_a = f"{pool_state['fee_growth_global_a']:,}"
            
            # ✅ MILESTONE OUTPUT: Print to terminal
            logger.info(
                f"Slot {slot:>9} | "
                f"Price: ${price:>8,.2f} | "
                f"Tick: {pool_state['tick_current_index']:>7} | "
                f"Liquidity: {pool_state['liquidity']:>15,} | "
                f"FeeGrowthA: {fee_growth_a}"
            )

            # ✅ STATE CACHING: Store pool state for swap correlation
            # Shift current state to previous before updating
            if self.latest_pool_state:
                self.previous_pool_state = self.latest_pool_state.copy()

            self.latest_pool_state = {
                'slot': slot,
                'sqrt_price': pool_state['sqrt_price'],
                'tick_current_index': pool_state['tick_current_index'],
                'liquidity': pool_state['liquidity'],
                'fee_growth_global_a': pool_state['fee_growth_global_a'],
                'fee_growth_global_b': pool_state.get('fee_growth_global_b', 0),
            }
            
        except Exception as e:
            logger.error(f"Error processing update: {e}", exc_info=True)
    
    async def process_swap_event(self, slot: int, signature: str, logs: List[str]) -> None:
        logger.info(
            f"🔄 SWAP DETECTED | "
            f"Slot: {slot} | "
            f"Sig: {signature[:16]}..."
        )

        # Spawn non-blocking background task for transaction fetching
        task = asyncio.create_task(self._fetch_and_insert_swap(slot, signature))
        self._processing_tasks.add(task)
        task.add_done_callback(self._processing_tasks.discard)

    async def _fetch_and_insert_swap(self, slot: int, signature: str) -> None:
        """Fetch transaction data, decode swap details, and insert into database."""
        async with self._rpc_semaphore:
            try:
                await self._fetch_and_insert_swap_inner(slot, signature)
            finally:
                if RPC_DELAY_SECONDS > 0:
                    await asyncio.sleep(RPC_DELAY_SECONDS)

    async def _fetch_and_insert_swap_inner(self, slot: int, signature: str) -> None:
        try:
            # Guard: Database must be connected
            if self.db is None:
                logger.warning(f"⚠️ Database not connected, skipping swap insert for {signature[:16]}...")
                return
            
            # Guard: RPC client must be initialized
            if self.rpc_client is None:
                logger.error(f"❌ RPC client not initialized, cannot fetch swap {signature[:16]}...")
                return
            
            # Parse signature
            sig_obj = Signature.from_string(signature)
            
            # Retry loop (WebSocket can arrive before HTTP RPC finalizes)
            tx_resp = None
            for attempt in range(1, 5):
                try:
                    tx_resp = await self.rpc_client.get_transaction(
                        sig_obj,
                        encoding="jsonParsed",
                        commitment="confirmed",
                        max_supported_transaction_version=1
                    )
                    
                    if tx_resp and tx_resp.value:
                        break
                        
                    logger.debug(f"Attempt {attempt}/4: Transaction not yet available, retrying...")
                    await asyncio.sleep(0.4 * attempt)  # Exponential backoff
                    
                except Exception as e:
                    logger.warning(f"Attempt {attempt}/4 failed: {e}")
                    if attempt < 4:
                        await asyncio.sleep(0.4 * attempt)
            
            # Guard: Null transaction check
            if not tx_resp or not tx_resp.value:
                logger.warning(f"⚠️ Transaction not found after retries: {signature[:16]}...")
                return
            
            tx_value = tx_resp.value
            
            # Guard: Check for failed transactions
            if tx_value.transaction.meta.err is not None:
                logger.debug(f"⏭️ Skipping failed transaction: {signature[:16]}...")
                return
            
            meta = tx_value.transaction.meta
            message = tx_value.transaction.transaction.message
            
            # Sanity check: Verify pool address is in account keys 
            account_keys = message.account_keys
            all_keys = [self._extract_pubkey(acc) for acc in account_keys]
            if hasattr(meta, "loaded_addresses") and meta.loaded_addresses:
                all_keys.extend(str(addr) for addr in meta.loaded_addresses.writable)
                all_keys.extend(str(addr) for addr in meta.loaded_addresses.readonly)

            pool_address_str = str(WHIRLPOOL_ADDRESS)
            if pool_address_str not in all_keys:
                logger.critical(f"🚨 RPC inconsistency: Pool address not in account_keys for {signature[:16]}...")
                return

            # Extract trader (fee payer - first account)
            trader = self._extract_pubkey(account_keys[0])
            
            # Extract token balance changes
            amount_in, amount_out, token_in, token_out = await self._extract_swap_amounts(
                meta.pre_token_balances,
                meta.post_token_balances,
                trader
            )
            
            # Guard: Validate extraction success
            if amount_in == 0 or amount_out == 0:
                logger.warning(f"⚠️ Could not extract swap amounts for {signature[:16]}...")
                return
            
            # Extract pool state from cached data (prefer latest, fallback to previous)
            pool_state = self.latest_pool_state if self.latest_pool_state else self.previous_pool_state
            
            if not pool_state:
                logger.warning(f"⚠️ No pool state cached yet, skipping swap {signature[:16]}...")
                return
            
            sqrt_price_after = pool_state['sqrt_price']
            liquidity = pool_state['liquidity']
            tick_after = pool_state['tick_current_index']
            
            # Extract fee and block time
            fee_amount = meta.fee
            block_time = tx_value.block_time if tx_value.block_time else int(time.time())
            
            # Insert into database
            await self.db.insert_swap(
                pool_address=pool_address_str,
                signature=signature,
                trader=trader,
                amount_in=amount_in,
                amount_out=amount_out,
                token_in=token_in,
                token_out=token_out,
                sqrt_price_after=sqrt_price_after,
                liquidity=liquidity,
                tick_after=tick_after,
                fee_amount=fee_amount,
                slot=slot,
                block_time=block_time,
            )
            
            logger.info(
                f"✅ SWAP INSERTED | "
                f"Sig: {signature[:16]}... | "
                f"In: {amount_in} {token_in[:8]}... | "
                f"Out: {amount_out} {token_out[:8]}..."
            )
            
        except Exception as e:
            logger.error(f"Error processing swap {signature[:16]}...: {e}", exc_info=True)

    async def _extract_swap_amounts(
        self,
        pre_balances: List[Any],
        post_balances: List[Any],
        trader: str
    ) -> Tuple[int, int, str, str]:
        try:
            # Build balance maps: {(account_index, mint): (amount, owner)}
            pre_map: Dict[Tuple[int, str], Tuple[int, str]] = {}
            post_map: Dict[Tuple[int, str], Tuple[int, str]] = {}
        
            for bal in pre_balances:
                key = (bal.account_index, str(bal.mint))
                amount = int(bal.ui_token_amount.amount)
                owner = str(bal.owner)
                pre_map[key] = (amount, owner)
        
            for bal in post_balances:
                key = (bal.account_index, str(bal.mint))
                amount = int(bal.ui_token_amount.amount)
                owner = str(bal.owner)
                post_map[key] = (amount, owner)
        
            # Find pool vault balance changes
            # Pool vaults are owned by the Whirlpool account itself
            pool_address_str = str(WHIRLPOOL_ADDRESS)
            vault_changes: List[Tuple[str, int]] = []  # (mint, diff)
        
            all_keys = set(pre_map.keys()) | set(post_map.keys())
        
            for key in all_keys:
                pre_amount, pre_owner = pre_map.get(key, (0, ""))
                post_amount, post_owner = post_map.get(key, (0, ""))
            
                # Only track accounts owned by the pool (vault accounts)
                owner = post_owner or pre_owner
                if owner != pool_address_str:
                    continue
            
                diff = post_amount - pre_amount
            
                if diff != 0:
                    mint = key[1]
                    vault_changes.append((mint, diff))
        
            # Validate we found exactly 2 vault changes (tokenVaultA and tokenVaultB)
            if len(vault_changes) != 2:
                logger.warning(
                    f"⚠️ Expected 2 vault changes, found {len(vault_changes)}. "
                    f"Possible multi-pool transaction or vault structure mismatch."
                )
                # Fallback: If we have at least 2 changes, use the top 2 by absolute value
                if len(vault_changes) >= 2:
                    vault_changes.sort(key=lambda x: abs(x[1]), reverse=True)
                    vault_changes = vault_changes[:2]
                else:
                    return 0, 0, "unknown", "unknown"
        
            # Extract token_in (vault increased = tokens came in) and token_out (vault decreased = tokens went out)
            amount_in = 0
            amount_out = 0
            token_in = "unknown"
            token_out = "unknown"
        
            for mint, diff in vault_changes:
                if diff > 0:
                    # Vault increased = trader deposited tokens into pool
                    amount_in = diff
                    token_in = mint
                elif diff < 0:
                    # Vault decreased = pool sent tokens to trader
                    amount_out = abs(diff)
                    token_out = mint
        
            # Validate we extracted both directions
            if amount_in == 0 or amount_out == 0:
                logger.warning(
                    f"⚠️ Incomplete vault extraction: "
                    f"amount_in={amount_in}, amount_out={amount_out}, "
                    f"token_in={token_in[:8]}..., token_out={token_out[:8]}..."
                )
                return 0, 0, "unknown", "unknown"
        
            return amount_in, amount_out, token_in, token_out
        
        except Exception as e:
            logger.error(f"Error extracting vault swap amounts: {e}", exc_info=True)
            return 0, 0, "unknown", "unknown"

    async def process_position_event(self, slot: int, signature: str, logs: List[str]) -> None:
        instruction_type = "Unknown"
        for log in logs:
            if "Instruction: OpenPosition" in log:
                instruction_type = "OpenPosition"
                break
            elif "Instruction: IncreaseLiquidity" in log:
                instruction_type = "IncreaseLiquidity"
                break
            elif "Instruction: DecreaseLiquidity" in log:
                instruction_type = "DecreaseLiquidity"
                break
    
        logger.info(f"💼 LP EVENT | Type: {instruction_type} | Slot: {slot} | Sig: {signature[:16]}...")
    
        #NOTE (Phase 2): LP position bounds (tick_lower, tick_upper) and liquidity decoding are scheduled for Phase 2.


    async def _route_message(self, message: Any) -> None:
        try:
            parsed_account = self._parse_notification(message)
            if parsed_account:
                await self.process_update(parsed_account)
                return
            
            parsed_log = self._parse_log_notification(message)
            if parsed_log:
                instruction_type = parsed_log['instruction_type']
                slot = parsed_log['slot']
                signature = parsed_log['signature']
                logs = parsed_log['logs']
                
                if instruction_type == "Swap":
                    await self.process_swap_event(slot, signature, logs)
                elif instruction_type in ["OpenPosition", "IncreaseLiquidity", "DecreaseLiquidity"]:
                    await self.process_position_event(slot, signature, logs)
                
                return
            
            # If neither parser handled it, it might be a subscription confirmation
            # (already handled in listen() method)
            
        except Exception as e:
            logger.error(f"Error routing message: {e}", exc_info=True)


    async def _sync_pool_state_from_rpc(self) -> None:
        if self.rpc_client is None:
            return

        try:
            resp = await self.rpc_client.get_account_info(
                WHIRLPOOL_ADDRESS,
                commitment=COMMITMENT,
                encoding="base64"
            )
            if not resp or not resp.value or not resp.value.data:
                logger.warning("⚠️ Could not fetch initial pool state from RPC")
                return

            slot = resp.context.slot
            raw_data = resp.value.data

            if isinstance(raw_data, bytes):
                account_data = raw_data
            elif isinstance(raw_data, list):
                account_data = base64.b64decode(raw_data[0])
            elif isinstance(raw_data, str):
                account_data = base64.b64decode(raw_data)
            else:
                account_data = bytes(raw_data)

            pool_state = decode_pool_state(account_data)
            price = calculate_price_from_sqrt(
                pool_state["sqrt_price"],
                decimals_a=SOL_DECIMALS,
                decimals_b=USDC_DECIMALS
            )

            if slot > (self.last_seen_slot or 0):
                self.last_seen_slot = slot

            if self.latest_pool_state:
                self.previous_pool_state = self.latest_pool_state.copy()

            self.latest_pool_state = {
                'slot': slot,
                'sqrt_price': pool_state['sqrt_price'],
                'tick_current_index': pool_state['tick_current_index'],
                'liquidity': pool_state['liquidity'],
                'fee_growth_global_a': pool_state['fee_growth_global_a'],
                'fee_growth_global_b': pool_state.get('fee_growth_global_b', 0),
            }

            logger.info(
                f"📸 BASELINE STATE | Slot {slot:>9} | "
                f"Price: ${price:>8,.2f} | "
                f"Tick: {pool_state['tick_current_index']:>7} | "
                f"Liquidity: {pool_state['liquidity']:>15,}"
            )
        except Exception as e:
            logger.error(f"❌ Error synchronizing baseline pool state: {e}", exc_info=True)
        
    async def listen(self) -> None:
        """Main listening loop with connection handling and dual subscriptions. """
        self.running = True
        self.reconnect_attempts = 0
        
        while self.running:
            try:
                clean_ws_url = RPC_ENDPOINT.partition("?")[0]
                logger.info(f"Connecting to {clean_ws_url}...")
                
                async with connect(RPC_ENDPOINT) as self.websocket:
                    logger.info("Establishing dual subscriptions...")

                    if self.rpc_client is None:
                        http_rpc_endpoint = RPC_ENDPOINT.replace("wss://", "https://").replace("ws://", "http://")
                        self.rpc_client = AsyncClient(http_rpc_endpoint)
                        clean_http_url = http_rpc_endpoint.partition("?")[0]
                        logger.info(f"✅ HTTP RPC client initialized: {clean_http_url}")
                    
                    await self._sync_pool_state_from_rpc()   

                    # ========================================
                    # SUBSCRIPTION 1: Account Updates
                    # ========================================
                    logger.info(f"Subscribing to Whirlpool account: {WHIRLPOOL_ADDRESS}")
                    await self.websocket.account_subscribe(
                        WHIRLPOOL_ADDRESS,
                        commitment=COMMITMENT,
                        encoding=ENCODING
                    )
                
                    account_sub_confirmed = False
                    while not account_sub_confirmed:
                        resp = await self.websocket.recv()
                        item = next(iter(resp), resp) if isinstance(resp, list) else resp
                        
                        if isinstance(item, SubscriptionResult) or (hasattr(item, 'result') and isinstance(item.result, int)):
                            self.account_subscription_id = item.result
                            account_sub_confirmed = True
                        elif isinstance(item, dict) and isinstance(item.get('result'), int):
                            self.account_subscription_id = item.get('result')
                            account_sub_confirmed = True
                        else:
                            task = asyncio.create_task(self._route_message(resp))
                            self._processing_tasks.add(task)
                            task.add_done_callback(self._processing_tasks.discard)
                    
                    logger.info(f"✅ Account subscription established | ID: {self.account_subscription_id}")
                    
                    # ========================================
                    # SUBSCRIPTION 2: Program Logs
                    # ========================================
                    logger.info(f"Subscribing to Whirlpool program logs: {WHIRLPOOL_ADDRESS}")
                    
                    await self.websocket.logs_subscribe(
                        RpcTransactionLogsFilterMentions(WHIRLPOOL_ADDRESS),
                        commitment=COMMITMENT
                    )
                    
                    logs_sub_confirmed = False
                    while not logs_sub_confirmed:
                        resp = await self.websocket.recv()
                        item = next(iter(resp), resp) if isinstance(resp, list) else resp
                        
                        if isinstance(item, SubscriptionResult) or (hasattr(item, 'result') and isinstance(item.result, int)):
                            self.logs_subscription_id = item.result
                            logs_sub_confirmed = True
                        elif isinstance(item, dict) and isinstance(item.get('result'), int):
                            self.logs_subscription_id = item.get('result')
                            logs_sub_confirmed = True
                        else:
                            task = asyncio.create_task(self._route_message(resp))
                            self._processing_tasks.add(task)
                            task.add_done_callback(self._processing_tasks.discard)
                    
                    logger.info(f"✅ Logs subscription established | ID: {self.logs_subscription_id}")
                    logger.info("🎯 Dual subscriptions active - listening for account updates & program events...")
                    
                    self.reconnect_attempts = 0
                    
                    # ========================================
                    # MESSAGE PROCESSING LOOP
                    # ========================================
                    while self.running:
                        try:
                            message = await asyncio.wait_for(self.websocket.recv(), timeout=30.0)
                            
                            task = asyncio.create_task(self._route_message(message))
                            self._processing_tasks.add(task)
                            task.add_done_callback(self._processing_tasks.discard)
                            
                        except asyncio.TimeoutError:
                            logger.debug("No message received for 30s, connection alive")
                            continue
                        except Exception as e:
                            logger.error(f"Error in message processing: {e}")
                            raise

            except SolanaRpcException as e:
                logger.error(f"Solana RPC error: {e}")
                await self._handle_reconnect()
                
            except ConnectionError as e:
                logger.error(f"Connection error: {e}")
                await self._handle_reconnect()
                
            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)
                await self._handle_reconnect()
                
            finally:
                if self._processing_tasks:
                    logger.info(f"Waiting for {len(self._processing_tasks)} background tasks to complete...")
                    await asyncio.gather(*self._processing_tasks, return_exceptions=True)
                    self._processing_tasks.clear()
                
                if self.websocket:
                    try:
                        await self.websocket.close()
                    except Exception:
                        pass
                    self.websocket = None
                    
                self.account_subscription_id = None
                self.logs_subscription_id = None
    
    async def _handle_reconnect(self) -> None:
        """Handle reconnection with exponential backoff."""
        self.reconnect_attempts += 1
        
        if self.reconnect_attempts > MAX_RECONNECT_ATTEMPTS:
            logger.error(f"Max reconnect attempts ({MAX_RECONNECT_ATTEMPTS}) reached. Giving up.")
            self.running = False
            return
            
        # Calculate backoff with jitter
        backoff = min(INITIAL_BACKOFF * (2 ** (self.reconnect_attempts - 1)), MAX_BACKOFF)
        jitter = backoff * 0.1 * (2 * (time.time() % 1) - 1)  # ±10% jitter
        sleep_time = max(0, backoff + jitter)
        
        logger.warning(f"Attempt {self.reconnect_attempts}/{MAX_RECONNECT_ATTEMPTS}: "
                      f"Reconnecting in {sleep_time:.1f} seconds...")
        
        await asyncio.sleep(sleep_time)
    
    async def stop(self) -> None:
        self.running = False

        if self._processing_tasks:
            logger.info(f"Shutting down - waiting for {len(self._processing_tasks)} tasks...")
            await asyncio.gather(*self._processing_tasks, return_exceptions=True)
            self._processing_tasks.clear()
    
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception:
                pass

        # Close HTTP RPC client
        if self.rpc_client:
            await self.rpc_client.close()
            self.rpc_client = None
            logger.info("✅ HTTP RPC client closed")
    
        logger.info("Listener stopped")

async def main():
    """Main entry point for the Whirlpool listener."""
    db = await get_database(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        database=os.getenv("DB_NAME", "solana_clmm"),
        user=os.getenv("DB_USER", "clmm_user"),
        password=os.getenv("DB_PASSWORD"),
    )

    
    listener = WhirlpoolListener(db_instance=db)
    
    try:
        logger.info("Starting Whirlpool listener with dual subscriptions...")
        await listener.listen()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        await listener.stop()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        await listener.stop()
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Listener shut down gracefully")