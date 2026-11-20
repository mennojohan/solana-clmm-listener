"""
Orca Whirlpool Account Decoder

This decoder matches the official Orca Whirlpool Rust struct layout byte-for-byte.
Source: https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/state/whirlpool.rs
Discriminator verified against official test suite.
"""

import math

from construct import (
    Array,
    Bytes,
    BytesInteger,
    Int8ul, 
    Int16ul, 
    Int32sl, 
    Int64ul,
    Struct,
)
from solders.pubkey import Pubkey

# Reward info sub-structure (128 bytes each, 3 total = 384 bytes)
WhirlpoolRewardInfo = Struct(
    "mint" / Bytes(32),
    "vault" / Bytes(32),
    "authority" / Bytes(32),
    "emissions_per_second_x64" / BytesInteger(16, swapped=True),
    "growth_global_x64" / BytesInteger(16, swapped=True),
)

WhirlpoolAccount = Struct(
    # Anchor discriminator (8 bytes) - SHA256("account:Whirlpool")[0:8]
    "discriminator" / Bytes(8),
    
    # Config and bump (33 bytes)
    "whirlpools_config" / Bytes(32),
    "whirlpool_bump" / Array(1, Int8ul),
    
    # Tick spacing (4 bytes)
    "tick_spacing" / Int16ul,
    "tick_spacing_seed" / Bytes(2),
    
    # Fee rates (4 bytes)
    "fee_rate" / Int16ul,
    "protocol_fee_rate" / Int16ul,
    
    # Core pool state (36 bytes)
    "liquidity" / BytesInteger(16, swapped=True),
    "sqrt_price" / BytesInteger(16, swapped=True),
    "tick_current_index" / Int32sl,  # i32 (4 bytes)
    
    # Protocol fees (16 bytes)
    "protocol_fee_owed_a" / Int64ul,
    "protocol_fee_owed_b" / Int64ul,
    
    # Token configuration (128 bytes)
    "token_mint_a" / Bytes(32),
    "token_mint_b" / Bytes(32),
    "token_vault_a" / Bytes(32),
    "token_vault_b" / Bytes(32),
    
    # Fee growth (32 bytes)
    "fee_growth_global_a" / BytesInteger(16, swapped=True),
    "fee_growth_global_b" / BytesInteger(16, swapped=True),
    
    # Reward timestamp (8 bytes)
    "reward_last_updated_timestamp" / Int64ul,
    
    # Reward infos (384 bytes)
    "reward_infos" / Array(3, WhirlpoolRewardInfo),
)

# Official Orca Whirlpool discriminator
# Source: whirlpool/src/state/whirlpool.rs discriminator test
WHIRLPOOL_DISCRIMINATOR = bytes([0x3f, 0x95, 0xd1, 0x0c, 0xe1, 0x80, 0x63, 0x09])


def decode_pool_state(data: bytes) -> dict:
    """
    Decode raw Whirlpool account bytes into a clean Python dictionary.
    
    Args:
        data: Raw bytes from a Whirlpool account (should be exactly 653 bytes)
        
    Returns:
        Dictionary containing fields needed for CLMM simulation
        
    Raises:
        ValueError: If data size is invalid or discriminator doesn't match
    """
    # Validate account size
    if len(data) != 653:
        raise ValueError(
            f"Invalid Whirlpool account size: {len(data)} bytes (expected 653). "
            f"This may not be a Whirlpool account."
        )
    
    # Parse the account data
    try:
        parsed = WhirlpoolAccount.parse(data)
    except Exception as e:
        raise ValueError(f"Failed to parse Whirlpool account structure: {e}")
    
    # Verify discriminator
    if parsed.discriminator != WHIRLPOOL_DISCRIMINATOR:
        raise ValueError(
            f"Invalid discriminator: {parsed.discriminator.hex()} "
            f"(expected: {WHIRLPOOL_DISCRIMINATOR.hex()}). "
            f"This account is not an Orca Whirlpool."
        )
    
    # The parsed fields are bytes objects, not hex strings
    whirlpools_config_b58 = bytes_to_base58(parsed.whirlpools_config)
    token_mint_a_b58 = bytes_to_base58(parsed.token_mint_a)
    token_mint_b_b58 = bytes_to_base58(parsed.token_mint_b)
    token_vault_a_b58 = bytes_to_base58(parsed.token_vault_a)
    token_vault_b_b58 = bytes_to_base58(parsed.token_vault_b)
    
    # Extract and return decoded fields
    return {
        "whirlpools_config": whirlpools_config_b58,
        "whirlpool_bump": parsed.whirlpool_bump[0],
        "tick_spacing": parsed.tick_spacing,
        "tick_spacing_seed": parsed.tick_spacing_seed.hex(),
        "fee_rate": parsed.fee_rate,
        "protocol_fee_rate": parsed.protocol_fee_rate,
        "liquidity": parsed.liquidity,
        "sqrt_price": parsed.sqrt_price,
        "tick_current_index": parsed.tick_current_index,
        "protocol_fee_owed_a": parsed.protocol_fee_owed_a,
        "protocol_fee_owed_b": parsed.protocol_fee_owed_b,
        "token_mint_a": token_mint_a_b58,
        "token_mint_b": token_mint_b_b58,
        "token_vault_a": token_vault_a_b58,
        "token_vault_b": token_vault_b_b58,
        "fee_growth_global_a": parsed.fee_growth_global_a,
        "fee_growth_global_b": parsed.fee_growth_global_b,
        "reward_last_updated_timestamp": parsed.reward_last_updated_timestamp,
        "reward_infos": [
            {
                "mint": bytes_to_base58(info.mint),
                "vault": bytes_to_base58(info.vault),
                "authority": bytes_to_base58(info.authority),
                "emissions_per_second_x64": info.emissions_per_second_x64,
                "growth_global_x64": info.growth_global_x64,
            }
            for info in parsed.reward_infos
        ],
    }


def calculate_price_from_sqrt(sqrt_price_x64: int, decimals_a: int, decimals_b: int) -> float:
    """Convert sqrt_price (Q64.64 fixed point) to human-readable price."""

    # Convert from Q64.64 fixed point to float
    # Orca uses Q64.64 format (not Q96 like Uniswap V3)
    sqrt_price = sqrt_price_x64 / (2 ** 64)
    
    # Square to get actual price
    price = sqrt_price ** 2
    
    # Adjust for token decimals
    # price_adjusted = price * (10^decimals_a / 10^decimals_b)
    price_adjusted = price * (10 ** decimals_a) / (10 ** decimals_b)
    
    return price_adjusted


def calculate_tick_from_sqrt_price(sqrt_price_x64: int) -> int:
    """Calculate tick index from sqrt_price."""
    
    sqrt_price = sqrt_price_x64 / (2 ** 64)
    price = sqrt_price ** 2
    
    # Tick formula: tick = log_base_1.0001(price)
    # = log(price) / log(1.0001)
    tick = math.log(price) / math.log(1.0001)
    
    return int(round(tick))


# ✅ HELPER: Convert bytes directly to Base58
def bytes_to_base58(raw_bytes: bytes) -> str:
    """
    Convert raw bytes to Base58 Solana address.
    
    Args:
        raw_bytes: 32 bytes representing a Solana public key
        
    Returns:
        Base58-encoded Solana address
    """
    return str(Pubkey.from_bytes(raw_bytes))