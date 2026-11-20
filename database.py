"""Async PostgreSQL storage layer for the Orca Whirlpool indexing pipeline."""

import logging
import os
from decimal import Decimal
from typing import Optional, Dict, Any

import asyncpg

logger = logging.getLogger(__name__)


class Database:
    """Async PostgreSQL connection pool and schema manager for Whirlpool data."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "whirlpool_analytics",
        user: str = "postgres",
        password: str = "postgres",
        min_pool_size: int = 5,
        max_pool_size: int = 20,
    ):
        """Store database connection and pool configuration."""
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self) -> None:
        """Create the connection pool, apply DDL schema, and seed initial metadata."""
        try:
            logger.info(f"Connecting to PostgreSQL at {self.host}:{self.port}/{self.database}...")
            
            # Create connection pool
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=self.min_pool_size,
                max_size=self.max_pool_size,
                command_timeout=60,
            )
            logger.info(f"✅ Connection pool created (min={self.min_pool_size}, max={self.max_pool_size})")
            
            await self._create_schema()
            await self._seed_initial_data()
            logger.info("✅ Database initialization complete")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}", exc_info=True)
            raise
    
    async def disconnect(self) -> None:
        """Close connection pool gracefully."""
        if self.pool:
            await self.pool.close()
            logger.info("✅ Database connection pool closed")
    
    async def _create_schema(self) -> None:
        """Create required database tables, indexes, and views."""
        async with self.pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS protocols (
                    protocol_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    name VARCHAR(100) UNIQUE NOT NULL,
                    program_id VARCHAR(44) UNIQUE NOT NULL,
                    protocol_type VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS pools (
                    pool_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    protocol_id UUID REFERENCES protocols(protocol_id) ON DELETE CASCADE,
                    pool_address VARCHAR(44) UNIQUE NOT NULL,
                    token_a_mint VARCHAR(44) NOT NULL,
                    token_b_mint VARCHAR(44) NOT NULL,
                    token_a_symbol VARCHAR(20) NOT NULL,
                    token_b_symbol VARCHAR(20) NOT NULL,
                    fee_rate INTEGER NOT NULL,
                    tick_spacing INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS pool_snapshots (
                    snapshot_id BIGSERIAL PRIMARY KEY,
                    pool_id UUID REFERENCES pools(pool_id) ON DELETE CASCADE,
                    pool_address VARCHAR(44) NOT NULL,
                    price NUMERIC(30, 10) NOT NULL,
                    tick INTEGER NOT NULL,
                    liquidity NUMERIC(30, 0) NOT NULL,
                    fee_growth_a NUMERIC(40, 0) NOT NULL,
                    slot BIGINT NOT NULL,
                    captured_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS position_snapshots (
                    snapshot_id BIGSERIAL PRIMARY KEY,
                    pool_id UUID REFERENCES pools(pool_id) ON DELETE CASCADE,
                    position_mint VARCHAR(44) NOT NULL,
                    owner VARCHAR(44),
                    tick_lower INTEGER NOT NULL,
                    tick_upper INTEGER NOT NULL,
                    liquidity NUMERIC(30, 0) NOT NULL,
                    fee_growth_inside_a NUMERIC(40, 0),
                    fee_growth_inside_b NUMERIC(40, 0),
                    slot BIGINT NOT NULL,
                    captured_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tick_liquidity_snapshots (
                    snapshot_id BIGSERIAL PRIMARY KEY,
                    pool_id UUID REFERENCES pools(pool_id) ON DELETE CASCADE,
                    tick_index INTEGER NOT NULL,
                    liquidity_net NUMERIC(30, 0) NOT NULL,
                    liquidity_gross NUMERIC(30, 0) NOT NULL,
                    fee_growth_outside_a NUMERIC(40, 0),
                    fee_growth_outside_b NUMERIC(40, 0),
                    slot BIGINT NOT NULL,
                    captured_at TIMESTAMP DEFAULT NOW()
                );
            """)
    
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS swaps (
                    swap_id BIGSERIAL PRIMARY KEY,
                    pool_id UUID REFERENCES pools(pool_id) ON DELETE CASCADE,
                    signature VARCHAR(88) UNIQUE NOT NULL,
                    trader VARCHAR(44) NOT NULL,
                    amount_in NUMERIC(30, 0) NOT NULL,
                    amount_out NUMERIC(30, 0) NOT NULL,
                    token_in VARCHAR(44) NOT NULL,
                    token_out VARCHAR(44) NOT NULL,
                    sqrt_price_before NUMERIC(30, 0),
                    sqrt_price_after NUMERIC(30, 0) NOT NULL,
                    liquidity NUMERIC(30, 0) NOT NULL,
                    tick_before INTEGER,
                    tick_after INTEGER NOT NULL,
                    fee_amount NUMERIC(30, 0) NOT NULL,
                    slot BIGINT NOT NULL,
                    block_time TIMESTAMP NOT NULL
                );
            """)

            # Create indexes for performance
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_pool_snapshots_pool_address 
                ON pool_snapshots(pool_address);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_pool_snapshots_slot 
                ON pool_snapshots(slot DESC);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_pool_snapshots_captured_at 
                ON pool_snapshots(captured_at DESC);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_position_snapshots_pool_id 
                ON position_snapshots(pool_id);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_position_snapshots_position_mint 
                ON position_snapshots(position_mint);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tick_liquidity_pool_tick 
                ON tick_liquidity_snapshots(pool_id, tick_index);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_swaps_pool_id 
                ON swaps(pool_id);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_swaps_signature 
                ON swaps(signature);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_swaps_slot 
                ON swaps(slot DESC);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_swaps_block_time 
                ON swaps(block_time DESC);
            """)
            
            # Materialized View: Pool Metrics 24h
            await conn.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS pool_metrics_24h AS
                SELECT
                    p.pool_id,
                    p.pool_address,
                    p.token_a_symbol,
                    p.token_b_symbol,
                    COUNT(s.swap_id) AS swap_count_24h,
                    SUM(s.amount_in) AS total_volume_in_24h,
                    SUM(s.amount_out) AS total_volume_out_24h,
                    SUM(s.fee_amount) AS total_fees_24h,
                    AVG(s.sqrt_price_after) AS avg_sqrt_price_24h,
                    MIN(s.slot) AS first_slot_24h,
                    MAX(s.slot) AS last_slot_24h,
                    NOW() AS refreshed_at
                FROM pools p
                LEFT JOIN swaps s ON p.pool_id = s.pool_id AND s.block_time >= NOW() - INTERVAL '24 hours'
                GROUP BY p.pool_id, p.pool_address, p.token_a_symbol, p.token_b_symbol;
            """)

            await conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_pool_metrics_24h_pool_id
                ON pool_metrics_24h(pool_id);
            """)
            
            logger.info("✅ Schema created successfully")
    
    async def _seed_initial_data(self) -> None:
        async with self.pool.acquire() as conn:
            # Seed Orca protocol
            await conn.execute("""
                INSERT INTO protocols (name, program_id, protocol_type)
                VALUES ('Orca', 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc', 'AMM')
                ON CONFLICT (program_id) DO NOTHING;
            """)
            
            protocol_row = await conn.fetchrow("""
                SELECT protocol_id FROM protocols WHERE program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc';
            """)
            
            if protocol_row:
                protocol_id = protocol_row['protocol_id']
                
                # Seed default SOL/USDC Whirlpool
                await conn.execute("""
                    INSERT INTO pools (
                        protocol_id,
                        pool_address,
                        token_a_mint,
                        token_b_mint,
                        token_a_symbol,
                        token_b_symbol,
                        fee_rate,
                        tick_spacing
                    )
                    VALUES (
                        $1,
                        'HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ',
                        'So11111111111111111111111111111111111111112',
                        'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                        'SOL',
                        'USDC',
                        30,
                        64
                    )
                    ON CONFLICT (pool_address) DO NOTHING;
                """, protocol_id)

                # Also seed custom WHIRLPOOL_ADDRESS from .env if different from default
                target_pool = os.getenv("WHIRLPOOL_ADDRESS", "HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ")
                if target_pool != "HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ":
                    await conn.execute("""
                        INSERT INTO pools (
                            protocol_id, pool_address, token_a_mint, token_b_mint,
                            token_a_symbol, token_b_symbol, fee_rate, tick_spacing
                        )
                        VALUES ($1, $2, 'UNKNOWN_A', 'UNKNOWN_B', 'TOKEN_A', 'TOKEN_B', 0, 0)
                        ON CONFLICT (pool_address) DO NOTHING;
                    """, protocol_id, target_pool)
                
                logger.info("✅ Initial data seeded (Orca protocol + target pool)")
            else:
                logger.warning("⚠️ Failed to retrieve Orca protocol_id for pool seeding")
    
    async def insert_pool_snapshot(self, pool_address: str, parsed_data: Dict[str, Any]) -> None:
        """Insert a pool snapshot from the WebSocket listener stream."""
        try:
            async with self.pool.acquire() as conn:
                pool_row = await conn.fetchrow("""
                    SELECT pool_id FROM pools WHERE pool_address = $1;
                """, pool_address)
                
                if not pool_row:
                    logger.error(f"Pool not found in database: {pool_address}")
                    return
                
                pool_id = pool_row['pool_id']
                
                slot = int(parsed_data['slot'])
                price = Decimal(str(parsed_data['price']))
                tick = int(parsed_data['tick'])
                liquidity = Decimal(str(parsed_data['liquidity']))
                fee_growth_a = Decimal(str(parsed_data['fee_growth_a']))
                
                await conn.execute("""
                    INSERT INTO pool_snapshots (
                        pool_id,
                        pool_address,
                        price,
                        tick,
                        liquidity,
                        fee_growth_a,
                        slot
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7);
                """, pool_id, pool_address, price, tick, liquidity, fee_growth_a, slot)
                
                logger.debug(f"✅ Pool snapshot inserted | Slot: {slot} | Price: {price}")
                
        except asyncpg.PostgresError as e:
            logger.error(f"Database error inserting pool snapshot: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error inserting pool snapshot: {e}", exc_info=True)
            raise
    
    async def insert_swap(
        self,
        pool_address: str,
        signature: str,
        trader: str,
        amount_in: int,
        amount_out: int,
        token_in: str,
        token_out: str,
        sqrt_price_after: int,
        liquidity: int,
        tick_after: int,
        fee_amount: int,
        slot: int,
        block_time: int,
        sqrt_price_before: Optional[int] = None,
        tick_before: Optional[int] = None,
    ) -> None:
        """Insert a swap transaction from program logs."""
        try:
            async with self.pool.acquire() as conn:
                pool_row = await conn.fetchrow("""
                    SELECT pool_id FROM pools WHERE pool_address = $1;
                """, pool_address)
                
                if not pool_row:
                    logger.error(f"Pool not found in database: {pool_address}")
                    return
                
                pool_id = pool_row['pool_id']
                
                from datetime import datetime
                block_timestamp = datetime.fromtimestamp(block_time)
                
                await conn.execute("""
                    INSERT INTO swaps (
                        pool_id,
                        signature,
                        trader,
                        amount_in,
                        amount_out,
                        token_in,
                        token_out,
                        sqrt_price_before,
                        sqrt_price_after,
                        liquidity,
                        tick_before,
                        tick_after,
                        fee_amount,
                        slot,
                        block_time
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (signature) DO NOTHING;
                """, pool_id, signature, trader, amount_in, amount_out, token_in, token_out,
                    sqrt_price_before, sqrt_price_after, liquidity, tick_before, tick_after,
                    fee_amount, slot, block_timestamp)
                
                logger.debug(f"✅ Swap inserted | Sig: {signature[:16]}... | Slot: {slot}")
                
        except asyncpg.UniqueViolationError:
            logger.debug(f"Swap already exists: {signature[:16]}...")
        except asyncpg.PostgresError as e:
            logger.error(f"Database error inserting swap: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error inserting swap: {e}", exc_info=True)
            raise
    
    async def insert_position_snapshot(
        self,
        pool_address: str,
        position_mint: str,
        tick_lower: int,
        tick_upper: int,
        liquidity: int,
        slot: int,
        owner: Optional[str] = None,
        fee_growth_inside_a: Optional[int] = None,
        fee_growth_inside_b: Optional[int] = None,
    ) -> None:
        """Insert a position snapshot from program logs."""
        try:
            async with self.pool.acquire() as conn:
                # Get pool_id
                pool_row = await conn.fetchrow("""
                    SELECT pool_id FROM pools WHERE pool_address = $1;
                """, pool_address)
                
                if not pool_row:
                    logger.error(f"Pool not found in database: {pool_address}")
                    return
                
                pool_id = pool_row['pool_id']
                
                await conn.execute("""
                    INSERT INTO position_snapshots (
                        pool_id,
                        position_mint,
                        owner,
                        tick_lower,
                        tick_upper,
                        liquidity,
                        fee_growth_inside_a,
                        fee_growth_inside_b,
                        slot
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
                """, pool_id, position_mint, owner, tick_lower, tick_upper, liquidity,
                    fee_growth_inside_a, fee_growth_inside_b, slot)
                
                logger.debug(f"✅ Position snapshot inserted | Mint: {position_mint[:16]}... | Slot: {slot}")
                
        except asyncpg.PostgresError as e:
            logger.error(f"Database error inserting position snapshot: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error inserting position snapshot: {e}", exc_info=True)
            raise
    
    async def refresh_metrics_view(self) -> None:
        """Refresh the pool_metrics_24h materialized view (e.g., every 5-15 minutes)."""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY pool_metrics_24h;")
                logger.info("✅ Materialized view pool_metrics_24h refreshed")
        except asyncpg.PostgresError as e:
            logger.error(f"Error refreshing materialized view: {e}", exc_info=True)
            raise
    
    async def get_latest_slot(self, pool_address: str) -> Optional[int]:
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT MAX(slot) as latest_slot
                    FROM pool_snapshots
                    WHERE pool_address = $1;
                """, pool_address)
                
                return row['latest_slot'] if row else None
                
        except asyncpg.PostgresError as e:
            logger.error(f"Error fetching latest slot: {e}", exc_info=True)
            return None
    
    async def health_check(self) -> bool:
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1;")
                return result == 1
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False


_db_instance: Optional[Database] = None

async def get_database(
    host: str = "localhost",
    port: int = 5432,
    database: str = "whirlpool_analytics",
    user: str = "postgres",
    password: str = "postgres",
) -> Database:
    global _db_instance
    
    if _db_instance is None:
        _db_instance = Database(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        await _db_instance.connect()
    
    return _db_instance


async def close_database() -> None:
    global _db_instance
    
    if _db_instance is not None:
        await _db_instance.disconnect()
        _db_instance = None