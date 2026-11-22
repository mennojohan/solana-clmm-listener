import altair as alt
import os
import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Solana CLMM Listener", layout="wide")
st.title("Solana CLMM Listener — Orca Whirlpool Inspector")

if st.button("🔄 Refresh Data"):
    st.rerun()

def get_swaps() -> pd.DataFrame:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "solana_clmm"),
        user=os.getenv("DB_USER", "clmm_user"),
        password=os.getenv("DB_PASSWORD"),
    )
    query = """
        SELECT 
            swap_id,
            block_time,
            slot,
            CASE 
                WHEN token_in = 'So11111111111111111111111111111111111111112' THEN 'SOL → USDC'
                ELSE 'USDC → SOL'
            END AS direction,
            amount_in,
            amount_out,
            sqrt_price_after,
            tick_after,
            fee_amount,
            trader,
            signature
        FROM clmm.swaps
        ORDER BY swap_id DESC
        LIMIT 500;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

try:
    df = get_swaps()

    if df.empty:
        st.info("Waiting for swaps to be indexed...")
    else:
        df["price_usdc"] = ((df["sqrt_price_after"].astype(float) / (2**64)) ** 2) * (10**9 / 10**6)

        # Determine decimals based on swap direction
        is_sol_in = df["direction"] == "SOL → USDC"

        # If SOL -> USDC: input & fee are SOL (10^9), output is USDC (10^6)
        # If USDC -> SOL: input & fee are USDC (10^6), output is SOL (10^9)
        df["amount_in_human"] = df["amount_in"].astype(float) / is_sol_in.map({True: 10**9, False: 10**6})
        df["amount_out_human"] = df["amount_out"].astype(float) / is_sol_in.map({True: 10**6, False: 10**9})
        df["fee_human"] = df["fee_amount"].astype(float) / 10**9

        st.subheader(f"Pool Price Curve (Last {len(df)} Swaps)")
        chart_df = df.sort_values("swap_id").copy()
        chart_df["seq"] = range(1, len(chart_df) + 1)
        chart = (
            alt.Chart(chart_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("seq:Q", title="Swap #"),
                y=alt.Y("price_usdc:Q", title="Price (USDC)", scale=alt.Scale(zero=False)),
                tooltip=[
                    alt.Tooltip("seq:Q", title="Swap #"),
                    alt.Tooltip("block_time:T", title="Time"),
                    alt.Tooltip("price_usdc:Q", title="Price (USDC)", format=",.4f"),
                    alt.Tooltip("tick_after:Q", title="Tick"),
                    alt.Tooltip("direction:N", title="Direction"),
                ]
            )
            .interactive()
        )
        st.altair_chart(chart, width="stretch")

        st.subheader("Latest Decoded Swaps")
        display_cols = [
            "block_time", "slot", "direction", "price_usdc",
            "amount_in_human", "amount_out_human", "fee_human", 
            "tick_after", "trader", "signature"
        ]
        
        st.dataframe(
            df[display_cols].head(50),
            width="stretch",
            hide_index=True,
            column_config={
                "block_time": st.column_config.DatetimeColumn("Time", format="YYYY-MM-DD HH:mm:ss"),
                "slot": st.column_config.NumberColumn("Slot", format="%d"),
                "direction": st.column_config.TextColumn("Direction"),
                "price_usdc": st.column_config.NumberColumn("Price (USDC)", format="$%.4f"),
                "amount_in_human": st.column_config.NumberColumn("Amount In", format="%.4f"),
                "amount_out_human": st.column_config.NumberColumn("Amount Out", format="%.4f"),
                "fee_human": st.column_config.NumberColumn("Tx Fee (SOL)", format="%.7f"),
                "tick_after": st.column_config.NumberColumn("Tick After", format="%d"),
                "trader": st.column_config.TextColumn("Trader"),
                "signature": st.column_config.TextColumn("Signature"),
            }
        )

except Exception as e:
    st.error(f"Database connection error: {e}")