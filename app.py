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

        st.subheader(f"Pool Price Curve (Last {len(df)} Swaps)")
        chart_df = df.sort_values("swap_id").copy()
        chart_df["seq"] = range(1, len(chart_df) + 1)
        chart = (
            alt.Chart(chart_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("seq:Q", title="Swap #"),
                y=alt.Y("price_usdc:Q", title="Price (USDC)", scale=alt.Scale(zero=False)),
                tooltip=["seq:Q", "block_time:T", "price_usdc:Q", "tick_after:Q", "direction:N"]
            )
            .interactive()
        )
        st.altair_chart(chart, use_container_width=True)

        st.subheader("Latest Decoded Swaps")
        display_cols = [
            "block_time", "slot", "direction", "price_usdc",
            "amount_in", "amount_out", "tick_after", "fee_amount", "trader", "signature"
        ]
        st.dataframe(df[display_cols].head(50), use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Database connection error: {e}")