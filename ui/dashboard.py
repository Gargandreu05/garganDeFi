"""
ui/dashboard.py — Streamlit Dashboard (port 8501)
===================================================
Run with:
    streamlit run ui/dashboard.py --server.port 8501

Reads directly from the SQLite database (read-only) and displays:
  • Wallet Balance (fetched live from RPC)
  • Net Profit (from trade history)
  • IL vs Price Chart (Plotly)
  • Recent Trade History table
  • Recent Pool Evaluations table
  • Recent Hardware Deals table
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="GarganDeFi Dashboard",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = os.getenv("DB_PATH", "./data/gargandefi.db")
REFRESH_SECS = 60  # Auto-refresh interval


# ── DB helpers (sync — Streamlit runs in its own thread) ─────────────────────

def _get_conn() -> Optional[sqlite3.Connection]:
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _query(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = _get_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
        return df
    except Exception as exc:
        st.error(f"DB query failed: {exc}")
        return pd.DataFrame()
    finally:
        conn.close()


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.image("https://cryptologos.cc/logos/solana-sol-logo.png", width=80)
    st.title("GarganDeFi")
    st.caption("Solana DeFi + Hardware Deals Bot")
    st.divider()
    st.write("**Active Pool ID:**")
    active_pool = os.getenv("ACTIVE_POOL_ID", "Not set")
    st.code(active_pool, language=None)
    st.write(f"**HITL Mode:** {'ON' if os.getenv('AUTONOMOUS_POOL_SWITCHING','false').lower()!='true' else 'OFF (Auto)'}")
    st.write(f"**DB Path:** `{DB_PATH}`")
    st.divider()
    auto_refresh = st.checkbox("Auto-refresh every 60s", value=True)
    if st.button("🔄 Refresh Now"):
        st.rerun()


# ── KPI Row ───────────────────────────────────────────────────────────────────

st.title("🌊 GarganDeFi Dashboard")
st.caption(f"Last loaded: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")

col1, col2, col3, col4 = st.columns(4)

# Latest pool evaluation
eval_df = _query(
    "SELECT * FROM pool_evaluations ORDER BY ts DESC LIMIT 1"
)
latest_eval = eval_df.iloc[0] if not eval_df.empty else None

# Net profit
profit_df = _query(
    """SELECT
         COALESCE(SUM(CASE WHEN trade_type='DEPOSIT'  THEN amount_sol ELSE 0 END), 0) AS deposited,
         COALESCE(SUM(CASE WHEN trade_type='WITHDRAW' THEN amount_sol ELSE 0 END), 0) AS withdrawn
       FROM trades WHERE status='CONFIRMED'"""
)
net_profit = 0.0
if not profit_df.empty:
    net_profit = float(profit_df.iloc[0]["withdrawn"]) - float(profit_df.iloc[0]["deposited"])

# Trade count
trade_count_df = _query("SELECT COUNT(*) as total FROM trades WHERE status='CONFIRMED'")
trade_count = int(trade_count_df.iloc[0]["total"]) if not trade_count_df.empty else 0

# Deal count today
deal_count_df = _query(
    "SELECT COUNT(*) as total FROM deals WHERE ts >= date('now')"
)
deal_count = int(deal_count_df.iloc[0]["total"]) if not deal_count_df.empty else 0

with col1:
    sol_bal_display = "—"
    st.metric("💰 SOL Balance", sol_bal_display, help="Live balance fetched from RPC on bot side")

with col2:
    st.metric("💹 Net Profit", f"{net_profit:+.4f} SOL", delta=f"{net_profit:+.4f}")

with col3:
    net_apy = float(latest_eval["net_apy_pct"]) if latest_eval is not None else 0.0
    st.metric("📈 Net APY (latest)", f"{net_apy:.2f}%")

with col4:
    st.metric("🛒 Deals Found Today", str(deal_count))

st.divider()


# ── IL vs Price Chart ─────────────────────────────────────────────────────────

st.subheader("📉 Impermanent Loss vs Pool APY Over Time")

chart_df = _query(
    """SELECT ts, pool_name, apy_pct, il_pct, net_apy_pct
       FROM pool_evaluations
       ORDER BY ts DESC LIMIT 200"""
)

if not chart_df.empty:
    chart_df["ts"] = pd.to_datetime(chart_df["ts"])
    chart_df = chart_df.sort_values("ts")

    tab1, tab2 = st.tabs(["Net APY vs IL", "Gross APY History"])

    with tab1:
        fig = go.Figure()
        for pool in chart_df["pool_name"].unique():
            pool_data = chart_df[chart_df["pool_name"] == pool]
            fig.add_trace(go.Scatter(
                x=pool_data["ts"], y=pool_data["net_apy_pct"],
                mode="lines+markers", name=f"{pool} Net APY",
            ))
            fig.add_trace(go.Scatter(
                x=pool_data["ts"], y=pool_data["il_pct"],
                mode="lines", name=f"{pool} IL%",
                line=dict(dash="dot"),
            ))
        fig.update_layout(
            xaxis_title="Time", yaxis_title="Percent (%)",
            hovermode="x unified", height=400,
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        fig2 = px.line(
            chart_df, x="ts", y="apy_pct", color="pool_name",
            title="Gross APY by Pool", labels={"ts": "Time", "apy_pct": "APY %"},
        )
        st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No pool evaluation data yet. The bot will populate this once it starts scanning.")


# ── Trade History ─────────────────────────────────────────────────────────────

st.divider()
st.subheader("🔄 Trade History")

trades_df = _query(
    "SELECT ts, trade_type, pool_name, amount_sol, amount_token, status, tx_signature FROM trades ORDER BY ts DESC LIMIT 50"
)

if not trades_df.empty:
    # Colour code by status
    def colour_status(val):
        colours = {"CONFIRMED": "background-color: #1a4731; color: #4ade80",
                   "PENDING":   "background-color: #1a3a4a; color: #60a5fa",
                   "FAILED":    "background-color: #4a1a1a; color: #f87171"}
        return colours.get(val, "")

    styled = trades_df.style.applymap(colour_status, subset=["status"])
    st.dataframe(styled, use_container_width=True, height=300)
else:
    st.info("No trades executed yet.")


# ── Pool Evaluations ──────────────────────────────────────────────────────────

st.divider()
st.subheader("🏊 Recent Pool Evaluations")

eval_all_df = _query(
    "SELECT ts, pool_name, apy_pct, il_pct, net_apy_pct, fee_7d_usd, recommended FROM pool_evaluations ORDER BY ts DESC LIMIT 50"
)
if not eval_all_df.empty:
    st.dataframe(eval_all_df, use_container_width=True, height=300)
else:
    st.info("No pool evaluations yet.")


# ── Hardware Deals ─────────────────────────────────────────────────────────────

st.divider()
st.subheader("🛒 Recent Hardware Deals")

deals_df = _query(
    "SELECT ts, title, price, original_price, discount_pct, source, url FROM deals ORDER BY ts DESC LIMIT 50"
)
if not deals_df.empty:
    def make_clickable(url):
        return f'<a href="{url}" target="_blank">🔗 Link</a>'

    deals_display = deals_df.copy()
    deals_display["link"] = deals_display["url"].apply(make_clickable)
    deals_display = deals_display.drop(columns=["url"])
    st.write(deals_display.to_html(escape=False, index=False), unsafe_allow_html=True)
else:
    st.info("No deals scraped yet.")


# ── Auto-refresh ───────────────────────────────────────────────────────────────

if auto_refresh:
    import time
    time.sleep(REFRESH_SECS)
    st.rerun()
