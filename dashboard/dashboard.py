"""
Binance Pipeline · Streamlit Dashboard
========================================
Real-time crypto analytics dashboard powered by DuckDB.

Run:
    streamlit run dashboard/dashboard.py
"""

import time
from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st

# =============================================================
# Page config
# =============================================================

st.set_page_config(
    page_title="Binance Pipeline · Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================
# Paths
# =============================================================

DASHBOARD_DIR = Path(__file__).parent
PROJECT_ROOT  = DASHBOARD_DIR.parent
DB_PATH       = PROJECT_ROOT / "data" / "pipeline.duckdb"
BRONZE_PATH   = PROJECT_ROOT / "data" / "bronze" / "trades"

SILVER = "main_silver"
GOLD   = "main_gold"

SYMBOL_COLORS = {
    "BTCUSDT": "#f7931a",
    "ETHUSDT": "#627eea",
    "SOLUSDT": "#9945ff",
}

# =============================================================
# DuckDB connection — cached for the session
# =============================================================

@st.cache_resource
def get_connection():
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"""
        CREATE OR REPLACE VIEW main.trades AS
        SELECT * FROM read_parquet(
            '{BRONZE_PATH.as_posix()}/**/*.parquet',
            hive_partitioning := true
        )
    """)
    return con


def query(sql: str) -> pd.DataFrame:
    return get_connection().sql(sql).df()


# =============================================================
# Sidebar · Filters
# =============================================================

with st.sidebar:
    st.title("⚙️ Filters")

    symbol = st.selectbox(
        "Symbol",
        options=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        index=0,
    )

    candle_limit = st.slider(
        "Candles (minutes)",
        min_value=30,
        max_value=480,
        value=120,
        step=30,
    )

    whale_threshold = st.number_input(
        "Whale threshold ($)",
        min_value=10_000,
        max_value=1_000_000,
        value=50_000,
        step=10_000,
    )

    auto_refresh = st.toggle("Auto-refresh (30s)", value=False)

    st.divider()
    st.caption("Binance Kafka Pipeline")
    st.caption("Bronze → Silver → Gold · dbt + DuckDB")

# =============================================================
# Auto-refresh
# =============================================================

if auto_refresh:
    time.sleep(30)
    st.rerun()

# =============================================================
# Header · KPI metrics
# =============================================================

st.title("📈 Crypto Analytics Dashboard")

color = SYMBOL_COLORS[symbol]

try:
    kpi = query(f"""
        select
            last(close order by candle_time)        as last_price,
            first(close order by candle_time)       as first_price,
            max(high)                               as high_24h,
            min(low)                                as low_24h,
            round(sum(volume_usd) / 1e6, 2)        as volume_m,
            last(vwap order by candle_time)         as last_vwap
        from {SILVER}.stg_ohlcv_1min
        where symbol = '{symbol}'
    """)

    last_price  = kpi["last_price"].iloc[0]
    first_price = kpi["first_price"].iloc[0]
    pct_change  = (last_price - first_price) / first_price * 100

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric(
        label=f"{symbol} Price",
        value=f"${last_price:,.2f}",
        delta=f"{pct_change:+.2f}%",
    )
    col2.metric("24h High",    f"${kpi['high_24h'].iloc[0]:,.2f}")
    col3.metric("24h Low",     f"${kpi['low_24h'].iloc[0]:,.2f}")
    col4.metric("Volume",      f"${kpi['volume_m'].iloc[0]:.2f}M")
    col5.metric("VWAP",        f"${kpi['last_vwap'].iloc[0]:,.2f}")

except Exception as e:
    st.error(f"Could not load KPI metrics: {e}")

st.divider()

# =============================================================
# Row 1 · OHLCV chart + Volume
# =============================================================

st.subheader(f"🕯️ {symbol} · OHLCV + Volume Delta")

try:
    df_ohlcv = query(f"""
        select candle_time, open, high, low, close,
               volume_usd, vwap, volume_delta, buy_volume, sell_volume
        from {SILVER}.stg_ohlcv_1min
        where symbol = '{symbol}'
        order by candle_time desc
        limit {candle_limit}
    """).sort_values("candle_time")

    df_ohlcv["candle_time"] = pd.to_datetime(df_ohlcv["candle_time"])
    df_ohlcv["color"]       = df_ohlcv.apply(
        lambda r: "#3fb950" if r["close"] >= r["open"] else "#f85149", axis=1
    )

    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        row_heights=[0.6, 0.2, 0.2],
        vertical_spacing=0.03,
    )

    # Candlesticks
    fig.add_trace(go.Candlestick(
        x=df_ohlcv["candle_time"],
        open=df_ohlcv["open"],   high=df_ohlcv["high"],
        low=df_ohlcv["low"],     close=df_ohlcv["close"],
        increasing_line_color="#3fb950",
        decreasing_line_color="#f85149",
        name="OHLCV",
    ), row=1, col=1)

    # VWAP line
    fig.add_trace(go.Scatter(
        x=df_ohlcv["candle_time"], y=df_ohlcv["vwap"],
        line=dict(color="#58a6ff", width=1.5, dash="dot"),
        name="VWAP",
    ), row=1, col=1)

    # Volume bars
    fig.add_trace(go.Bar(
        x=df_ohlcv["candle_time"], y=df_ohlcv["volume_usd"],
        marker_color=df_ohlcv["color"],
        name="Volume",
        opacity=0.8,
    ), row=2, col=1)

    # Volume delta
    delta_colors = ["#3fb950" if d >= 0 else "#f85149"
                    for d in df_ohlcv["volume_delta"]]
    fig.add_trace(go.Bar(
        x=df_ohlcv["candle_time"], y=df_ohlcv["volume_delta"],
        marker_color=delta_colors,
        name="Delta",
        opacity=0.8,
    ), row=3, col=1)

    fig.update_layout(
        height=560,
        paper_bgcolor="#0d1117",
        plot_bgcolor="#161b22",
        font_color="#c9d1d9",
        xaxis_rangeslider_visible=False,
        legend=dict(bgcolor="#161b22", bordercolor="#30363d"),
        margin=dict(l=0, r=0, t=10, b=0),
    )
    fig.update_yaxes(gridcolor="#21262d", zerolinecolor="#30363d")
    fig.update_xaxes(gridcolor="#21262d")
    fig.update_yaxes(title_text="Price (USDT)", row=1, col=1)
    fig.update_yaxes(title_text="Vol ($)",      row=2, col=1)
    fig.update_yaxes(title_text="Delta",        row=3, col=1)

    st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Could not load OHLCV chart: {e}")

# =============================================================
# Row 2 · Bollinger Bands | VWAP deviation
# =============================================================

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📊 Bollinger Bands")
    try:
        df_bb = query(f"""
            select candle_time, close, sma_20, bb_upper, bb_lower, pct_b
            from {GOLD}.mart_volatility
            where symbol = '{symbol}'
            order by candle_time desc
            limit {candle_limit}
        """).sort_values("candle_time")

        df_bb["candle_time"] = pd.to_datetime(df_bb["candle_time"])

        fig_bb = make_subplots(rows=2, cols=1, shared_xaxes=True,
                               row_heights=[0.7, 0.3], vertical_spacing=0.05)

        fig_bb.add_trace(go.Scatter(
            x=df_bb["candle_time"], y=df_bb["close"],
            line=dict(color=color, width=1.5), name="Close",
        ), row=1, col=1)
        fig_bb.add_trace(go.Scatter(
            x=df_bb["candle_time"], y=df_bb["sma_20"],
            line=dict(color="#58a6ff", width=1, dash="dash"), name="SMA-20",
        ), row=1, col=1)
        fig_bb.add_trace(go.Scatter(
            x=df_bb["candle_time"], y=df_bb["bb_upper"],
            line=dict(color="#e3b341", width=0.8), name="Upper",
            fill=None,
        ), row=1, col=1)
        fig_bb.add_trace(go.Scatter(
            x=df_bb["candle_time"], y=df_bb["bb_lower"],
            line=dict(color="#e3b341", width=0.8), name="Lower",
            fill="tonexty", fillcolor="rgba(227,179,65,0.07)",
        ), row=1, col=1)
        fig_bb.add_trace(go.Scatter(
            x=df_bb["candle_time"], y=df_bb["pct_b"],
            line=dict(color="#e3b341", width=1), name="%B",
            fill="tozeroy", fillcolor="rgba(227,179,65,0.1)",
        ), row=2, col=1)
        fig_bb.add_hline(y=1.0, line_color="#f85149", line_dash="dash",
                         line_width=0.7, row=2, col=1)
        fig_bb.add_hline(y=0.0, line_color="#3fb950", line_dash="dash",
                         line_width=0.7, row=2, col=1)

        fig_bb.update_layout(
            height=400, paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
            font_color="#c9d1d9", showlegend=True,
            legend=dict(bgcolor="#161b22", bordercolor="#30363d", font_size=10),
            margin=dict(l=0, r=0, t=10, b=0),
        )
        fig_bb.update_yaxes(gridcolor="#21262d")
        fig_bb.update_xaxes(gridcolor="#21262d")
        st.plotly_chart(fig_bb, use_container_width=True)

    except Exception as e:
        st.error(f"Could not load Bollinger Bands: {e}")

with col_right:
    st.subheader("📉 VWAP Deviation")
    try:
        df_vwap = query(f"""
            select candle_time, close, vwap_30min, vwap_60min, price_vs_vwap
            from {GOLD}.mart_vwap
            where symbol = '{symbol}'
            order by candle_time desc
            limit {candle_limit}
        """).sort_values("candle_time")

        df_vwap["candle_time"] = pd.to_datetime(df_vwap["candle_time"])

        fig_vwap = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                 row_heights=[0.7, 0.3], vertical_spacing=0.05)

        fig_vwap.add_trace(go.Scatter(
            x=df_vwap["candle_time"], y=df_vwap["close"],
            line=dict(color=color, width=1.5), name="Close",
        ), row=1, col=1)
        fig_vwap.add_trace(go.Scatter(
            x=df_vwap["candle_time"], y=df_vwap["vwap_30min"],
            line=dict(color="#58a6ff", width=1.2), name="VWAP 30m",
        ), row=1, col=1)
        fig_vwap.add_trace(go.Scatter(
            x=df_vwap["candle_time"], y=df_vwap["vwap_60min"],
            line=dict(color="#d2a8ff", width=0.9, dash="dot"), name="VWAP 60m",
        ), row=1, col=1)

        # Deviation fill
        pos = df_vwap["price_vs_vwap"].clip(lower=0)
        neg = df_vwap["price_vs_vwap"].clip(upper=0)
        fig_vwap.add_trace(go.Scatter(
            x=df_vwap["candle_time"], y=pos,
            fill="tozeroy", fillcolor="rgba(63,185,80,0.2)",
            line=dict(color="rgba(0,0,0,0)"), name="Above VWAP",
        ), row=2, col=1)
        fig_vwap.add_trace(go.Scatter(
            x=df_vwap["candle_time"], y=neg,
            fill="tozeroy", fillcolor="rgba(248,81,73,0.2)",
            line=dict(color="rgba(0,0,0,0)"), name="Below VWAP",
        ), row=2, col=1)

        fig_vwap.update_layout(
            height=400, paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
            font_color="#c9d1d9",
            legend=dict(bgcolor="#161b22", bordercolor="#30363d", font_size=10),
            margin=dict(l=0, r=0, t=10, b=0),
        )
        fig_vwap.update_yaxes(gridcolor="#21262d")
        fig_vwap.update_xaxes(gridcolor="#21262d")
        st.plotly_chart(fig_vwap, use_container_width=True)

    except Exception as e:
        st.error(f"Could not load VWAP chart: {e}")

# =============================================================
# Row 3 · Whale trades
# =============================================================

st.subheader(f"🐋 Whale Trades  (> ${whale_threshold:,})")

try:
    df_whale = query(f"""
        select traded_at, symbol, price, volume_usd, side,
               whale_tier, pct_of_hour_volume, rank_in_hour
        from {GOLD}.mart_whale_trades
        where volume_usd >= {whale_threshold}
        order by traded_at desc
        limit 100
    """)

    df_whale["traded_at"] = pd.to_datetime(df_whale["traded_at"])

    col_w1, col_w2 = st.columns([2, 1])

    with col_w1:
        # Scatter: time vs price, size = volume
        fig_w = px.scatter(
            df_whale,
            x="traded_at", y="price",
            size="volume_usd",
            color="side",
            color_discrete_map={"BUY": "#3fb950", "SELL": "#f85149"},
            symbol="whale_tier",
            hover_data=["volume_usd", "pct_of_hour_volume"],
            size_max=40,
        )
        fig_w.update_layout(
            height=320, paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
            font_color="#c9d1d9",
            legend=dict(bgcolor="#161b22", bordercolor="#30363d"),
            margin=dict(l=0, r=0, t=10, b=0),
        )
        fig_w.update_yaxes(gridcolor="#21262d")
        fig_w.update_xaxes(gridcolor="#21262d")
        st.plotly_chart(fig_w, use_container_width=True)

    with col_w2:
        st.dataframe(
            df_whale[[
                "traded_at", "symbol", "side",
                "price", "volume_usd", "whale_tier"
            ]].head(15).style.apply(
                lambda col: [
                    "color: #3fb950" if v == "BUY" else
                    "color: #f85149" if v == "SELL" else ""
                    for v in col
                ], subset=["side"]
            ),
            height=320,
            use_container_width=True,
        )

except Exception as e:
    st.error(f"Could not load whale trades: {e}")

# =============================================================
# Row 4 · Multi-symbol volatility + Buy pressure heatmap
# =============================================================

col_v1, col_v2 = st.columns(2)

with col_v1:
    st.subheader("⚡ Volatility Comparison")
    try:
        df_multi = query(f"""
            select symbol, candle_time, realized_vol_pct, annualized_vol
            from {GOLD}.mart_volatility
            order by candle_time desc
            limit 360
        """).sort_values(["symbol", "candle_time"])

        df_multi["candle_time"] = pd.to_datetime(df_multi["candle_time"])

        fig_vol = go.Figure()
        for sym, grp in df_multi.groupby("symbol"):
            fig_vol.add_trace(go.Scatter(
                x=grp["candle_time"], y=grp["realized_vol_pct"],
                line=dict(color=SYMBOL_COLORS.get(sym, "#fff"), width=1.2),
                name=sym,
            ))

        fig_vol.update_layout(
            height=300, paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
            font_color="#c9d1d9",
            legend=dict(bgcolor="#161b22", bordercolor="#30363d"),
            margin=dict(l=0, r=0, t=10, b=0),
            yaxis_title="Realized vol (%)",
        )
        fig_vol.update_yaxes(gridcolor="#21262d")
        fig_vol.update_xaxes(gridcolor="#21262d")
        st.plotly_chart(fig_vol, use_container_width=True)

    except Exception as e:
        st.error(f"Could not load volatility chart: {e}")

with col_v2:
    st.subheader("🌡️ Buy Pressure Heatmap")
    try:
        df_heat = query(f"""
            select
                symbol,
                date_trunc('hour', traded_at)               as hour,
                round(sum(volume_usd) filter (where side = 'BUY')
                    / nullif(sum(volume_usd), 0) * 100, 1)  as buy_pct
            from {SILVER}.stg_trades
            group by 1, 2
            order by symbol, hour
        """)

        df_heat["hour"] = pd.to_datetime(df_heat["hour"])
        pivot = df_heat.pivot(index="symbol", columns="hour", values="buy_pct")
        pivot.columns = [c.strftime("%H:%M") for c in pivot.columns]

        fig_heat = px.imshow(
            pivot,
            color_continuous_scale="RdYlGn",
            zmin=30, zmax=70,
            aspect="auto",
            text_auto=".0f",
        )
        fig_heat.update_layout(
            height=300, paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
            font_color="#c9d1d9",
            margin=dict(l=0, r=0, t=10, b=0),
            coloraxis_colorbar=dict(title="Buy %"),
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    except Exception as e:
        st.error(f"Could not load heatmap: {e}")

# =============================================================
# Footer
# =============================================================

st.divider()
st.caption("Data: Binance WebSocket API · Pipeline: Kafka → dbt → DuckDB · Built with Streamlit")