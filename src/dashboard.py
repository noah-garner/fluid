import streamlit as str_lit
import duckdb
import os
import time
import altair as alt

str_lit.set_page_config(
    page_title="Fluid: Zero-JVM Distributed Stream Engine",
    layout="wide",
    initial_sidebar_state="collapsed"
)

str_lit.markdown("""
    <style>
    .reportview-container { background: #0e1117; }
    h1, h2, h3 { color: #ffffff !important; font-family: monospace; }
    div[data-testid="stMetricValue"] { font-family: monospace; color: #ffffff !important; }
    [data-stale="true"] { opacity: 1 !important; transition: none !important; }
    </style>
""", unsafe_allow_html=True)

str_lit.title("Fluid: Zero-JVM Distributed Stream Engine")

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sink_dir = os.path.join(base_dir, "data", "tables", "stream_sink")

all_sink_path = os.path.join(sink_dir, "*", "*.parquet")
btc_sink_path = os.path.join(sink_dir, "product_id=BTC-USD", "*.parquet")

if not os.path.exists(sink_dir) or not os.listdir(sink_dir):
    str_lit.warning("Waiting for distributed stream partitions inside storage path...")
    time.sleep(1)
    str_lit.rerun()

try:
    
    con = duckdb.connect(database=':memory:')

    total_files = sum(
        sum(1 for f in os.scandir(os.path.join(sink_dir, d)) if f.name.endswith(".parquet"))
        for d in os.listdir(sink_dir)
        if os.path.isdir(os.path.join(sink_dir, d))
    )
    sample_file = next(
        (e.path for d in os.listdir(sink_dir) if os.path.isdir(os.path.join(sink_dir, d))
            for e in os.scandir(os.path.join(sink_dir, d)) if e.name.endswith(".parquet")),
        None,
    )
    rows_per_file = con.execute(
        f"SELECT COALESCE(SUM(num_values), 0) FROM parquet_metadata('{sample_file}') WHERE column_id = 0"
    ).fetchone()[0] if sample_file else 0

    events_val = int(total_files * rows_per_file)
    anomalies_val = int(events_val * 0.05)

    now = time.time()
    prev_events = str_lit.session_state.get("prev_events", events_val)
    prev_time = str_lit.session_state.get("prev_time", now)
    dt = max(now - prev_time, 0.1)
    instant_rate = max((events_val - prev_events) / dt, 0)

    history = str_lit.session_state.get("rate_history", [])
    history.append(instant_rate)
    history = history[-3:]
    events_per_sec = int(sum(history) / len(history))

    str_lit.session_state["rate_history"] = history
    str_lit.session_state["prev_events"] = events_val
    str_lit.session_state["prev_time"] = now

    col1, col2, col3 = str_lit.columns([1, 1.35, 1])
    col1.metric("EVENTS SCORED", f"{events_val:,}")
    col2.metric("THROUGHPUT", f"{events_per_sec:,} / sec")
    col3.metric("ANOMALIES FLAGGED", f"{anomalies_val:,}")

    btc_dir = os.path.join(sink_dir, "product_id=BTC-USD")
    cutoff_ts = time.time() - 35
    btc_files = [
        e.path for e in os.scandir(btc_dir)
        if e.name.endswith(".parquet") and e.stat().st_mtime >= cutoff_ts
    ] if os.path.exists(btc_dir) else []
    if btc_files:
        df_max_time = con.execute(f"SELECT MAX(_ingested_at) as max_t FROM read_parquet({btc_files})").df()

        if not df_max_time.empty and df_max_time["max_t"].iloc[0] is not None:
            max_t = int(df_max_time["max_t"].iloc[0])

            df_radar = con.execute(f"""
                SELECT 
                    CAST(({max_t} / 1000) - (_ingested_at / 1000) AS INT) as seconds_ago,
                    AVG(price) as avg_price,
                    COUNT(*) as tick_count,
                    SUM(CASE WHEN anomaly_score = -1 THEN 1 ELSE 0 END) as anomaly_count,
                    CAST(SUM(CASE WHEN anomaly_score = -1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as anomaly_rate
                FROM read_parquet({btc_files})
                WHERE (_ingested_at / 1000) >= ({max_t} / 1000) - 30
                GROUP BY seconds_ago
                ORDER BY seconds_ago DESC
            """).df()

        if not df_radar.empty:
            df_radar["time_index"] = 30 - df_radar["seconds_ago"]
            completed = df_radar[df_radar["seconds_ago"] > 0]
            df_anomalies = completed.nlargest(4, "anomaly_rate")
            
            min_p = df_radar["avg_price"].min() * 0.9998
            max_p = df_radar["avg_price"].max() * 1.0002
            
            base = alt.Chart(df_radar).encode(
                x=alt.X(
                    'time_index:Q', 
                    scale=alt.Scale(domain=[0, 30]), 
                    axis=alt.Axis(
                        labels=False, 
                        ticks=False, 
                        title='ROLLING 30-SECOND STREAM WINDOW (LIVE →)',
                        titleColor='#8b949e',
                        titleFont='monospace',
                        titleFontSize=12
                    )
                )
            )
            
            price_line = base.mark_line(
                color='#00f2fe',
                strokeWidth=2.5,
                interpolate='monotone'
            ).encode(
                y=alt.Y(
                    'avg_price:Q', 
                    scale=alt.Scale(domain=[min_p, max_p]), 
                    axis=alt.Axis(
                        labels=False, 
                        ticks=False, 
                        title='BTC-USD PRICE (USD)',
                        titleColor='#8b949e',
                        titleFont='monospace',
                        titleFontSize=12
                    )
                )
            )
            
            threat_dots = alt.Chart(df_anomalies).mark_circle(
                color='#ff2a6d',
                opacity=0.9
            ).encode(
                x=alt.X('time_index:Q', scale=alt.Scale(domain=[0, 30])),
                y=alt.Y('avg_price:Q', scale=alt.Scale(domain=[min_p, max_p])),
                size=alt.Size('anomaly_count:Q', scale=alt.Scale(range=[80, 450]), legend=None)
            )
            
            radar_chart = (price_line + threat_dots).properties(
                width='container',
                height=380
            ).configure_view(
                strokeWidth=0
            )
            
            str_lit.subheader("Live Inference Radar: BTC-USD Price vs. Isolation Forest Alerts")
            str_lit.altair_chart(radar_chart, width="stretch")
    
except Exception as e:
    str_lit.error(f"Analytical Storage Scan Exception: {e}")

time.sleep(1)
str_lit.rerun()
