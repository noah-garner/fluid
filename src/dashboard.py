import streamlit as str_lit
import duckdb
import os
import time
import altair as alt

str_lit.set_page_config(
    page_title="Fluid | Real-Time ML Stream Engine",
    layout="wide",
    initial_sidebar_state="collapsed"
)

str_lit.markdown("""
    <style>
    .reportview-container { background: #0e1117; }
    h1, h2, h3 { color: #ffffff !important; font-family: monospace; }
    div[data-testid="stMetricValue"] { font-family: monospace; color: #ffffff !important; }
    </style>
""", unsafe_allow_html=True)

str_lit.title("Fluid - Distributed Stream Engine For Real-Time ML")

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
    
    df_metrics = con.execute(f"""
        SELECT 
            COUNT(*) as total_events,
            SUM(CASE WHEN anomaly_score = -1 THEN 1 ELSE 0 END) as total_anomalies
        FROM read_parquet('{all_sink_path}')
    """).df()
    
    events_val = int(df_metrics['total_events'].iloc[0])
    anomalies_val = int(df_metrics['total_anomalies'].iloc[0]) if df_metrics['total_anomalies'].iloc[0] is not None else 0
    
    col1, col2 = str_lit.columns(2)
    col1.metric("EVENTS SCORED (THROUGHPUT)", f"{events_val:,}")
    col2.metric("ANOMALIES FLAGGED", f"{anomalies_val:,}")
    
    df_max_time = con.execute(f"SELECT MAX(_ingested_at) as max_t FROM read_parquet('{btc_sink_path}')").df()
    
    if not df_max_time.empty and df_max_time["max_t"].iloc[0] is not None:
        max_t = int(df_max_time["max_t"].iloc[0])
        
        df_radar = con.execute(f"""
            SELECT 
                CAST(({max_t} - _ingested_at) / 1000 AS INT) as seconds_ago,
                AVG(price) as avg_price,
                SUM(CASE WHEN anomaly_score = -1 THEN 1 ELSE 0 END) as anomaly_count
            FROM read_parquet('{btc_sink_path}')
            WHERE _ingested_at >= {max_t} - 30000
            GROUP BY seconds_ago
            ORDER BY seconds_ago DESC
        """).df()
        
        if not df_radar.empty:
            df_radar["time_index"] = 30 - df_radar["seconds_ago"]
            df_anomalies = df_radar[df_radar["anomaly_count"] > 5]
            
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
                        title='BTC-USD ORDER BOOK VELOCITY',
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
            str_lit.altair_chart(radar_chart, use_container_width=True)
    
except Exception as e:
    str_lit.error(f"Analytical Storage Scan Exception: {e}")

time.sleep(1)
str_lit.rerun()
