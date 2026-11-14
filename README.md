# Fluid — Distributed Stream Engine for Real-Time ML

A high-throughput, zero-copy stream processing engine built on **Redpanda**, **Ray**, and **Daft**—designed to execute real-time machine learning without the JVM serialization tax of legacy Spark or Flink clusters.

## Architecture

```mermaid
flowchart TD
    A["<b>Coinbase WebSocket</b><br/>12 Pairs · L2 + Matches"] -->|Async TCP| B["<b>Redpanda Broker</b><br/>C++ · 4 Partitions"]
    
    B -->|librdkafka| C1["<b>Ray Actor 1</b><br/>NumPy + IsolationForest"]
    B -->|librdkafka| C2["<b>Ray Actor 2</b><br/>NumPy + IsolationForest"]
    B -->|librdkafka| C3["<b>Ray Actor 3</b><br/>NumPy + IsolationForest"]
    B -->|librdkafka| C4["<b>Ray Actor 4</b><br/>NumPy + IsolationForest"]
    
    C1 & C2 & C3 & C4 -->|Zero-Copy Arrow| D[("<b>Partitioned Parquet Sink</b><br/>Daft Rust Writer")]

    
    D -->|Sub-10ms Scan| E["<b>DuckDB + Streamlit</b><br/>Live Anomaly Dashboard"]

    style A fill:#161b22,stroke:#30363d,color:#f0f6fc
    style B fill:#161b22,stroke:#ff2a6d,color:#f0f6fc
    style C1 fill:#161b22,stroke:#00f2fe,color:#f0f6fc
    style C2 fill:#161b22,stroke:#00f2fe,color:#f0f6fc
    style C3 fill:#161b22,stroke:#00f2fe,color:#f0f6fc
    style C4 fill:#161b22,stroke:#00f2fe,color:#f0f6fc
    style D fill:#161b22,stroke:#30363d,color:#f0f6fc
    style E fill:#161b22,stroke:#00f2fe,color:#f0f6fc
```

## Key Architectural Features

* **C-Level Network Ingestion (`confluent-kafka`):** Connects directly to Redpanda partitions via `librdkafka`, bypassing heavy JVM query planners to pull raw event bytes into RAM at sub-millisecond latency.
* **Stateful Distributed Orchestration (`Ray`):** Spawns a pool of isolated `@ray.remote` worker actors across compute nodes, decoupling network consumption and inline ML scoring from the main driver thread with automatic offset-checkpoint fault recovery.
* **Zero-Copy Columnar Storage (`Daft` + `Apache Arrow`):** Evaluates price velocity and volume ratios through `scikit-learn` `IsolationForest` models in memory, then uses Daft's Rust execution engine to write partitioned Parquet blocks in parallel.
* **Sub-10ms Analytical Telemetry (`DuckDB` + `Streamlit`):** Queries partitioned Parquet storage directly via DuckDB to power a live anomaly dashboard without a separate OLAP database.

## Repository Structure

* `src/fetch_feed.py` — Asynchronous WebSocket client streaming live order book and trade ticks into Redpanda across 12 markets.
* `src/fluid_stream.py` — Distributed Ray actor pool running inline `IsolationForest` anomaly detection and Daft Parquet writes.
* `src/dashboard.py` — Real-time DuckDB and Streamlit telemetry dashboard tracking throughput and structural market alerts.

## Quickstart

Ensure Docker (for Redpanda) and `uv` are installed, then launch the pipeline across three terminal panes:

```bash

# 1. Clone and sync dependencies
git clone https://github.com/noah-garner/fluid.git && cd fluid
uv sync

# 2. Start Redpanda and create a 4-partition topic
docker compose up -d
docker exec -it redpanda-local rpk topic create liquidity_transactions -p 4

# 3. Run the ingestion feed, distributed Ray workers, and live dashboard
uv run src/fetch_feed.py
uv run src/fluid_stream.py
uv run streamlit run src/dashboard.py
```

You may then view your real-time ML dashboard [here](http://localhost:8501/).

## Consulting & Advisory

I help high-growth engineering teams replace expensive, serialization-bound Spark, Flink, and Databricks streaming pipelines with lean, Python-and-Rust engines that slash compute costs and run real-time ML at a fraction of the cluster footprint.

* **Get in Touch:** Connect with me on [LinkedIn](https://www.linkedin.com/in/noah-douglas-garner/) or schedule a brief [Technical Exploration Call](https://calendly.com/deltastreams/technical-exploration-call) to walk through your architecture.
