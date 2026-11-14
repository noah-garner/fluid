import os
import time
import json
import daft
import ray
from sklearn.ensemble import IsolationForest
import pandas as pd
import numpy as np

class AnomalyScorer:
    def __init__(self):
        self.model = IsolationForest(contamination=0.05, random_state=42)

    def score_dataframe(self, pdf):
        if len(pdf) < 5:
            pdf["anomaly_score"] = np.zeros(len(pdf), dtype=np.int64)
            return pdf
            
        price_np = pdf["price"].to_numpy()
        size_np = pdf["size"].to_numpy()
        
        price_velocity = np.diff(price_np, prepend=0.0)
        rolling_mean = pd.Series(size_np).rolling(window=5, min_periods=1).mean().to_numpy()
        size_velocity_ratio = np.where(rolling_mean == 0, 1.0, size_np / rolling_mean)
        
        X = np.column_stack((price_velocity, size_velocity_ratio))
        pdf["anomaly_score"] = self.model.fit_predict(X)
        return pdf

@ray.remote
class FluidStreamConsumer:
    def __init__(self, bootstrap_servers, topic, sink_path):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.sink_path = sink_path
        self.scorer = AnomalyScorer()
        self.is_running = True

    def start_streaming(self):
        from confluent_kafka import Consumer as ConfluentConsumer, KafkaError
        
        print(f"Client socket listening on: '{self.topic}'")
        
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'fluid_ray_consumers_group_v2',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        
        consumer = ConfluentConsumer(conf)
        consumer.subscribe([self.topic])
        
        batch_records = []
        last_flush_time = time.time()
        
        try:
            while self.is_running:
                msg = consumer.poll(timeout=0.1)
                
                if msg is None:
                    if len(batch_records) > 0 and (time.time() - last_flush_time >= 2.0):
                        self.flush_batch(batch_records)
                        batch_records = []
                        last_flush_time = time.time()
                    continue
                    
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"Broker Error: {msg.error()}")
                    continue
                    
                try:
                    raw_val = msg.value().decode('utf-8', errors='ignore')
                    data = json.loads(raw_val)
                    
                    if "product_id" in data and "price" in data and "last_size" in data:
                        meta_offset = msg.offset()
                        meta_ts_type, meta_timestamp = msg.timestamp()
                        
                        batch_records.append({
                            "product_id": str(data["product_id"]),
                            "price": float(data["price"]),
                            "size": float(data["last_size"]),
                            "volume_24h": float(data.get("volume_24h", 0.0)),
                            "sequence": int(data.get("sequence", 0)),
                            "side": str(data.get("side", "buy")),
                            "offset_val": int(meta_offset),
                            "_ingested_at": int(meta_timestamp)
                        })
                except Exception as parse_err:
                    print(f"Parse Error: {parse_err}", flush=True)
                    
                if len(batch_records) >= 5000 or (time.time() - last_flush_time >= 2.0 and len(batch_records) > 0):
                    self.flush_batch(batch_records)
                    batch_records = []
                    last_flush_time = time.time()

        finally:
            consumer.close()

    def flush_batch(self, batch_records):
        try:
            pdf = pd.DataFrame(batch_records)
            pdf_scored = self.scorer.score_dataframe(pdf)
            
            df_gold = daft.from_pandas(pdf_scored).select(
                "product_id", "price", "size", "volume_24h", "sequence", "side", "anomaly_score", "_ingested_at"
            )
            
            df_gold.write_parquet(self.sink_path, partition_cols=["product_id"])
            print(f"Fluid Stream success. Flushed vector batch slice. Length: {len(pdf_scored)}", flush=True)
        except Exception as e:
            print(f"Write Error: {e}")

    def stop_streaming(self):
        self.is_running = False

def main():
    bootstrap_servers = "localhost:19092"
    topic = "liquidity_transactions"
    num_workers = 4
    
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    sink_path = os.path.join(base_dir, "data", "tables", "stream_sink")
    os.makedirs(sink_path, exist_ok=True)
    
    ray.init(
        runtime_env={"env_vars": {"VIRTUAL_ENV": os.environ.get("VIRTUAL_ENV", "")}},
        ignore_reinit_error=True
    )
    
    workers = [
        FluidStreamConsumer.remote(bootstrap_servers, topic, sink_path)
        for _ in range(num_workers)
    ]
    ray.get([w.start_streaming.remote() for w in workers])

if __name__ == "__main__":
    main()
