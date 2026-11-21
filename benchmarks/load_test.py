import json
import random
import time
from confluent_kafka import Producer

REDPANDA_BROKER = "localhost:19092"
TOPIC_NAME = "liquidity_transactions"
TARGET_RATE = 120000

PAIRS = {
    "BTC-USD": 92500.0, "ETH-USD": 3120.0, "SOL-USD": 215.0, "XRP-USD": 1.15,
    "DOGE-USD": 0.38, "ADA-USD": 0.78, "AVAX-USD": 36.5, "LINK-USD": 15.2,
    "SUI-USD": 3.45, "DOT-USD": 6.10, "LTC-USD": 88.0, "BCH-USD": 445.0,
}
PAIR_KEYS = [(p, p.encode("utf-8")) for p in PAIRS]


def run_benchmark():
    producer = Producer({
        "bootstrap.servers": REDPANDA_BROKER,
        "queue.buffering.max.messages": 2000000,
        "batch.size": 131072,
        "linger.ms": 5,
        "compression.type": "none",
        "acks": 1,
    })

    print(f"Streaming synthetic load -> '{TOPIC_NAME}' at ~{TARGET_RATE:,} msg/sec...")
    seq = 1
    batch_size = 500
    batch_interval = batch_size / TARGET_RATE
    total_sent = 0
    last_log = time.time()

    try:
        while True:
            loop_start = time.perf_counter()
            is_spike = (seq % 180000) < 400

            for _ in range(batch_size):
                symbol, key_bytes = random.choice(PAIR_KEYS)
                drift = random.uniform(-0.004, 0.004) if is_spike else random.uniform(-0.0004, 0.0004)
                PAIRS[symbol] = max(0.01, PAIRS[symbol] * (1.0 + drift))
                size = random.uniform(2.0, 25.0) if is_spike else random.uniform(0.01, 0.5)

                payload = json.dumps({
                    "product_id": symbol,
                    "price": f"{PAIRS[symbol]:.2f}",
                    "last_size": f"{size:.4f}",
                    "volume_24h": "1500000",
                    "sequence": seq,
                    "side": "buy" if drift >= 0 else "sell",
                }).encode("utf-8")

                # key=None is only for benchmarking to ensure all partitions and ray workers get exercised evenly
                producer.produce(topic=TOPIC_NAME, key=None, value=payload)
                seq += 1

            producer.poll(0)
            total_sent += batch_size

            now = time.time()
            if now - last_log >= 2.0:
                rate = int(total_sent / (now - last_log))
                print(f"[Benchmark] Published {total_sent:,} ticks -> Redpanda ({rate:,} msg/sec)")
                total_sent = 0
                last_log = now

            elapsed = time.perf_counter() - loop_start
            if elapsed < batch_interval:
                time.sleep(batch_interval - elapsed)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()


if __name__ == "__main__":
    run_benchmark()
