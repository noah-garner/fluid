import asyncio
import json
import websockets
from confluent_kafka import Producer

REDPANDA_BROKER = "localhost:19092"
TOPIC_NAME = "liquidity_transactions"

def ack_callback(err, msg):
    if err is not None:
        pass

async def stream_to_redpanda():
    conf = {
        'bootstrap.servers': REDPANDA_BROKER,
        'queue.buffering.max.messages': 1000000,
        'batch.size': 65536,
        'linger.ms': 10,
        'compression.type': 'none',
        'acks': 1
    }
    producer = Producer(conf)
    uri = "wss://ws-feed.exchange.coinbase.com"
    
    subscribe_payload = {
        "type": "subscribe",
        "product_ids": [
            "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD",
            "DOGE-USD", "ADA-USD", "AVAX-USD", "LINK-USD",
            "SUI-USD", "DOT-USD", "LTC-USD", "BCH-USD"
        ],
        "channels": ["ticker", "matches", "level2_batch"]
    }

    async for websocket in websockets.connect(uri, ping_interval=10, ping_timeout=10, max_size=None):
        try:
            await websocket.send(json.dumps(subscribe_payload))
            message_counter = 0
            while True:
                raw_message = await websocket.recv()
                data = json.loads(raw_message)
                msg_type = data.get("type")
                product_id = data.get("product_id")
                
                if not product_id:
                    continue

                if msg_type in ("ticker", "match"):
                    payload = json.dumps({
                        "product_id": product_id,
                        "price": str(data.get("price", "0")),
                        "last_size": str(data.get("last_size") or data.get("size", "0")),
                        "volume_24h": str(data.get("volume_24h", "0")),
                        "sequence": data.get("sequence", 0),
                        "side": data.get("side", "buy")
                    })
                    producer.produce(
                        topic=TOPIC_NAME,
                        key=product_id.encode('utf-8'),
                        value=payload.encode('utf-8'),
                        callback=ack_callback
                    )
                    message_counter += 1

                elif msg_type == "l2update":
                    for side, price, size in data.get("changes", []):
                        if float(size) == 0:
                            continue
                        payload = json.dumps({
                            "product_id": product_id,
                            "price": str(price),
                            "last_size": str(size),
                            "volume_24h": "0",
                            "sequence": 0,
                            "side": side
                        })
                        producer.produce(
                            topic=TOPIC_NAME,
                            key=product_id.encode('utf-8'),
                            value=payload.encode('utf-8'),
                            callback=ack_callback
                        )
                        message_counter += 1

                if message_counter >= 500:
                    producer.poll(0)
                    message_counter = 0

        except websockets.ConnectionClosed:
            continue
        except Exception:
            await asyncio.sleep(2)
            continue
        finally:
            producer.flush()

if __name__ == "__main__":
    try:
        asyncio.run(stream_to_redpanda())
    except KeyboardInterrupt:
        pass
