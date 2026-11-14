"""
stream/dispatcher.py
====================
Food-order & Ride Dispatcher for RabbitMQ Topic Exchange (rapido-dispatch).
Publishes events with routing key: <city>.<zone>.<type>

Every 10th message sends a poison pill missing the 'zone' field to test
ACK/NACK handling (Part D3).
"""

import os
import sys
import time
import json
import random
import argparse
from datetime import datetime, timezone
import pika

DEFAULT_AMQP_URL = os.environ.get(
    "AMQP_URL",
    "amqps://rvapxilv:cBV7nrY5YT-Cy0OYmLxDWnNfR9aN3_co@seal.lmq.cloudamqp.com/rvapxilv"
)
EXCHANGE_NAME = "rapido-dispatch"

CITIES = ["bangalore", "chennai"]
ZONES = ["koramangala", "indiranagar", "whitefield", "tnagar"]
TYPES = ["ride", "food"]

# The 6 canonical routing keys from Assignment Part D1
D1_TEST_KEYS = [
    ("bangalore.koramangala.food", "bangalore", "koramangala", "food"),
    ("bangalore.indiranagar.ride", "bangalore", "indiranagar", "ride"),
    ("bangalore.whitefield.ride",   "bangalore", "whitefield",   "ride"),
    ("chennai.tnagar.food",         "chennai",   "tnagar",       "food"),
    ("chennai.whitefield.ride",     "chennai",   "whitefield",   "ride"),
    ("bangalore.koramangala",       "bangalore", "koramangala",  None),   # 2-word key
]


def create_connection(url):
    params = pika.URLParameters(url)
    params.heartbeat = 60
    params.blocked_connection_timeout = 30
    return pika.BlockingConnection(params)


def run_dispatcher(url=DEFAULT_AMQP_URL, interval=2.0, max_messages=None, test_d1=False):
    print(f"[DISPATCHER] Connecting to RabbitMQ broker...")
    connection = create_connection(url)
    channel = connection.channel()

    # Declare topic exchange
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type="topic",
        durable=False
    )
    print(f"[DISPATCHER] Topic exchange '{EXCHANGE_NAME}' ready.")
    print(f"[DISPATCHER] Starting dispatch loop (interval={interval}s)...\n")

    msg_count = 0
    try:
        # If test_d1 is requested, first emit the 6 exact assignment D1 keys
        if test_d1:
            print("--- EMITTING 6 CANONICAL D1 ASSIGNMENT KEYS ---")
            for idx, (rk, city, zone, typ) in enumerate(D1_TEST_KEYS, start=1):
                msg_count += 1
                req_id = f"D1-REQ-{idx:02d}"
                ts = datetime.now(timezone.utc).isoformat()
                
                payload = {
                    "id": req_id,
                    "city": city,
                    "zone": zone,
                    "ts": ts
                }
                if typ is not None:
                    payload["type"] = typ

                body = json.dumps(payload)
                channel.basic_publish(
                    exchange=EXCHANGE_NAME,
                    routing_key=rk,
                    body=body,
                    properties=pika.BasicProperties(content_type="application/json")
                )
                print(f"[DISPATCH #{msg_count:02d}] Key: '{rk}' -> Payload: {body}")
                time.sleep(interval)
            print("--- D1 CANONICAL KEYS COMPLETE. CONTINUING RANDOM STREAM ---\n")

        while True:
            msg_count += 1
            if max_messages and msg_count > max_messages:
                print(f"[DISPATCHER] Reached max message limit ({max_messages}). Stopping.")
                break

            city = random.choice(CITIES)
            zone = random.choice(ZONES)
            typ = random.choice(TYPES)
            routing_key = f"{city}.{zone}.{typ}"
            req_id = f"REQ-{msg_count:04d}"
            ts = datetime.now(timezone.utc).isoformat()

            # Every 10th message is a poison pill (missing 'zone' field) for D3 verification
            is_poison_pill = (msg_count % 10 == 0)

            if is_poison_pill:
                payload = {
                    "id": req_id,
                    "city": city,
                    # 'zone' is intentionally omitted!
                    "type": typ,
                    "ts": ts,
                    "_poison_pill": True
                }
                print(f"\n⚠️  [POISON PILL #{msg_count}] Emitting malformed request (missing 'zone'):")
            else:
                payload = {
                    "id": req_id,
                    "city": city,
                    "zone": zone,
                    "type": typ,
                    "ts": ts
                }

            body = json.dumps(payload)
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(content_type="application/json")
            )
            print(f"[DISPATCH #{msg_count:03d}] Published to '{routing_key}': {payload}")
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[DISPATCHER] Interrupted by user. Shutting down...")
    finally:
        if connection.is_open:
            connection.close()
        print("[DISPATCHER] Connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Food/Ride Dispatcher")
    parser.add_argument("--url", default=DEFAULT_AMQP_URL, help="AMQP Broker URL")
    parser.add_argument("--interval", type=float, default=2.0, help="Interval in seconds between dispatches")
    parser.add_argument("--count", type=int, default=None, help="Stop after publishing N messages")
    parser.add_argument("--test-d1", action="store_true", help="Publish the 6 canonical D1 assignment keys first")
    args = parser.parse_args()

    run_dispatcher(url=args.url, interval=args.interval, max_messages=args.count, test_d1=args.test_d1)
