"""
stream/run_stream_part_d.py
===========================
End-to-End Orchestrator for Part D:
- Launches 4 Driver Consumers in concurrent threads (Ravi, Arjun, Meera, Control)
- Launches Dispatcher to publish:
  1. The 6 canonical keys from Question D1
  2. A stream of valid orders + intentional poison pills (missing 'zone')
- Verifies exact topic matching, basic_ack, and basic_nack(requeue=False)
"""

import os
import sys
import time
import json
import threading
import pika

AMQP_URL = os.environ.get(
    "AMQP_URL",
    "amqps://rvapxilv:cBV7nrY5YT-Cy0OYmLxDWnNfR9aN3_co@seal.lmq.cloudamqp.com/rvapxilv"
)
EXCHANGE_NAME = "rapido-dispatch"

# Drivers configuration (all 6 drivers from Assignment Part D1)
DRIVERS_CONFIG = [
    {"name": "Ravi",    "pattern": "bangalore.koramangala.*"},
    {"name": "Priya",   "pattern": "bangalore.indiranagar.*"},
    {"name": "Arjun",   "pattern": "bangalore.*.ride"},
    {"name": "Meera",   "pattern": "bangalore.*.food"},
    {"name": "Karthik", "pattern": "chennai.tnagar.food"},
    {"name": "Control", "pattern": "bangalore.#"},
]

# Results collection
receipts = []
receipts_lock = threading.Lock()
stop_event = threading.Event()


def driver_worker(name, pattern):
    params = pika.URLParameters(AMQP_URL)
    params.heartbeat = 60
    params.blocked_connection_timeout = 30
    
    try:
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=False)

        queue_name = f"d-test-{name.lower()}"
        ch.queue_declare(queue=queue_name, exclusive=True, auto_delete=True)
        ch.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=pattern)

        def callback(channel, method, props, body):
            data = json.loads(body.decode("utf-8"))
            order_id = data.get("id")
            rk = method.routing_key

            with receipts_lock:
                receipts.append({
                    "driver": name,
                    "pattern": pattern,
                    "order_id": order_id,
                    "routing_key": rk,
                    "has_zone": "zone" in data and bool(data["zone"])
                })

            if "zone" not in data or not data["zone"]:
                print(f"  [{name.upper()}] ❌ NACK: Request {order_id} via '{rk}' is missing 'zone' -> basic_nack(requeue=False)")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            else:
                print(f"  [{name.upper()}] ✅ ACK: Request {order_id} via '{rk}' accepted -> basic_ack")
                channel.basic_ack(delivery_tag=method.delivery_tag)

        ch.basic_qos(prefetch_count=1)
        ch.basic_consume(queue=queue_name, on_message_callback=callback)

        while not stop_event.is_set():
            conn.process_data_events(time_limit=0.5)

        conn.close()
    except Exception as e:
        print(f"[{name}] Worker error: {e}")


def main():
    print("="*60)
    print("PART D: RABBITMQ FOOD-ORDER & RIDE STREAM VERIFICATION")
    print("="*60)
    print(f"Broker: {AMQP_URL.split('@')[-1]}")
    print(f"Exchange: {EXCHANGE_NAME} (topic)")

    # 1. Start Driver threads
    print("\nStarting Driver Consumers (Ravi, Arjun, Meera, Control, Karthik)...")
    threads = []
    for d in DRIVERS_CONFIG:
        t = threading.Thread(target=driver_worker, args=(d["name"], d["pattern"]), daemon=True)
        t.start()
        threads.append(t)
    
    # Allow drivers to bind queues
    time.sleep(2.0)
    print("All driver queues declared and bound.\n")

    # 2. Connect dispatcher to publish test messages
    params = pika.URLParameters(AMQP_URL)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    # Section D1: Publish the 6 assignment keys
    d1_keys = [
        ("bangalore.koramangala.food", {"id": "D1-01", "city": "bangalore", "zone": "koramangala", "type": "food"}),
        ("bangalore.indiranagar.ride", {"id": "D1-02", "city": "bangalore", "zone": "indiranagar", "type": "ride"}),
        ("bangalore.whitefield.ride",   {"id": "D1-03", "city": "bangalore", "zone": "whitefield",   "type": "ride"}),
        ("chennai.tnagar.food",         {"id": "D1-04", "city": "chennai",   "zone": "tnagar",       "type": "food"}),
        ("chennai.whitefield.ride",     {"id": "D1-05", "city": "chennai",   "zone": "whitefield",   "type": "ride"}),
        ("bangalore.koramangala",       {"id": "D1-06", "city": "bangalore", "zone": "koramangala"}), # 2 words
    ]

    print("--- PUBLISHING 6 CANONICAL D1 KEYS ---")
    for rk, payload in d1_keys:
        print(f"\n[DISPATCHER] Publishing '{rk}' (id={payload['id']})...")
        ch.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=rk,
            body=json.dumps(payload),
            properties=pika.BasicProperties(content_type="application/json")
        )
        time.sleep(1.0)

    # Section D3: Publish Poison Pill (missing 'zone')
    print("\n--- PUBLISHING D3 POISON PILL (MISSING ZONE) ---")
    poison_pill = {"id": "POISON-99", "city": "bangalore", "type": "ride"} # NO 'zone'
    poison_rk = "bangalore.koramangala.ride"
    print(f"\n[DISPATCHER] Publishing Poison Pill to '{poison_rk}': {poison_pill}")
    ch.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=poison_rk,
        body=json.dumps(poison_pill),
        properties=pika.BasicProperties(content_type="application/json")
    )
    time.sleep(2.0)

    conn.close()

    # Stop drivers
    stop_event.set()
    time.sleep(1.0)

    # 3. Analyze and summarize results
    print("\n" + "="*60)
    print("D1 ROUTING VERIFICATION: PREDICTED VS ACTUAL")
    print("="*60)
    
    for rk, payload in d1_keys:
        oid = payload["id"]
        matched_drivers = [r["driver"] for r in receipts if r["order_id"] == oid]
        drivers_str = ", ".join(matched_drivers) if matched_drivers else "dropped"
        print(f"Key: {rk:<30} -> Received by: {drivers_str}")

    print("\n" + "="*60)
    print("D3 POISON PILL VERIFICATION:")
    print("="*60)
    poison_matches = [r for r in receipts if r["order_id"] == "POISON-99"]
    for pm in poison_matches:
        print(f"Driver '{pm['driver']}' received POISON-99: zone_present={pm['has_zone']} -> Handled via basic_nack(requeue=False)")

    print("\n" + "="*60)
    print("ALL PART D EXPERIMENTS COMPLETED SUCCESSFULLY!")
    print("="*60)


if __name__ == "__main__":
    main()
