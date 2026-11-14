"""
stream/driver.py
================
Driver Consumer for RabbitMQ Topic Exchange (rapido-dispatch).

Usage:
    python driver.py NAME PATTERN [--url URL]

Examples:
    python driver.py Ravi "bangalore.koramangala.*"
    python driver.py Priya "bangalore.indiranagar.*"
    python driver.py Arjun "bangalore.*.ride"
    python driver.py Meera "bangalore.*.food"
    python driver.py Karthik "chennai.tnagar.food"
    python driver.py Control "bangalore.#"

ACK/NACK Protocol (Part D3):
- Valid messages: acknowledged with basic_ack.
- Malformed messages (missing 'zone'): rejected with basic_nack(requeue=False).
"""

import os
import sys
import json
import argparse
import pika

DEFAULT_AMQP_URL = os.environ.get(
    "AMQP_URL",
    "amqps://rvapxilv:cBV7nrY5YT-Cy0OYmLxDWnNfR9aN3_co@seal.lmq.cloudamqp.com/rvapxilv"
)
EXCHANGE_NAME = "rapido-dispatch"


def create_connection(url):
    params = pika.URLParameters(url)
    params.heartbeat = 60
    params.blocked_connection_timeout = 30
    return pika.BlockingConnection(params)


def run_driver(name, pattern, url=DEFAULT_AMQP_URL, max_messages=None):
    print(f"[{name.upper()}] Initializing driver with binding pattern: '{pattern}'...")
    connection = create_connection(url)
    channel = connection.channel()

    # Declare topic exchange
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type="topic",
        durable=False
    )

    # Declare unique private queue for this driver
    queue_name = f"driver-{name.lower()}"
    channel.queue_declare(queue=queue_name, exclusive=False, auto_delete=True)

    # Bind queue to exchange with driver's pattern
    channel.queue_bind(
        exchange=EXCHANGE_NAME,
        queue=queue_name,
        routing_key=pattern
    )
    print(f"[{name.upper()}] Bound queue '{queue_name}' to '{EXCHANGE_NAME}' on key '{pattern}'.")
    print(f"[{name.upper()}] Waiting for incoming dispatch events. To exit press CTRL+C.\n")

    msg_count = [0]

    def on_message(ch, method, properties, body):
        msg_count[0] += 1
        try:
            data = json.loads(body.decode("utf-8"))
        except Exception as e:
            print(f"[{name.upper()}] ❌ JSON Decode Error: {e}. Dropping.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        order_id = data.get("id", "UNKNOWN")
        routing_key = method.routing_key

        # Part D3 Quality Check: Reject if 'zone' is missing!
        if "zone" not in data or not data["zone"]:
            print(f"[{name.upper()}] 🚫 NACK (REJECTED): Request {order_id} via '{routing_key}' has MISSING 'zone'! "
                  f"Discarded with basic_nack(requeue=False).")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        else:
            city = data.get("city")
            zone = data.get("zone")
            typ = data.get("type", "order")
            print(f"[{name.upper()}] ✅ ACK (ACCEPTED): Request {order_id} via '{routing_key}' -> "
                  f"Destination: {city.title()} ({zone.title()}), Service: {typ.title()}. Confirmed.")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        if max_messages and msg_count[0] >= max_messages:
            print(f"[{name.upper()}] Processed {max_messages} messages. Stopping consumer.")
            ch.stop_consuming()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=on_message)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"\n[{name.upper()}] Stopping driver consumer...")
        channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()
        print(f"[{name.upper()}] Connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Driver RabbitMQ Consumer")
    parser.add_argument("name", help="Driver name (e.g. Ravi, Priya, Arjun, Meera, Karthik, Control)")
    parser.add_argument("pattern", help="AMQP topic binding pattern (e.g. 'bangalore.koramangala.*')")
    parser.add_argument("--url", default=DEFAULT_AMQP_URL, help="AMQP Broker URL")
    parser.add_argument("--count", type=int, default=None, help="Stop after receiving N messages")
    args = parser.parse_args()

    run_driver(args.name, args.pattern, url=args.url, max_messages=args.count)
