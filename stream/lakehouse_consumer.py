"""
stream/lakehouse_consumer.py
============================
Bonus (+10 Marks): Stream into the Lakehouse
--------------------------------------------
Connects RabbitMQ streaming event consumer to Snowflake ELT pipeline:
1. Receives streaming FreshCart orders from RabbitMQ topic/queue.
2. Buffers messages for a micro-batch interval (e.g. 10-60 seconds).
3. Writes buffered batch to compressed JSON landing stage: write_landing().
4. Stages and loads file into Snowflake Bronze: load_file().
5. Executes the ELT transformation chain into silver, dim, ref, and gold.
6. Proves Idempotency: Re-running the batch does NOT double gold totals!
"""

import os
import sys
import gzip
import json
import time
from datetime import datetime, timezone
import pika
import snowflake.connector

# RabbitMQ configuration
AMQP_URL = os.environ.get(
    "AMQP_URL",
    "amqps://rvapxilv:cBV7nrY5YT-Cy0OYmLxDWnNfR9aN3_co@seal.lmq.cloudamqp.com/rvapxilv"
)
EXCHANGE_NAME = "rapido-dispatch"
ROUTING_KEY = "freshcart.orders.#"
QUEUE_NAME = "freshcart-lakehouse-stream"

# Snowflake configuration
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER", "TEJDEVARAKONDA")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "DevaTejpranay1978")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "ubcdgbv-je50533")
SNOWFLAKE_WAREHOUSE = "FRESHCART_WH"
SNOWFLAKE_DATABASE = "FRESHCART"
SNOWFLAKE_ROLE = "ACCOUNTADMIN"

# Local landing path
LANDING_DIR = r"C:\Users\lenovo\.gemini\antigravity\scratch\freshcart_landing"


def get_snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema="BRONZE",
        role=SNOWFLAKE_ROLE
    )


def publish_sample_stream_orders():
    """Publishes sample streamed FreshCart orders to RabbitMQ."""
    print("\n[PRODUCER] Publishing streamed FreshCart orders to RabbitMQ...")
    params = pika.URLParameters(AMQP_URL)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=False)

    sample_orders = [
        {
            "order_id": "ORD-2001",
            "customer_email": "priya.r@gmail.com",
            "customer_name": "Priya R",
            "order_date": "2026-09-04",
            "amount": 1500.00,
            "currency": "INR",
            "pincode": "600041",
            "updated_at": "2026-09-04 10:15:00",
            "source": "stream"
        },
        {
            "order_id": "ORD-2002",
            "customer_email": "arjun.k@yahoo.in",
            "customer_name": "Arjun K",
            "order_date": "2026-09-04",
            "amount": 50.00,
            "currency": "USD",
            "pincode": "560034",
            "updated_at": "2026-09-04 11:30:00",
            "source": "stream"
        }
    ]

    for order in sample_orders:
        rk = f"freshcart.orders.{order['order_id'].lower()}"
        ch.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=rk,
            body=json.dumps(order),
            properties=pika.BasicProperties(content_type="application/json")
        )
        print(f"  Published order {order['order_id']} ({order['customer_email']}, amount: {order['amount']} {order['currency']}) to '{rk}'")

    conn.close()
    return sample_orders


def consume_and_buffer_orders(buffer_duration=10):
    """
    Subscribes to RabbitMQ queue and buffers incoming order events for buffer_duration seconds.
    """
    print(f"\n[CONSUMER] Connecting to RabbitMQ and buffering orders for {buffer_duration} seconds...")
    params = pika.URLParameters(AMQP_URL)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=False)
    ch.queue_declare(queue=QUEUE_NAME, exclusive=False, auto_delete=True)
    ch.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)

    buffered_orders = []
    start_time = time.time()

    def on_message(channel, method, props, body):
        data = json.loads(body.decode("utf-8"))
        buffered_orders.append(data)
        print(f"  [BUFFERED] Received order {data.get('order_id')} from stream.")
        channel.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    while (time.time() - start_time) < buffer_duration:
        conn.process_data_events(time_limit=0.5)

    conn.close()
    print(f"[CONSUMER] Buffer window closed. Collected {len(buffered_orders)} orders.")
    return buffered_orders


def write_landing(source, records, run_id):
    """Writes buffered records to local landing directory as compressed JSON."""
    source_dir = os.path.join(LANDING_DIR, source)
    os.makedirs(source_dir, exist_ok=True)
    filename = f"{source}__{run_id}.json.gz"
    path = os.path.join(source_dir, filename)

    payload = b"\n".join(json.dumps(r).encode("utf-8") for r in records)
    with gzip.open(path, "wb") as f:
        f.write(payload)

    print(f"[LANDING] Buffered {len(records)} records -> {path}")
    return path


def load_file_and_transform(filepath, source, run_id):
    """Stages file to Snowflake, copies into bronze, and runs ELT transformation."""
    conn = get_snowflake_connection()
    cur = conn.cursor()

    unix_path = filepath.replace("\\", "/")
    print(f"[SNOWFLAKE] Staging file via PUT file://{unix_path} @bronze.landing/{source}/...")
    cur.execute(f"PUT file://{unix_path} @bronze.landing/{source}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

    filename = os.path.basename(filepath)
    print(f"[SNOWFLAKE] Loading into bronze.orders_raw...")
    copy_sql = f"""
    COPY INTO bronze.orders_raw (run_id, ingest_seq, source, raw)
    FROM (
        SELECT '{run_id}', METADATA$FILE_ROW_NUMBER, '{source}', $1
        FROM @bronze.landing/{source}/{filename}
    )
    """
    cur.execute(copy_sql)

    print("[SNOWFLAKE] Running ELT Transformations (Silver Clean, Dedup, Gold Merge)...")
    
    # 1. Silver Clean
    sql_silver_clean = f"""
    SET run_id = '{run_id}';
    CREATE OR REPLACE TEMPORARY TABLE typed AS
    SELECT ingest_seq, source,
           COALESCE(raw:order_id::STRING, raw:orderId::STRING) AS order_id,
           LOWER(TRIM(COALESCE(raw:customer_email::STRING, raw:user.email::STRING))) AS email,
           INITCAP(TRIM(COALESCE(raw:customer_name::STRING, raw:user.name::STRING))) AS name,
           COALESCE(TRY_TO_DATE(raw:order_date::STRING), TRY_TO_DATE(raw:orderDate::STRING)) AS order_date,
           COALESCE(TRY_TO_NUMBER(raw:amount::STRING, 12, 2), TRY_TO_NUMBER(REPLACE(raw:total.value::STRING, ',', ''), 12, 2)) AS amount,
           UPPER(TRIM(COALESCE(raw:currency::STRING, raw:total.curr::STRING))) AS currency,
           TRIM(COALESCE(raw:pincode::STRING, raw:deliveryPincode::STRING)) AS pincode,
           COALESCE(TRY_TO_TIMESTAMP_NTZ(raw:updated_at::STRING), TRY_TO_TIMESTAMP_NTZ(raw:updatedAt::STRING)) AS updated_at
    FROM bronze.orders_raw;

    CREATE OR REPLACE TABLE silver.orders_checked AS
    SELECT *,
      ARRAY_CONSTRUCT_COMPACT(
        IFF(email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{{2,}}$', 'bad_email', NULL),
        IFF(pincode IS NOT NULL AND pincode NOT RLIKE '^[1-9][0-9]{{5}}$', 'bad_pincode', NULL),
        IFF(updated_at IS NULL, 'unparseable_updated_at', NULL),
        IFF(amount <= 0, 'non_positive_amount', NULL),
        IFF(currency NOT IN ('INR', 'USD'), 'unsupported_currency', NULL)
      ) AS reasons
    FROM typed;

    CREATE OR REPLACE TABLE silver.orders_clean AS
    SELECT * EXCLUDE reasons, (amount IS NULL) AS amount_missing
    FROM silver.orders_checked
    WHERE ARRAY_SIZE(reasons) = 0;
    """
    for stmt in sql_silver_clean.strip().split(";"):
        if stmt.strip():
            cur.execute(stmt)

    # 2. Silver Dedup
    cur.execute("""
    CREATE OR REPLACE TABLE silver.orders_dedup AS
    SELECT *
    FROM silver.orders_clean
    QUALIFY ROW_NUMBER() OVER (
              PARTITION BY order_id
              ORDER BY updated_at DESC, ingest_seq DESC
            ) = 1;
    """)

    # 3. Gold Merge
    sql_gold_merge = """
    MERGE INTO gold.fct_orders tgt
    USING (
      WITH enriched AS (
        SELECT o.order_id, o.order_date,
               c.cust_id, c.tier,
               CASE c.tier WHEN 'Platinum' THEN 3 WHEN 'Gold' THEN 2 ELSE 1 END AS multiplier,
               o.amount, o.currency,
               COALESCE(fx.rate_to_inr, 1.0000) AS fx_rate,
               ROUND(o.amount * COALESCE(fx.rate_to_inr, 1.0000), 2) AS amount_inr,
               FLOOR(ROUND(o.amount * COALESCE(fx.rate_to_inr, 1.0000), 2) / 100) *
                 CASE c.tier WHEN 'Platinum' THEN 3 WHEN 'Gold' THEN 2 ELSE 1 END AS loyalty_points
        FROM silver.orders_dedup o
        LEFT JOIN dim.customers_scd2 c
               ON o.email = c.email
              AND o.order_date BETWEEN c.valid_from AND c.valid_to
        LEFT JOIN ref.fx_daily fx
               ON o.currency = fx.currency
              AND o.order_date = fx.rate_date
      )
      SELECT e.*,
             HASH(order_date, cust_id, tier, multiplier, amount, currency, fx_rate, amount_inr, loyalty_points) AS row_hash
      FROM enriched e
    ) src
    ON tgt.order_id = src.order_id
    WHEN MATCHED AND tgt.row_hash != src.row_hash THEN
      UPDATE SET order_date     = src.order_date,
                 cust_id        = src.cust_id,
                 tier           = src.tier,
                 multiplier     = src.multiplier,
                 amount         = src.amount,
                 currency       = src.currency,
                 fx_rate        = src.fx_rate,
                 amount_inr     = src.amount_inr,
                 loyalty_points = src.loyalty_points,
                 row_hash       = src.row_hash,
                 published_at   = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (order_id, order_date, cust_id, tier, multiplier,
              amount, currency, fx_rate, amount_inr, loyalty_points, published_at, row_hash)
      VALUES (src.order_id, src.order_date, src.cust_id, src.tier, src.multiplier,
              src.amount, src.currency, src.fx_rate, src.amount_inr, src.loyalty_points,
              CURRENT_TIMESTAMP(), src.row_hash);
    """
    cur.execute(sql_gold_merge)

    # Query current totals
    cur.execute("""
    SELECT COUNT(*) AS total_orders,
           SUM(amount_inr) AS total_revenue,
           SUM(loyalty_points) AS total_points
    FROM gold.fct_orders;
    """)
    totals = cur.fetchone()
    conn.close()

    return {
        "total_orders": totals[0],
        "total_revenue": float(totals[1]),
        "total_points": totals[2]
    }


def main():
    print("="*60)
    print("BONUS: STREAM INTO THE LAKEHOUSE & IDEMPOTENCY DEMO")
    print("="*60)

    # 0. Ensure queue is declared and bound to exchange BEFORE publishing!
    print("[SETUP] Declaring stream queue and binding to topic exchange...")
    params = pika.URLParameters(AMQP_URL)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=False)
    ch.queue_declare(queue=QUEUE_NAME, exclusive=False, auto_delete=True)
    ch.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)
    conn.close()

    # 1. Publish test orders to RabbitMQ
    publish_sample_stream_orders()

    # 2. Consume and buffer from RabbitMQ for 5-10 seconds
    buffered = consume_and_buffer_orders(buffer_duration=5)

    if not buffered:
        print("No orders received from stream! Exiting.")
        return

    # 3. First execution: Write landing -> Load Bronze -> Transform to Gold
    run_id_1 = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = write_landing("stream", buffered, run_id_1)

    print("\n--- FIRST BATCH RUN (Ingest & Merge) ---")
    res1 = load_file_and_transform(filepath, "stream", run_id_1)
    print(f"Gold State after Run 1:")
    print(f"  Total Orders:  {res1['total_orders']}")
    print(f"  Total Revenue: ₹{res1['total_revenue']:,.2f}")
    print(f"  Total Points:  {res1['total_points']}")

    # 4. Second execution: Re-run the EXACT same batch to prove Idempotency
    print("\n--- SECOND BATCH RUN (Simulating Duplicate Batch / Replay) ---")
    run_id_2 = datetime.now().strftime("%Y%m%d_%H%M%S") + "_replay"
    res2 = load_file_and_transform(filepath, "stream", run_id_2)
    print(f"Gold State after Run 2:")
    print(f"  Total Orders:  {res2['total_orders']}")
    print(f"  Total Revenue: ₹{res2['total_revenue']:,.2f}")
    print(f"  Total Points:  {res2['total_points']}")

    # 5. Idempotency Verification Check
    print("\n" + "="*60)
    print("IDEMPOTENCY VERIFICATION RESULTS")
    print("="*60)
    orders_diff = res2['total_orders'] - res1['total_orders']
    revenue_diff = res2['total_revenue'] - res1['total_revenue']
    points_diff = res2['total_points'] - res1['total_points']

    print(f"Delta Orders:  {orders_diff} (Expected: 0)")
    print(f"Delta Revenue: ₹{revenue_diff:.2f} (Expected: ₹0.00)")
    print(f"Delta Points:  {points_diff} (Expected: 0)")

    if orders_diff == 0 and revenue_diff == 0.0 and points_diff == 0:
        print("\n🎉 IDEMPOTENCY PROVEN: Running the batch twice DID NOT double the gold totals!")
    else:
        print("\n❌ IDEMPOTENCY FAILED: Totals changed on re-run.")


if __name__ == "__main__":
    main()
