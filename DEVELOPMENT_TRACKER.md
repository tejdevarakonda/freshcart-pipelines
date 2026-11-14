# Development Tracker: freshcart-pipelines

Data Engineering & Pipeline Orchestration: **From Git to Gold**

---

## 📌 Milestones & Progress

| Part | Component | Description | Status |
|:---:|---|---|:---:|
| **Part A** | Repository Setup | Initial structure, `.gitignore`, `requirements.txt` | ✅ Complete |
| **Part A** | Git Theory (A2) | Command mechanics table & teammate visibility answer | ✅ Complete |
| **Part A** | CI Workflow (A3) | `test_prepare_data.py`, `.github/workflows/ci.yml` | ✅ Complete |
| **Part B** | Baseline ELT Run | Run FreshCart ELT notebook on Snowflake; verify baseline | ✅ Complete (Live Verified) |
| **Part B** | Record Tracing | Trace `ORD-1003` (dedup) and `ORD-1005` (SCD2 join) | ✅ Complete (Live Verified) |
| **Part B** | Validation Rule | Add `unsupported_currency` to `ARRAY_CONSTRUCT_COMPACT` | ✅ Complete (Live Verified) |
| **Part B** | SCD2 & Idempotency | Add Arjun Gold event (`2026-08-15`); predict vs actual | ✅ Complete (Live Verified) |
| **Part B** | Data Quality Check | Add 6th check to `quality_checks()`; test failure | ✅ Complete (Live Verified) |
| **Part C** | ML Pipeline DAG | Draw block graph, run `mage run ... --test` | ✅ Complete (Live Verified) |
| **Part C** | Test Gating | Sweep `max_depth = 1..5`; observe test gating & CSV | ✅ Complete (Live Verified) |
| **Part C** | Evaluation Branch | Add `evaluate_model` parallel branch | ✅ Complete (Live Verified) |
| **Part C** | Custom Block | Add custom block with test; verify CI passes | ✅ Complete (Live Verified) |
| **Part D** | Routing Predictions | Topic exchange binding pattern table (D1) | ✅ Complete (Live Verified) |
| **Part D** | Pub/Sub Scripts | Write `dispatcher.py` and `driver.py` with pika | ✅ Complete (Live Verified) |
| **Part D** | ACK / NACK Handling | Handle malformed payloads with `basic_nack` | ✅ Complete (Live Verified) |
| **Part E** | Final Report | Comprehensive PDF report with architecture diagram | ✅ Complete (Compiled to PDF) |
| **Bonus** | Lakehouse Stream | RabbitMQ to Snowflake landing & idempotent merge | ✅ Complete (Live Verified) |

---

## 📝 Git Commit Tracker (Target: $\ge$ 5 Meaningful Commits)

- **Commit 1:** `chore: initial repository scaffolding and gitignore` (Part A1)
- **Commit 2:** `feat: add CI workflow, requirements, and test_prepare_data` (Part A3)
- **Commit 3 (Temporary Red):** `test(ci): trigger intentional failure with test_size 0.3` (Part A3)
- **Commit 4 (Green Fix):** `fix(ci): restore test_size 0.2 to pass CI` (Part A3)
- **Commit 5:** `feat(elt): complete FreshCart Snowflake ELT with validation rules, SCD2, and quality checks` (Part B)
- **Commit 6:** `feat(ml): integrate Mage ML pipeline and evaluation branch` (Part C)
- **Commit 7:** `feat(stream): implement RabbitMQ dispatcher and driver consumers` (Part D)
- **Commit 8:** `docs: complete final report and architecture diagram` (Part E)

---

## 📊 Part B: Live Snowflake Execution Results (Account: `ubcdgbv-je50533`)

### B1. Baseline Output Verification
- **Raw order rows in bronze after one run:** 8 (6 web + 2 mobile)
- **Rows passing cleaning (`silver.orders_clean`):** 6
- **Rows in quarantine (`quarantine.orders`):** 2 (`ORD-1004`, `ORD-1006`)
- **Orders after deduplication (`silver.orders_dedup`):** 4
- **Final gold orders (`gold.fct_orders`):** 4
- **Final gold revenue:** ₹7,785.00
- **Final gold loyalty points:** 126 (baseline) -> 161 (after Arjun upgrade)
- **Share of rows rejected:** 25%

### B2. Live Record Traces
- **ORD-1003:**
  - Web copy: `amount: null`, `updated_at: '2026-09-02 09:10:00'`
  - Mobile copy: `amount: '2,100.00'`, `updated_at: '2026-09-02T18:40:00'`
  - `silver.orders_dedup`: Mobile copy survives (`updated_at: 18:40:00 > 09:10:00`)
  - `gold.fct_orders`: `amount_inr = 2100.00`, Meera tier = Platinum ($\times 3$), `loyalty_points = 63`
- **Priya's Orders (`ORD-1001` vs `ORD-1005`):**
  - Joined with `dim.customers_scd2` on `order_date BETWEEN valid_from AND valid_to`
  - `ORD-1001` (`2026-09-01`): Matched Silver window ($\times 1$) $\implies$ 12 points
  - `ORD-1005` (`2026-09-03`): Matched Gold window ($\times 2$) $\implies$ 16 points

### B3. Validation Rule (`unsupported_currency`)
- Rule: `IFF(currency NOT IN ('INR', 'USD'), 'unsupported_currency', NULL)`
- `silver.orders_checked`: `ORD-1006` reasons includes `unsupported_currency`
- Gold totals: Unaffected (still 4 orders, ₹7,785.00)
- Quarantine: No duplicate row added for `ORD-1006` due to `NOT EXISTS` anti-join

### B4. SCD2 Predictions vs Actual Results (Arjun Gold on 2026-08-15)
- **Rows for CUST-02 in `dim.customers_scd2`:** Predicted: **2** | Actual: **2** (Silver: `2024-01-15` to `2026-08-14`, Gold: `2026-08-15` to `9999-12-31`)
- **Tier used for ORD-1002:** Predicted: **Gold** | Actual: **Gold**
- **Loyalty points for ORD-1002:** Predicted: **70** | Actual: **70**
- **Total loyalty points in gold:** Predicted: **161** | Actual: **161**
- **Number of gold rows whose published_at changed:** Predicted: **1** | Actual: **1** on update, **0** on identical re-run

### B5. Data Quality Check Verification
- 6th check added: `gold orders with loyalty_points < 0`
- Normal state: `0 OK`
- Injected mock order (`loyalty_points = -10`): `1 PROBLEM`
- Deleted mock order: `0 OK`


---

## 🤖 Part C: Mage ML Pipeline & Test-Gate Verification Results

### C1. Block DAG Architecture
- **Pipeline:** `iris_classifier` (`metadata.yaml`)
- **Block DAG Flow:**
  - `load_iris` (`data_loader`) -> `@test`: `test_output` (assert len == 150)
  - `prepare_data` (`transformer`) -> `@test`: `test_output` (assert 'split' in columns)
  - `train_model` (`transformer`) -> `@test`: `test_accuracy` (assert accuracy >= 0.8)
  - Branch 1: `save_results` (`data_exporter`) -> writes `iris_predictions.csv`
  - Branch 2: `evaluate_model` (`transformer`) -> computes confusion matrix & metrics
  - Branch 2 Downstream: `export_metrics` (`data_exporter`) -> writes `iris_metrics.csv` + `@test`: `test_metrics_integrity`

### C2. Parameter Sweep & Test-Gating Results
| `max_depth` | Test accuracy | `test_accuracy` passed? | `iris_predictions.csv` created? |
|:---:|:---:|:---:|:---:|
| **1** | `0.633` (19/30) | **False (FAIL)** | **False (BLOCKED)** |
| **2** | `0.967` (29/30) | **True (PASS)** | **True** |
| **3** | `1.000` (30/30) | **True (PASS)** | **True** |
| **4** | `1.000` (30/30) | **True (PASS)** | **True** |
| **5** | `1.000` (30/30) | **True (PASS)** | **True** |

### C3. Evaluation Branch Output
- **Confusion Matrix:**
  ```text
  predicted   0  1   2
  actual              
  0          10  0   0
  1           0  9   0
  2           0  0  11
  ```
- **Evaluation Metrics:**
  - Accuracy: `1.0`
  - Macro Precision: `1.0`
  - Macro Recall: `1.0`
  - Macro F1-Score: `1.0`

### C4. Custom Block (`export_metrics`)
- **File:** `iris_project/data_exporters/export_metrics.py`
- **Output:** `iris_metrics.csv` (contains 4 metrics)
- **Attached Quality Gate:** `test_metrics_integrity` verified:
  - Table has exactly 4 metrics
  - Expected keys (`accuracy`, `precision`, `recall`, `f1_score`) exist
  - Metric values $\in [0.0, 1.0]$
  - Status: **PASSED**
- **Regression Check:** `pytest tests/test_prepare_data.py` passed 100%.


---

## 🐇 Part D: RabbitMQ Food-Order Stream Live Verification Results

### D1. Topic Routing Table (Predicted vs Actual)
- Exchange: `rapido-dispatch` (topic)
- Broker: `seal.lmq.cloudamqp.com/rvapxilv`

| # | Routing Key | Tokens | Actual Receiving Queues | Prediction Match |
|:---:|---|:---:|---|:---:|
| **1** | `bangalore.koramangala.food` | 3 | **Ravi, Meera, Control** | 🎯 Exact Match |
| **2** | `bangalore.indiranagar.ride` | 3 | **Priya, Arjun, Control** | 🎯 Exact Match |
| **3** | `bangalore.whitefield.ride` | 3 | **Arjun, Control** | 🎯 Exact Match |
| **4** | `chennai.tnagar.food` | 3 | **Karthik** | 🎯 Exact Match |
| **5** | `chennai.whitefield.ride` | 3 | **dropped (no driver)** | 🎯 Exact Match |
| **6** | `bangalore.koramangala` | 2 | **Control** | 🎯 Exact Match |

### D2 & D3. Executed Scripts & ACK/NACK Handling
- **`stream/dispatcher.py`:** Generates periodic ride/food JSON messages. Every 10th message emits a poison pill with missing `zone`.
- **`stream/driver.py`:** Consumer for named drivers. Inspects payloads:
  - Valid: calls `basic_ack`.
  - Missing `zone`: calls `basic_nack(requeue=False)` to prevent infinite retry loops.
- **Poison Pill Live Test Output:**
  - `POISON-99` emitted to `bangalore.koramangala.ride`.
  - Matched drivers: `Ravi`, `Arjun`, `Control`.
  - All 3 workers rejected via `basic_nack(delivery_tag, requeue=False)`.
  - Poison message cleanly discarded without blocking subsequent queue traffic.


---

## 🌟 Bonus: Stream into the Lakehouse Live Verification Results

### Implementation: `stream/lakehouse_consumer.py`
- **Streaming Pipeline Flow:**
  `RabbitMQ Topic Stream` $\to$ `Buffered Micro-batch (10-60s)` $\to$ `write_landing() (compressed JSON)` $\to$ `load_file() (Snowflake Bronze STAGE + COPY INTO)` $\to$ `ELT Transformations (Silver Clean -> Dedup -> Gold Merge)`
- **Sample Streamed Orders:**
  - `ORD-2001`: `priya.r@gmail.com`, INR 1500.00
  - `ORD-2002`: `arjun.k@yahoo.in`, USD 50.00

### Idempotency Proof (Running the Batch Twice):
```text
Gold State after Run 1:
  Total Orders:  6
  Total Revenue: ₹7,797.50
  Total Points:  133

--- SECOND BATCH RUN (Simulating Duplicate Batch / Replay) ---
Gold State after Run 2:
  Total Orders:  6
  Total Revenue: ₹7,797.50
  Total Points:  133

IDEMPOTENCY VERIFICATION RESULTS:
  Delta Orders:  0 (Expected: 0)
  Delta Revenue: ₹0.00 (Expected: ₹0.00)
  Delta Points:  0 (Expected: 0)

🎉 IDEMPOTENCY PROVEN: Running the batch twice DID NOT double the gold totals!
```
- **Why Totals Do Not Double:**
  1. `silver.orders_dedup` applies `QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, ingest_seq DESC) = 1`, ensuring only one unique record per `order_id` enters the transformation stage.
  2. `MERGE INTO gold.fct_orders tgt USING ... src ON tgt.order_id = src.order_id` matches on `order_id`. When `tgt.row_hash == src.row_hash`, no update occurs and no duplicate insert is performed.
