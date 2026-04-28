# Real-Time Data Quality Validation Pipeline

A streaming data pipeline that ingests high-volume e-commerce events from Apache Kafka, applies a multi-layer validation engine, routes clean and bad data to separate PostgreSQL tables, and tracks quality metrics over time.

Built to demonstrate real-world data engineering patterns: event streaming, data quality gates, quarantine zones, and observability.

---

## Architecture

```
[Event Producer]
     |
     |  (5 events/sec, ~20% intentionally malformed)
     v
[Apache Kafka]  ←── topic: ecommerce-events
     |
     v
[Validation Engine]
     |
     |── VALID ──►  clean_events table   (trusted data zone)
     |
     └── INVALID ►  bad_events table     (quarantine zone)
                         +
                    quality_metrics table (bad rate over time)
```

---

## Validation Rules

Each rule is an independent function in `consumer/validator.py`:

| Rule | Description |
|------|-------------|
| Required fields | All 7 fields must be present |
| Null check | No field can be null or empty |
| Amount check | Must be a positive number |
| Event type | Must be one of: page_view, add_to_cart, purchase, wishlist |
| Currency | Must be one of: USD, INR, EUR, GBP |
| Timestamp | Must be valid ISO 8601 format |

Validation now runs in **collect-all-errors mode** inside the consumer, so a bad record can capture multiple rejection reasons instead of only the first failed rule.

---

## Tech Stack

- **Apache Kafka** — event streaming
- **Python** — producer, consumer, validation engine
- **PostgreSQL** — clean data store, quarantine store, metrics
- **Docker** — local infrastructure (Kafka + Zookeeper + PostgreSQL)

---

## Project Structure

```
data-quality-pipeline/
├── docker-compose.yml          # Spins up Kafka + Zookeeper + PostgreSQL
├── requirements.txt
├── config/
│   └── settings.py             # All config in one place
├── producer/
│   └── producer.py             # Generates and publishes fake events
├── consumer/
│   ├── consumer.py             # Reads from Kafka, routes events
│   ├── validator.py            # All validation rules
│   └── db_writer.py            # PostgreSQL write logic
├── tests/
│   └── test_validator.py       # Unit tests (pytest)
└── sql/
    └── init.sql                # Table definitions
```

---

## Tests

```bash
python -m pytest -q
```

---

## How to Run

### 1. Start infrastructure

```bash
docker-compose up -d
```

Wait ~15 seconds for Kafka and PostgreSQL to be ready.

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Start the consumer (Terminal 1)

```bash
python consumer/consumer.py
```

### 4. Start the producer (Terminal 2)

```bash
python producer/producer.py
```

You will see clean and bad events being processed in real time in the consumer terminal.

### 5. Check results in PostgreSQL

```bash
docker exec -it postgres psql -U pipeline_user -d ecommerce_quality
```

```sql
-- See clean events
SELECT * FROM clean_events LIMIT 10;

-- See rejected events and why
SELECT event_id, rejection_reason, ingested_at FROM bad_events LIMIT 10;

-- See quality metrics over time
SELECT * FROM quality_metrics ORDER BY batch_time DESC;

-- See top validation failures by reason
SELECT batch_time, failure_reason, failure_count
FROM validation_failure_metrics
ORDER BY batch_time DESC, failure_count DESC
LIMIT 20;

-- See bad-rate spike alerts
SELECT alert_time, bad_rate_pct, threshold_pct, message
FROM quality_alerts
ORDER BY alert_time DESC
LIMIT 20;
```

---

## Sample Output

```
[Consumer] Listening on topic 'ecommerce-events'...

[CLEAN] event_id=a1b2c3.. | type=purchase     | amount=129.99 USD
[BAD ] event_id=d4e5f6.. | REJECTED: Invalid amount: -45.0 (must be > 0)
[CLEAN] event_id=g7h8i9.. | type=add_to_cart  | amount=39.99 INR
[BAD ] event_id=j1k2l3.. | REJECTED: Unknown event_type: 'hover'

--- Quality Report (last 50 events) ---
    Clean : 41 (82.0%)
    Bad   : 9  (18.0%)
----------------------------------------
```

---

## Key Design Decisions

**Why separate clean and bad tables?**
In production data pipelines, bad data should never silently drop — it goes to a quarantine zone for investigation and reprocessing. This mirrors the medallion architecture (Bronze → Silver → Gold) used at scale.

**Why modular validation rules?**
Each rule is an independent function. Adding a new rule means adding one function and one line to `ALL_RULES`. No other code changes needed.

**Why track quality metrics separately?**
The `quality_metrics` table lets you monitor bad data rate trends over time — if it spikes from 5% to 30%, something upstream broke. This is the foundation of data observability.

**Why collect all rule failures instead of fail-fast only?**
A single malformed event can violate multiple constraints. Capturing all failures gives faster root-cause analysis, better producer feedback loops, and richer quality dashboards.

**Why add quality alerts and failure-metrics tables?**
Teams need more than pass/fail counts. Rule-level failure trends and threshold-based alerts make this pipeline act like a real production observability system.

---

## Current Status and Limitations

Current implementation focuses on the core stream-quality loop (ingest -> validate -> route -> measure) and is intentionally optimized for local development.

Known limitations:

- Single-broker Kafka setup in Docker Compose (not HA)
- Consumer writes directly to PostgreSQL (no buffering/batch sink yet)
- No schema registry / schema evolution contract
- No dashboard layer yet (metrics are queryable in SQL)
- Basic alerting via DB table only (no Slack/email/webhook integration)

---

## Practical Next Steps

Suggested next iterations if you extend this repo further:

1. Extend `pytest` coverage to DB writes (validators are covered today).
2. Add a Kafka dead-letter topic (DLQ) for poison messages (beyond quarantining rows in Postgres).
3. Add dashboard/monitoring (Grafana/Metabase) using the metrics tables.
4. Add a lightweight CI smoke test (infra up + bounded producer sample).

Operational note: Postgres write retries are implemented in `consumer/db_writer.py`.

---

## Implementation Notes

- Validation rules are kept as isolated functions to avoid tight coupling and simplify future changes.
- The consumer uses collect-all-errors mode for richer failure diagnostics and faster triage.
- Metrics are interval-based to keep the write pattern simple while still exposing quality drift over time.
- SQL tables are separated by trust level (clean vs quarantine) to mirror real warehouse quality zones.

---
