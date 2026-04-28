"""
consumer.py
-----------
Core of the pipeline. Reads events from Kafka, runs them through
the validation engine, and routes them:

    VALID event   →  clean_events table  (trusted data)
    INVALID event →  bad_events table    (quarantine / investigation)

Every 50 events, a quality metrics summary is written to PostgreSQL
so you can track bad data rate over time.

Run:  python consumer/consumer.py
"""

import json
import sys
import os
from collections import Counter

# Make sure imports from sibling folders work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka import KafkaConsumer
from consumer.validator import validate_event_collect_errors
from consumer.db_writer import (
    get_connection,
    write_clean_event,
    write_bad_event,
    write_quality_metrics,
    write_failure_metrics,
    write_quality_alert,
)
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    METRICS_INTERVAL,
    BAD_RATE_ALERT_THRESHOLD_PCT,
    TOP_FAILURE_REASONS_LIMIT,
)


def run_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id="quality-validator-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    conn = get_connection()

    total = 0
    clean_count = 0
    bad_count = 0
    interval_failure_counts = Counter()

    print(f"[Consumer] Listening on topic '{KAFKA_TOPIC}'...\n")

    try:
        for message in consumer:
            event = message.value
            total += 1

            is_valid, errors = validate_event_collect_errors(event)

            if is_valid:
                write_clean_event(conn, event)
                clean_count += 1
                print(f"[CLEAN] event_id={event['event_id']} | "
                      f"type={event['event_type']} | amount={event['amount']} {event['currency']}")
            else:
                reason = " | ".join(errors)
                write_bad_event(conn, event, reason)
                bad_count += 1
                interval_failure_counts.update(errors)
                print(f"[BAD ] event_id={event.get('event_id', 'N/A')} | "
                      f"REJECTED: {reason}")

            # Write summary metrics every N events
            if total % METRICS_INTERVAL == 0:
                write_quality_metrics(conn, total, clean_count, bad_count)
                write_failure_metrics(conn, total, clean_count, bad_count, dict(interval_failure_counts))
                bad_rate = round((bad_count / total) * 100, 1)

                top_reasons = interval_failure_counts.most_common(TOP_FAILURE_REASONS_LIMIT)
                print(f"\n--- Quality Report (last {total} events) ---")
                print(f"    Clean : {clean_count} ({100 - bad_rate}%)")
                print(f"    Bad   : {bad_count} ({bad_rate}%)")
                if top_reasons:
                    print(f"    Top failure reasons (last {METRICS_INTERVAL} events):")
                    for idx, (failure_reason, failure_count) in enumerate(top_reasons, start=1):
                        print(f"      {idx}. {failure_reason} -> {failure_count}")
                if bad_rate > BAD_RATE_ALERT_THRESHOLD_PCT:
                    write_quality_alert(
                        conn,
                        total=total,
                        clean=clean_count,
                        bad=bad_count,
                        bad_rate_pct=bad_rate,
                        threshold_pct=BAD_RATE_ALERT_THRESHOLD_PCT,
                    )
                    print(
                        f"    ALERT : Bad rate {bad_rate}% crossed threshold "
                        f"{BAD_RATE_ALERT_THRESHOLD_PCT}%"
                    )
                print(f"--------------------------------------------\n")
                interval_failure_counts.clear()

    except KeyboardInterrupt:
        print(f"\n[Consumer] Stopped.")
        print(f"Final stats → Total: {total} | Clean: {clean_count} | Bad: {bad_count}")
    finally:
        conn.close()
        consumer.close()


if __name__ == "__main__":
    run_consumer()
