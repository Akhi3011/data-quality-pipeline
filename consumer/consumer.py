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

import sys
import os
from collections import Counter

# Make sure imports from sibling folders work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka import KafkaConsumer
from consumer.db_writer import (
    get_connection,
    MutableConnection,
)
from consumer.pipeline_logic import consume_one_message_and_write, maybe_emit_interval_report
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    CONSUMER_GROUP_ID,
    KAFKA_AUTO_OFFSET_RESET,
)


def run_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        group_id=CONSUMER_GROUP_ID,
    )

    mconn = MutableConnection(get_connection())

    total = 0
    clean_count = 0
    bad_count = 0
    interval_failure_counts = Counter()

    print(f"[Consumer] Listening on topic '{KAFKA_TOPIC}'...\n")

    try:
        for message in consumer:
            total += 1

            clean_count, bad_count = consume_one_message_and_write(
                mconn,
                message,
                clean_count=clean_count,
                bad_count=bad_count,
                interval_failure_counts=interval_failure_counts,
            )

            maybe_emit_interval_report(
                mconn,
                total=total,
                clean_count=clean_count,
                bad_count=bad_count,
                interval_failure_counts=interval_failure_counts,
            )

    except KeyboardInterrupt:
        print(f"\n[Consumer] Stopped.")
        print(f"Final stats → Total: {total} | Clean: {clean_count} | Bad: {bad_count}")
    finally:
        try:
            mconn.conn.close()
        except Exception:
            pass
        consumer.close()


if __name__ == "__main__":
    run_consumer()
