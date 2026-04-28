"""
Offline consumer demo without Kafka.

This runs the exact same decoding + validation + DB write path used by consumer/consumer.py,
but feeds synthetic payloads generated from producer/event_factory.py.

Important: this demo still connects to Postgres using `config/settings.py` (same as the kafka consumer).

Run:

  cd /Users/teluguntiakhil/Downloads/data-quality-pipeline
  python scripts/offline_consumer_demo.py --events 200
"""

import argparse
import json
import os
import random
import sys
from collections import Counter

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from config.settings import BAD_DATA_RATIO, OFFLINE_DEMO_MAX_EVENTS  # noqa: E402
from consumer.db_writer import MutableConnection, get_connection  # noqa: E402
from consumer.pipeline_logic import (  # noqa: E402
    SimulatedKafkaMessage,
    consume_one_message_and_write,
    maybe_emit_interval_report,
)
from producer.event_factory import generate_bad_event, generate_good_event  # noqa: E402


def build_payload(random_bad_ratio: float) -> dict:
    is_bad = random.random() < random_bad_ratio
    return generate_bad_event() if is_bad else generate_good_event()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=200, help="How many simulated kafka messages")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--bad-ratio",
        type=float,
        default=BAD_DATA_RATIO,
        help="Probability a generated event starts as intentionally malformed",
    )
    parser.add_argument(
        "--inject-bad-json",
        action="store_true",
        help="Rarely emit a deliberately invalid JSON kafka payload string",
    )
    args = parser.parse_args()

    max_events = OFFLINE_DEMO_MAX_EVENTS or args.events

    random.seed(args.seed)

    mconn = MutableConnection(get_connection())

    total = 0
    clean_count = 0
    bad_count = 0
    interval_failure_counts: Counter[str] = Counter()

    print("[Offline Demo] Simulating kafka messages (Kafka not required; Postgres still required)\n")

    for i in range(1, max_events + 1):
        total += 1

        if args.inject_bad_json and i % 97 == 0:
            payload = b"{broken-json"
        else:
            payload_obj = build_payload(args.bad_ratio)

            extra_garbage = ""
            # tiny chance extra bytes after JSON -> still parseable-ish for json.loads? often fails -> good demo
            if args.inject_bad_json and i % 149 == 0:
                extra_garbage = " TRAILING-JUNK"

            payload = (json.dumps(payload_obj) + extra_garbage).encode("utf-8")

        msg = SimulatedKafkaMessage(value=payload, topic="offline-demo", partition=0, offset=i)

        clean_count, bad_count = consume_one_message_and_write(
            mconn,
            msg,
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

    print("\n[Offline Demo] Done.")
    print(f"Processed: {total} | Clean: {clean_count} | Bad: {bad_count}")

    try:
        mconn.conn.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
