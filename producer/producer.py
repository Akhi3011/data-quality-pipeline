"""
producer.py
-----------
Simulates an e-commerce event stream by generating fake events
and publishing them to a Kafka topic.

Intentionally injects ~20% bad/malformed events so the consumer
can demonstrate real validation and rejection logic.

Run:  python producer/producer.py
"""

import json
import random
import time

from kafka import KafkaProducer
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_RATE_PER_SECOND,
    BAD_DATA_RATIO,
)
from producer.event_factory import generate_bad_event, generate_good_event


def create_producer():
    """Create and return a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def run_producer():
    producer = create_producer()
    sleep_time = 1.0 / PRODUCER_RATE_PER_SECOND
    total_sent = 0

    print(f"[Producer] Starting. Sending {PRODUCER_RATE_PER_SECOND} events/sec to '{KAFKA_TOPIC}'")
    print(f"[Producer] ~{int(BAD_DATA_RATIO * 100)}% will be intentionally malformed\n")

    try:
        while True:
            is_bad = random.random() < BAD_DATA_RATIO
            event = generate_bad_event() if is_bad else generate_good_event()

            producer.send(KAFKA_TOPIC, value=event)
            total_sent += 1

            label = "BAD " if is_bad else "GOOD"
            print(f"[{label}] Sent event_id={event.get('event_id', 'N/A')} | "
                  f"type={event.get('event_type', 'MISSING')} | "
                  f"amount={event.get('amount', 'MISSING')}")

            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\n[Producer] Stopped. Total events sent: {total_sent}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run_producer()
