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
import uuid
from datetime import datetime

from kafka import KafkaProducer
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_RATE_PER_SECOND,
    BAD_DATA_RATIO,
    VALID_EVENT_TYPES,
    VALID_CURRENCIES,
)


def generate_good_event():
    """Generate a valid, well-formed e-commerce event."""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1000, 9999)}",
        "product_id": f"prod_{random.randint(100, 999)}",
        "event_type": random.choice(VALID_EVENT_TYPES),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "currency": random.choice(VALID_CURRENCIES),
        "timestamp": datetime.utcnow().isoformat(),
    }


def generate_bad_event():
    """
    Generate a malformed event to test validation.
    Randomly picks one of several failure modes.
    """
    bad_type = random.choice([
        "missing_field",
        "negative_amount",
        "invalid_event_type",
        "null_user",
        "invalid_currency",
    ])

    event = generate_good_event()

    if bad_type == "missing_field":
        # Remove a required field
        field_to_remove = random.choice(["product_id", "amount", "timestamp"])
        event.pop(field_to_remove, None)

    elif bad_type == "negative_amount":
        event["amount"] = round(random.uniform(-500.0, -0.1), 2)

    elif bad_type == "invalid_event_type":
        event["event_type"] = random.choice(["click", "hover", "scroll", "unknown"])

    elif bad_type == "null_user":
        event["user_id"] = None

    elif bad_type == "invalid_currency":
        event["currency"] = random.choice(["XYZ", "AAA", "123"])

    event["_injected_error"] = bad_type  # helpful for debugging
    return event


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
