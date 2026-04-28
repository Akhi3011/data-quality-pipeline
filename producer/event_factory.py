import random
import uuid
from datetime import datetime

from config.settings import VALID_EVENT_TYPES, VALID_CURRENCIES


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
