"""
validator.py
------------
Contains all data quality validation rules.

Each rule is a separate function that returns (is_valid, reason).
This makes it easy to add, remove, or modify rules independently —
a pattern commonly used in production data quality frameworks.
"""

from config.settings import VALID_EVENT_TYPES, VALID_CURRENCIES


REQUIRED_FIELDS = ["event_id", "user_id", "product_id", "event_type", "amount", "currency", "timestamp"]


def check_event_is_object(event):
    """
    Guardrail: kafka-json payloads should decode to a JSON object (dict).
    If not, downstream field rules are not meaningful.
    """
    if isinstance(event, dict):
        return True, None
    return False, f"Event must be a JSON object/dict; got {type(event).__name__}"


def check_required_fields(event: dict):
    """Rule 1: All required fields must be present."""
    missing = [f for f in REQUIRED_FIELDS if f not in event]
    if missing:
        return False, f"Missing required fields: {missing}"
    return True, None


def check_null_values(event: dict):
    """Rule 2: No required field can be None or empty string."""
    for field in REQUIRED_FIELDS:
        val = event.get(field)
        if val is None or str(val).strip() == "":
            return False, f"Null or empty value in field: '{field}'"
    return True, None


def check_amount(event: dict):
    """Rule 3: Amount must be a positive number."""
    try:
        amount = float(event.get("amount", 0))
        if amount <= 0:
            return False, f"Invalid amount: {amount} (must be > 0)"
    except (TypeError, ValueError):
        return False, f"Amount is not a valid number: {event.get('amount')}"
    return True, None


def check_event_type(event: dict):
    """Rule 4: event_type must be one of the known valid types."""
    event_type = event.get("event_type")
    if event_type not in VALID_EVENT_TYPES:
        return False, f"Unknown event_type: '{event_type}' (valid: {VALID_EVENT_TYPES})"
    return True, None


def check_currency(event: dict):
    """Rule 5: currency must be one of the supported currencies."""
    currency = event.get("currency")
    if currency not in VALID_CURRENCIES:
        return False, f"Unsupported currency: '{currency}' (valid: {VALID_CURRENCIES})"
    return True, None


def check_timestamp(event: dict):
    """Rule 6: timestamp must be a parseable ISO format string."""
    from datetime import datetime
    ts = event.get("timestamp")
    try:
        datetime.fromisoformat(str(ts))
    except (TypeError, ValueError):
        return False, f"Invalid timestamp format: '{ts}'"
    return True, None


# All rules in execution order
ALL_RULES = [
    check_event_is_object,
    check_required_fields,
    check_null_values,
    check_amount,
    check_event_type,
    check_currency,
    check_timestamp,
]


def validate_event(event: dict):
    """
    Run all validation rules against an event.

    Returns:
        (True, None)           if all rules pass
        (False, reason_str)    if any rule fails (stops at first failure)
    """
    for rule in ALL_RULES:
        is_valid, reason = rule(event)
        if not is_valid:
            return False, reason
    return True, None


def validate_event_collect_errors(event: dict):
    """
    Run all validation rules and collect all failures (no fail-fast).

    Returns:
        (True, [])                  if all rules pass
        (False, [reason1, ...])     if one or more rules fail
    """
    errors = []
    for rule in ALL_RULES:
        is_valid, reason = rule(event)
        if not is_valid and reason:
            errors.append(reason)
            # If the payload isn't a JSON object/dict, other field rules aren't meaningful (and may crash).
            if rule is check_event_is_object:
                break
    return len(errors) == 0, errors
