from consumer.validator import validate_event_collect_errors


def _good_event(**overrides):
    base = {
        "event_id": "evt-1",
        "user_id": "user_1",
        "product_id": "prod_1",
        "event_type": "purchase",
        "amount": 12.34,
        "currency": "USD",
        "timestamp": "2026-04-28T12:34:56",
    }
    base.update(overrides)
    return base


def test_collect_all_errors_reports_multiple_issues():
    event = _good_event(amount=-1, currency="XYZ", timestamp="bad-ts")
    is_valid, errors = validate_event_collect_errors(event)

    assert is_valid is False
    assert any("amount" in e.lower() or "invalid amount" in e.lower() for e in errors)
    assert any("currency" in e.lower() for e in errors)
    assert any("timestamp" in e.lower() for e in errors)


def test_requires_json_object_guardrail():
    is_valid, errors = validate_event_collect_errors([])
    assert is_valid is False
    assert any("json object/dict" in e.lower() for e in errors)
