import json
from collections import Counter
from dataclasses import dataclass
from typing import Any, Optional

from consumer.db_writer import (
    MutableConnection,
    write_bad_event,
    write_clean_event,
    write_failure_metrics,
    write_quality_alert,
    write_quality_metrics,
)
from consumer.validator import validate_event_collect_errors
from config.settings import (
    BAD_RATE_ALERT_THRESHOLD_PCT,
    METRICS_INTERVAL,
    TOP_FAILURE_REASONS_LIMIT,
)


@dataclass(frozen=True)
class SimulatedKafkaMessage:
    """
    Lightweight stand-in for kafka-python ConsumerRecord attributes used by the consumer loop.
    """

    value: bytes

    topic: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None


def consume_one_message_and_write(
    mconn: MutableConnection,
    message,
    *,
    clean_count: int,
    bad_count: int,
    interval_failure_counts: Counter[str],
):
    """
    Process a single kafka message-ish object and persist results.

    Returns updated counters tuple:
        (clean_count, bad_count)
    """

    raw_bytes = getattr(message, "value", None)
    if raw_bytes is None:
        raw_bytes = b""
    if isinstance(raw_bytes, memoryview):
        raw_bytes = raw_bytes.tobytes()

    try:
        raw_text = raw_bytes.decode("utf-8", errors="replace")
    except Exception:
        raw_text = "<unprintable payload>"

    event: Optional[Any] = None
    try:
        event = json.loads(raw_text)
    except json.JSONDecodeError as e:
        quarantine_event = {
            "event_id": "PARSE_ERROR",
            "topic": getattr(message, "topic", None),
            "partition": getattr(message, "partition", None),
            "offset": getattr(message, "offset", None),
            "raw": raw_text,
        }
        reason = f"Kafka payload is not valid JSON: {e}"
        write_bad_event(mconn, quarantine_event, reason)
        bad_count += 1
        interval_failure_counts.update([reason])
        print(f"[BAD ] event_id=N/A | REJECTED: {reason}")

    if event is not None:
        is_valid, errors = validate_event_collect_errors(event)

        if is_valid:
            write_clean_event(mconn, event)
            clean_count += 1
            print(
                f"[CLEAN] event_id={event['event_id']} | "
                f"type={event['event_type']} | amount={event['amount']} {event['currency']}"
            )
        else:
            reason = " | ".join(errors)
            write_bad_event(mconn, event, reason)
            bad_count += 1
            interval_failure_counts.update(errors)
            print(f"[BAD ] event_id={event.get('event_id', 'N/A')} | REJECTED: {reason}")

    return clean_count, bad_count


def maybe_emit_interval_report(
    mconn: MutableConnection,
    *,
    total: int,
    clean_count: int,
    bad_count: int,
    interval_failure_counts: Counter[str],
):
    if total % METRICS_INTERVAL != 0:
        return

    write_quality_metrics(mconn, total, clean_count, bad_count)
    write_failure_metrics(mconn, total, clean_count, bad_count, dict(interval_failure_counts))

    bad_rate = round((bad_count / total) * 100, 1) if total > 0 else 0.0

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
            mconn,
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
    print("--------------------------------------------\n")
    interval_failure_counts.clear()


