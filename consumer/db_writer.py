"""
db_writer.py
------------
Handles all PostgreSQL writes.

Separates clean events, bad events, and quality metrics
into their own tables — mirroring a real data lakehouse pattern
where raw, clean, and quarantine zones are kept separate.
"""

import json
import psycopg2
from datetime import datetime
from config.settings import (
    POSTGRES_HOST, POSTGRES_PORT,
    POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
)


def get_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def write_clean_event(conn, event: dict):
    """Insert a validated clean event into the clean_events table."""
    sql = """
        INSERT INTO clean_events
            (event_id, user_id, product_id, event_type, amount, currency, timestamp)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            event["event_id"],
            event["user_id"],
            event["product_id"],
            event["event_type"],
            float(event["amount"]),
            event["currency"],
            datetime.fromisoformat(event["timestamp"]),
        ))
    conn.commit()


def write_bad_event(conn, event: dict, reason: str):
    """Insert a rejected event into the bad_events quarantine table."""
    sql = """
        INSERT INTO bad_events (event_id, raw_data, rejection_reason)
        VALUES (%s, %s, %s);
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            event.get("event_id", "UNKNOWN"),
            json.dumps(event),
            reason,
        ))
    conn.commit()


def write_quality_metrics(conn, total: int, clean: int, bad: int):
    """Record a quality summary row after each reporting interval."""
    bad_rate = round((bad / total) * 100, 2) if total > 0 else 0.0
    sql = """
        INSERT INTO quality_metrics (total_received, total_clean, total_bad, bad_rate_pct)
        VALUES (%s, %s, %s, %s);
    """
    with conn.cursor() as cur:
        cur.execute(sql, (total, clean, bad, bad_rate))
    conn.commit()


def write_failure_metrics(conn, total: int, clean: int, bad: int, failure_counts: dict):
    """Persist per-batch validation failure breakdown for observability."""
    sql = """
        INSERT INTO validation_failure_metrics
            (total_received, total_clean, total_bad, failure_reason, failure_count)
        VALUES
            (%s, %s, %s, %s, %s);
    """
    with conn.cursor() as cur:
        for reason, count in failure_counts.items():
            cur.execute(sql, (total, clean, bad, reason, count))
    conn.commit()


def write_quality_alert(conn, total: int, clean: int, bad: int, bad_rate_pct: float, threshold_pct: float):
    """Persist quality alerts when bad-rate crosses threshold."""
    sql = """
        INSERT INTO quality_alerts
            (total_received, total_clean, total_bad, bad_rate_pct, threshold_pct, message)
        VALUES
            (%s, %s, %s, %s, %s, %s);
    """
    message = (
        f"Bad-rate alert triggered: {bad_rate_pct}% exceeded threshold {threshold_pct}% "
        f"at total_received={total}."
    )
    with conn.cursor() as cur:
        cur.execute(sql, (total, clean, bad, bad_rate_pct, threshold_pct, message))
    conn.commit()
