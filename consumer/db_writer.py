"""
db_writer.py
------------
Handles all PostgreSQL writes.

Separates clean events, bad events, and quality metrics
into their own tables — mirroring a real data lakehouse pattern
where raw, clean, and quarantine zones are kept separate.
"""

import json
import random
import time
import psycopg2
from datetime import datetime
from psycopg2 import OperationalError, InterfaceError
from config.settings import (
    POSTGRES_HOST, POSTGRES_PORT,
    POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
    DB_MAX_RETRIES, DB_RETRY_BASE_SLEEP_SECONDS, DB_RETRY_MAX_SLEEP_SECONDS,
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


def _sleep_with_backoff(attempt: int) -> None:
    base = DB_RETRY_BASE_SLEEP_SECONDS * (2 ** max(0, attempt - 1))
    sleep_s = min(DB_RETRY_MAX_SLEEP_SECONDS, base)
    # tiny jitter to reduce thundering herd if multiple workers restart together
    jitter = random.uniform(0, min(0.25, sleep_s))
    time.sleep(sleep_s + jitter)


class MutableConnection:
    """
    Small wrapper so database writers can swap to a fresh psycopg2 connection mid-flight.
    Consumer code can keep a single object reference while connection objects rotate.
    """

    __slots__ = ("conn",)

    def __init__(self, conn):
        self.conn = conn

    def replace(self, new_conn):
        self.conn = new_conn


def _commit_with_retry(mconn: MutableConnection, label: str) -> None:
    last_err = None
    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            mconn.conn.commit()
            return
        except (OperationalError, InterfaceError) as e:
            last_err = e
            try:
                mconn.conn.close()
            except Exception:
                pass
            mconn.replace(get_connection())
            _sleep_with_backoff(attempt)
    raise RuntimeError(f"PostgreSQL commit failed for {label} after {DB_MAX_RETRIES} attempts") from last_err


def write_clean_event(mconn: MutableConnection, event: dict):
    """Insert a validated clean event into the clean_events table."""
    sql = """
        INSERT INTO clean_events
            (event_id, user_id, product_id, event_type, amount, currency, timestamp)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
    """
    last_err = None
    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            with mconn.conn.cursor() as cur:
                cur.execute(sql, (
                    event["event_id"],
                    event["user_id"],
                    event["product_id"],
                    event["event_type"],
                    float(event["amount"]),
                    event["currency"],
                    datetime.fromisoformat(event["timestamp"]),
                ))
            _commit_with_retry(mconn, "write_clean_event")
            return
        except (OperationalError, InterfaceError) as e:
            last_err = e
            try:
                mconn.conn.close()
            except Exception:
                pass
            mconn.replace(get_connection())
            _sleep_with_backoff(attempt)
    raise RuntimeError("write_clean_event failed after retries") from last_err


def write_bad_event(mconn: MutableConnection, event: dict, reason: str):
    """Insert a rejected event into the bad_events quarantine table."""
    sql = """
        INSERT INTO bad_events (event_id, raw_data, rejection_reason)
        VALUES (%s, %s, %s);
    """
    last_err = None
    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            with mconn.conn.cursor() as cur:
                cur.execute(sql, (
                    event.get("event_id", "UNKNOWN"),
                    json.dumps(event),
                    reason,
                ))
            _commit_with_retry(mconn, "write_bad_event")
            return
        except (OperationalError, InterfaceError) as e:
            last_err = e
            try:
                mconn.conn.close()
            except Exception:
                pass
            mconn.replace(get_connection())
            _sleep_with_backoff(attempt)
    raise RuntimeError("write_bad_event failed after retries") from last_err


def write_quality_metrics(mconn: MutableConnection, total: int, clean: int, bad: int):
    """Record a quality summary row after each reporting interval."""
    bad_rate = round((bad / total) * 100, 2) if total > 0 else 0.0
    sql = """
        INSERT INTO quality_metrics (total_received, total_clean, total_bad, bad_rate_pct)
        VALUES (%s, %s, %s, %s);
    """
    last_err = None
    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            with mconn.conn.cursor() as cur:
                cur.execute(sql, (total, clean, bad, bad_rate))
            _commit_with_retry(mconn, "write_quality_metrics")
            return
        except (OperationalError, InterfaceError) as e:
            last_err = e
            try:
                mconn.conn.close()
            except Exception:
                pass
            mconn.replace(get_connection())
            _sleep_with_backoff(attempt)
    raise RuntimeError("write_quality_metrics failed after retries") from last_err


def write_failure_metrics(mconn: MutableConnection, total: int, clean: int, bad: int, failure_counts: dict):
    """Persist per-batch validation failure breakdown for observability."""
    sql = """
        INSERT INTO validation_failure_metrics
            (total_received, total_clean, total_bad, failure_reason, failure_count)
        VALUES
            (%s, %s, %s, %s, %s);
    """
    last_err = None
    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            with mconn.conn.cursor() as cur:
                for reason, count in failure_counts.items():
                    cur.execute(sql, (total, clean, bad, reason, count))
            _commit_with_retry(mconn, "write_failure_metrics")
            return
        except (OperationalError, InterfaceError) as e:
            last_err = e
            try:
                mconn.conn.close()
            except Exception:
                pass
            mconn.replace(get_connection())
            _sleep_with_backoff(attempt)
    raise RuntimeError("write_failure_metrics failed after retries") from last_err


def write_quality_alert(mconn: MutableConnection, total: int, clean: int, bad: int, bad_rate_pct: float, threshold_pct: float):
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
    last_err = None
    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            with mconn.conn.cursor() as cur:
                cur.execute(sql, (total, clean, bad, bad_rate_pct, threshold_pct, message))
            _commit_with_retry(mconn, "write_quality_alert")
            return
        except (OperationalError, InterfaceError) as e:
            last_err = e
            try:
                mconn.conn.close()
            except Exception:
                pass
            mconn.replace(get_connection())
            _sleep_with_backoff(attempt)
    raise RuntimeError("write_quality_alert failed after retries") from last_err
