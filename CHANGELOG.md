# Changelog

All notable changes to this project are documented in this file.

The format is inspired by Keep a Changelog and uses practical milestone notes rather than strict release tags.

---

## 2026-04-28 - Project Baseline Published

### Added
- Initial end-to-end skeleton for a real-time data quality pipeline.
- Kafka event producer with controlled bad-data injection.
- Kafka consumer with rule-based validation and routing.
- PostgreSQL writers for `clean_events`, `bad_events`, and quality metrics.
- SQL bootstrap script for table creation.
- Local orchestration with Docker Compose (`zookeeper`, `kafka`, `postgres`).

### Improved
- Validation execution mode switched to collect-all-errors for richer diagnostics.
- Failure-reason aggregation for interval-level observability.
- Quality alert persistence when bad-rate breaches threshold.
- README expanded with architecture, SQL checks, and operational notes.

### Notes
- Current setup is intentionally local-first and single-node for easy demonstration.
- Future milestones focus on reliability, testing, and better observability.

---

## 2026-04-28 - Reliability and Test Harness Improvements

### Added
- `pytest` test coverage for validator edge cases (`tests/`).
- Shared processing logic in `consumer/pipeline_logic.py` to reduce drift between demos and the Kafka consumer.
- `scripts/offline_consumer_demo.py` to exercise decoding + validation + DB writes without running Kafka producer/consumer binaries.
- `producer/event_factory.py` extracted from `producer.py` for reuse across producer and offline tooling.

### Improved
- Consumer now survives invalid JSON payloads by quarantining parse failures instead of crashing the deserialization layer.
- PostgreSQL commits and writes include retry/backoff for transient connection failures (`consumer/db_writer.py`).
- Validation engine short-circuits non-object payloads to avoid noisy cascading errors.

---

## Planned Next Iterations

### Iteration 1 - Reliability Hardening
- Add retry and backoff around DB writes.
- Define idempotency behavior for duplicate event IDs.
- Add safer error handling for malformed Kafka payloads.

### Iteration 2 - Test Coverage
- Add unit tests for each validation rule.
- Add integration test for consumer routing behavior.
- Add smoke test for startup and basic processing path.

### Iteration 3 - Monitoring and Ops
- Add dashboard layer for quality metrics and alerts.
- Add dead-letter topic for unparseable payloads.
- Add runbook-style troubleshooting section.
