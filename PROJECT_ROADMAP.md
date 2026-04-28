# Project Roadmap

This roadmap captures practical milestones to evolve the pipeline from a local demo into a stronger portfolio-grade data engineering project.

---

## Milestone 1 - Quality and Test Foundation

Target: make core logic safer to modify and easier to trust.

- Expand `pytest` suite for `consumer/validator.py` (initial tests exist under `tests/`).
- Add test cases for edge conditions (nulls, bad timestamps, bad amount types).
- Add lightweight tests for `consumer/db_writer.py` SQL execution behavior.
- Add deterministic test data fixtures.

Success criteria:
- Rule coverage is visible and repeatable.
- New validation rules can be added with confidence.

---

## Milestone 2 - Runtime Reliability

Target: reduce fragility in long-running consumer flows.

- Add retry/backoff strategy for transient PostgreSQL errors (initial version implemented in code).
- Add defensive handling for JSON decode and schema-mismatch issues.
- Add dead-letter handling path for records that cannot be processed.
- Improve startup checks for Kafka/PostgreSQL readiness.

Success criteria:
- Consumer recovers from common transient failures without restart.
- Critical failures are captured with enough context for triage.

---

## Milestone 3 - Observability and Alerting

Target: improve visibility into quality trends and failure patterns.

- Add dashboard layer (Grafana or Metabase) connected to metrics tables.
- Add top failure reason trend chart.
- Add bad-rate alert forwarding (Slack/email/webhook).
- Add operational runbook for common incident scenarios.

Success criteria:
- Quality regressions become visible without manual SQL checks.
- Alert paths support faster response during bad-rate spikes.

---

## Milestone 4 - Production-Style Practices

Target: strengthen software engineering and deployment posture.

- Add CI workflow for lint/test gates.
- Add `.env.example` and config validation on startup.
- Add structured logging fields for easier filtering.
- Add deployment notes for cloud-managed Kafka/Postgres variants.

Success criteria:
- Project demonstrates both DE fundamentals and production-minded practices.

---

## Backlog Ideas

- Schema registry integration for event contract evolution.
- Partitioning strategy discussion for higher throughput topics.
- Batch sink option for analytics warehouse targets.
- Data quality scorecard report generation per day/week.
