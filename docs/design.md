# Real-Time Programme Recommendation Pipeline

## 1. Goal and scope

Build a small-scale, event-driven recommendation pipeline that identifies trending programmes from synthetic viewing events. The system will ingest events continuously, aggregate viewing activity over rolling time windows, and publish ranked recommendations for downstream consumers.

The primary objective is to demonstrate reliable streaming-data engineering, including event-time processing, duplicate handling, fault recovery, automated testing, and measurable performance.

**In scope:** synthetic event generation, Kafka ingestion, Flink stream processing, sliding-window aggregation, DynamoDB persistence, a Lambda-based read API, automated tests, and performance measurements.

**Out of scope for the initial version:** personalised recommendations, production-scale infrastructure, real user data, and continue-watching recommendations. Continue-watching may be considered as a later extension.

## 2. Architecture

The project uses a hybrid local/cloud architecture.

- **Python:** synthetic event generation, test utilities, and performance measurement.
- **Kafka:** event ingestion and buffering.
- **Apache Flink:** stream processing, event-time windows, aggregation, and ranking.
- **AWS DynamoDB:** persistence of the latest recommendation results.
- **AWS Lambda:** API for retrieving the current trending programmes.
- **pytest:** unit, integration, and failure-handling tests.
- **Docker:** reproducible local development environment for Kafka and Flink.

The intended data flow is:

Synthetic event generator → Kafka → Flink → DynamoDB → Lambda read API → Client.

Kafka and Flink run locally during development. DynamoDB and Lambda run in AWS. Tests should support local execution wherever practical, with cloud integration tests separated from the default test suite.

## 3. Event schema

Each viewing event contains:

| Field | Type | Description |
|---|---|---|
| `event_id` | string | Globally unique synthetic event identifier |
| `user_id` | string | Synthetic viewer identifier |
| `programme_id` | string | Identifier from the invented programme catalogue |
| `event_type` | string | For example, `play`, `progress`, or `complete` |
| `event_time` | timestamp | UTC time when the viewing event occurred |
| `duration_seconds` | integer | Viewing duration associated with the event, where applicable |

All events use invented catalogue entries and generated user identifiers. The initial trending calculation counts eligible viewing events; it does not attempt to infer individual preferences.

## 4. Windowing and ranking

Flink calculates programme popularity using **one-hour sliding event-time windows, evaluated every five minutes**.

For each window, the pipeline counts eligible, unique viewing events per programme and selects the top 20 programmes. Results include the programme identifier, event count, window start and end timestamps, and calculation timestamp.

Ranking is based on descending event count, with `programme_id` as a deterministic tie-breaker. The initial implementation uses synthetic events and a clearly documented eligibility rule.

Events arriving late are handled according to a configurable event-time watermark and allowed-lateness policy. Events arriving beyond the accepted lateness threshold are recorded or measured rather than silently changing historical results.

## 5. Reliability and serving

Each event is deduplicated using `event_id` within a documented retention period. The pipeline must tolerate repeated delivery without inflating popularity counts. The design will document how deduplication state, checkpoints, and recovery interact.

Flink checkpoints and Kafka replay provide the intended recovery mechanism. DynamoDB writes should be idempotent for a given result-window identifier so that retries do not create duplicate logical results. The design does not assume end-to-end exactly-once delivery; the implementation must establish and test its actual guarantees.

Lambda exposes a read endpoint that returns the latest available trending results. The API returns a clear response when results are unavailable and does not perform the streaming aggregation itself.

## 6. Performance and acceptance criteria

The initial performance target is **p95 event-to-result latency of 30 seconds or less under a documented local test workload**, measured from event ingestion to the corresponding updated result becoming available. This is a provisional engineering target, not a production service-level agreement.

The implementation will measure:
- Sustained event throughput in events per second.
- p50, p95, and p99 processing latency.
- Duplicate-event handling and count correctness.
- Recovery behaviour following a simulated processing failure.
- Result freshness and successful DynamoDB writes.

Correctness takes priority over throughput. The project is successful when automated tests validate aggregation, window boundaries, ranking, duplicate handling, and recovery, and a repeatable benchmark documents the achieved performance.
