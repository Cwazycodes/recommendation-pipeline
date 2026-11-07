# Metrics

What the pipeline measures, how each number is defined, and how to read them for a live run. The definitions come first so that every benchmark measures the same thing.

## Definitions

### Throughput

Events per second the Flink job reads from the events topic: how much the Kafka source's `numRecordsOut` counter rises over a run, divided by the run's length. It counts every record the source reads, malformed events and duplicates included, because each costs the job the same work. The report shows the produced rate beside it: how fast the topic's end offsets rose.

**Sustained throughput** is the highest producer rate at which consumer lag does not grow over a run. A run's throughput counts as sustained only if its lag trend is steady. When the producer outpaces the job, the job still reads as fast as it can, so throughput shows how fast that is, and it falls below the produced rate while the lag grows.

### Consumer lag

Records on the events topic that the job has not read yet, as Kafka measures it: for each partition, the end offset minus the offset the job's consumer group (`FLINK_SOURCE_GROUP_ID`) has committed, summed over the topic's partitions. kafka-exporter reads both from Kafka for Prometheus.

The job commits its offsets when a checkpoint completes, every 10 s, so between checkpoints the group's lag climbs by the events that arrived and then drops back. **Lag at a moment is the lowest value in the 15 s before it**, which is the lag just after the last checkpoint.

Flink's own `pendingRecords` metric is not used. It comes from the Kafka client's lag figure, which is only updated when the client fetches, and when the job falls behind, the fetcher waits on a buffer it has already filled. In a run at 400 events/s, Kafka showed 16,493 records behind while `pendingRecords` never went above 989.

**Lag trend** is the slope of lag over the run, from a least-squares fit (Prometheus `deriv`).
- **Growing:** lag rises by more than 1 record for every 100 the job reads (slope above 1% of throughput).
- **Shrinking:** lag falls as fast.
- **Steady:** anything in between. Lag is never perfectly flat, because events arrive between checkpoints.

### Freshness

For each window the writer stores: **`written_at − window_end`**.

- `window_end` is the end of the window in event time, the item's sort key.
- `written_at` is the writer's clock, to the millisecond, when the `put_item` that stored the item succeeded. The writer adds it to the item. Retries and DynamoDB outages make it later, so freshness includes them.

Freshness is how out of date the API's newest list is when it appears. It only means something in a live run, where each event is stamped with the time it is sent (`--live`, as `make demo` does). Without that, event times start on a fixed date in the past, and every window ended long before it is written.

### Watermark delay and processing latency

A window fires when the watermark passes its end, and the watermark is the newest event time minus 30 seconds (`MAX_OUT_OF_ORDERNESS_S`; see design.md, "Event time and lateness"). In a live run the newest event time is about now, so no window can fire before `window_end + 30 s`. **Freshness therefore always includes this 30-second watermark delay.** It is the price of accepting events up to 30 s out of order, set by the design rather than by how fast the pipeline is.

**Processing latency** is whatever is above that: **`freshness − 30 s`**. It is made up of:

- the watermark reaching the window operator: every partition needs an event later than `window_end + 30 s`, and Flink emits watermarks every 200 ms;
- Flink computing the window and emitting its ranked list (`generated_at`);
- the checkpoint that commits the Kafka transaction holding the list, up to 10 s after the rows are emitted, before the writer can read them;
- the writer completing the window, when the next window's first row arrives or after 2 s without rows;
- the DynamoDB write.

Flink's `watermarkLag` (now minus the source's watermark) shows the watermark delay as it is. While the job keeps up it stays just above 30 s; when the job falls behind it grows, and so does freshness.

### Delivery latency

**`written_at − generated_at`**: from Flink emitting a window (the latest `emitted_at` of its rows) to its item being stored. It is the part of processing latency after Flink: the checkpoint commit, the writer and DynamoDB.

### Percentiles

p50, p95 and p99 are taken over the windows written in a stretch of time, one sample per window. They come from the writer's Prometheus histograms (`histogram_quantile`), which estimate each percentile to within one bucket: 0.5 s from 30 to 45 s of freshness, and from 0 to 12 s of delivery. A 2-minute demo writes about 10 windows, so its p99 is close to its slowest window. Exact values for each window are in the item's `written_at` and in the writer's `wrote window` log lines (`freshness_s`).

The target in design.md section 6, p95 of 30 seconds or less, is checked against processing latency. Freshness cannot be below the 30-second watermark delay, so it could never meet that target.

## Where the numbers come from

| Number | Prometheus metric | From |
|---|---|---|
| Throughput | `flink_taskmanager_job_task_operator_numRecordsOut{operator_name="Source:_viewing_events_source"}` | Flink's Prometheus reporter, port 9249 in each Flink container |
| Produced rate | `kafka_topic_partition_current_offset{topic=...}` | kafka-exporter (Compose), from Kafka |
| Consumer lag | `kafka_consumergroup_lag{consumergroup="flink-validate", topic=...}` | kafka-exporter |
| Watermark delay | `flink_taskmanager_job_task_operator_watermarkLag{...}` (ms), same source | Flink |
| Freshness | `trending_window_freshness_seconds` (histogram) | the results writer, `METRICS_WRITER_PORT` (9464) |
| Delivery latency | `trending_window_delivery_seconds` (histogram) | the results writer |
| Windows written, dead-lettered | `trending_windows_written_total`, `trending_windows_dead_lettered_total` | the results writer |

Flink's reporter replaces the hyphens in operator names with underscores, so the source `viewing-events-source` is `Source:_viewing_events_source`.

Prometheus runs in Compose (`make up`, http://localhost:9090) and scrapes Flink, kafka-exporter and the writer every 5 seconds (`infra/prometheus/prometheus.yml`). The writer runs on the host, so Prometheus reaches it as `host.docker.internal:9464`. `make writer`, `make demo` and the chaos drills' writer all serve on that port, so run one of them at a time. Grafana is left out: Prometheus's own graph page draws these queries, and the stack already needs most of a 4 GB Docker VM.

## Reading them for a live run

- **`make demo`** prints its run's metrics at the end: throughput and lag while it produced, and latency for the windows it wrote.
- **`make metrics`** prints the same report for the last 5 minutes (`MINUTES=2` for 2), during or after any live run, for the job reading `KAFKA_EVENTS_TOPIC`. The demo's job reads its own topic, so during a demo use `make metrics TOPIC=viewing-events-demo`. For a longer run, start `make demo DEMO_DURATION=600` and call that from another terminal.

From `make demo` (50 events/s for 2 minutes on the local stack):

```text
Metrics from 15:30:17 to 15:32:34 UTC (see docs/metrics.md)
  Throughput       49.7 events/s read by the job (49.6 produced)
  Consumer lag     93 records at the end, at most 355, trend 0.0 records/s (steady)
  Watermark delay  30.3 s at the end (bound 30 s)
  Windows written  9
  Freshness        p50  36.2 s  p95  36.5 s  p99  36.5 s
  Processing       p50   6.2 s  p95   6.5 s  p99   6.5 s  (freshness - 30 s)
  Delivery         p50   2.2 s  p95   2.5 s  p99   2.5 s
```

The same demo at 300 events/s fell behind. Throughput was 195.7 events/s read against 292.5 produced, lag reached 8,006 records and was growing at 138 records/s, and the watermark delay was 47 s. No window had been written yet, so the latency rows showed `n/a`.

In Prometheus's graph page:

```promql
# Events read per second
sum(rate(flink_taskmanager_job_task_operator_numRecordsOut{operator_name="Source:_viewing_events_source"}[1m]))
# Consumer lag in records, for the demo's topic
sum(kafka_consumergroup_lag{consumergroup="flink-validate", topic="viewing-events-demo"})
# Freshness p95 over the last 5 minutes
histogram_quantile(0.95, sum by (le) (rate(trending_window_freshness_seconds_bucket[5m])))
```
