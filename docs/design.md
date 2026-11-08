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

For each window, the pipeline scores each programme by its number of distinct viewers and selects the top 20 programmes. Results include the programme identifier, score, window start and end timestamps, and calculation timestamp.

Ranking is based on descending score, with `programme_id` as a deterministic tie-breaker. The initial implementation uses synthetic events and a clearly documented eligibility rule.

Events arriving late are handled according to a configurable event-time watermark and allowed-lateness policy. Events arriving beyond the accepted lateness threshold are recorded or measured rather than silently changing historical results.


### Event time and lateness

Windows use `event_time`, the moment the viewer's action happened, not the time a record reached Kafka or Flink.

- **Where event time comes from:** the producer copies `event_time` into the Kafka record timestamp, and the Flink Kafka source uses that timestamp for each record. After validation and deduplication the stream becomes a Table whose `event_time` column (`TIMESTAMP_LTZ(3)`) is the rowtime attribute, carrying the same watermarks.
- **Why the Kafka timestamp:** PyFlink ignores a Python `TimestampAssigner` passed to `from_source`, so the record timestamp is the only way to get watermarks generated per partition. Per-partition watermarks matter here: the generator compresses roughly 8 hours of event time into a 60-second run, so merging partitions before assigning watermarks would turn millisecond-level interleaving into minutes of apparent disorder.
- **Watermark:** for each partition, the newest `event_time` seen minus 30 seconds. The source's watermark is the minimum across active partitions.
- **Idleness:** a partition that delivers nothing for 60 seconds is marked idle and stops holding the watermark back. When it delivers again it rejoins.
- **What counts as late:** an event whose `event_time` is at or before the current watermark, meaning more than 30 seconds behind the newest event in its partition, or behind the others after its partition was idle.
- **What happens beyond the bound:** a late event still passes validation and deduplication. In a window aggregation it is counted if its window has not fired yet; if the window has already fired, the window operator drops it and counts it in its `numLateRecordsDropped` metric. Published results are never revised (no allowed lateness), which keeps historical results stable.
- **Malformed events:** when `event_time` is not an integer the producer stamps the record with `produced_at`, so a bad event cannot push a partition's watermark ahead. It is dead-lettered as before.
- **Known risk:** a well-formed but far-future `event_time` would advance its partition's watermark and make the events that follow late. Validation does not check plausibility yet.
- **Retention:** the event topics use `retention.ms=-1` locally, because Kafka's time-based retention reads record timestamps and synthetic event times can be weeks old.

### Trending results

- **Eligibility and score:** only `play_start` and `heartbeat` events count, since they mean someone is watching. A programme's score is `COUNT(DISTINCT user_id)` in the window, so one viewer sending many heartbeats still counts once.
- **Windows:** Flink SQL `HOP` windows, 60 minutes long, sliding every 5 minutes. Both are job arguments in seconds (`--window-size-seconds`, `--window-slide-seconds`, and `--top-n`); the size must be a whole multiple of the slide. The SQL writes them as `INTERVAL 'H:MM:SS' HOUR TO SECOND`, because a plain `SECOND` interval only allows two digits. The job's topics are arguments too (`--source-topic`, `--trending-topic`, `--dlq-topic`), which is how the demo runs it on its own topics.
- **Ranking:** Window Top-N with `ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY score DESC, programme_id)`, keeping ranks 1 to 20. Each window emits its list once, when the watermark passes `window_end`.
- **Output:** one JSON message per ranked programme on `trending-results`:

  | Field | Meaning |
  |---|---|
  | `window_start`, `window_end` | Window bounds, ISO 8601 UTC (`2026-09-21T12:00:00Z`) |
  | `rank` | 1 = most distinct viewers |
  | `programme_id` | Ranked programme |
  | `score` | Distinct viewers in the window |
  | `max_produced_at` | Latest `produced_at` among the programme's events in the window (epoch ms) |
  | `emitted_at` | When Flink emitted the row (epoch ms) |

- **Quiet windows:** a window with no eligible events emits nothing, and the last windows of a run wait until the watermark passes their end.

## 5. Reliability and serving

Each event is deduplicated using `event_id` within a documented retention period. The pipeline must tolerate repeated delivery without inflating popularity counts. The design will document how deduplication state, checkpoints, and recovery interact.

Flink checkpoints and Kafka replay provide the intended recovery mechanism. DynamoDB writes should be idempotent for a given result-window identifier so that retries do not create duplicate logical results. The design does not assume end-to-end exactly-once delivery; the implementation must establish and test its actual guarantees.

Lambda exposes a read endpoint that returns the latest available trending results. The API returns a clear response when results are unavailable and does not perform the streaming aggregation itself.

### Serving table (DynamoDB)

The read path answers one question, "what is trending now?", so the table is designed around that query.

- **Table `trending`:** partition key `pk`, sort key `sk`, both strings, on-demand billing. Each window is one item: `pk = "TRENDING#1h"` (one partition for the 1-hour lists), `sk = window_end` as ISO 8601 UTC (`2026-09-21T18:10:00Z`), plus `items` (the ranked list of `rank`, `programme_id`, `score`), `generated_at`, `ttl` and `written_at` (when the writer stored it; see metrics.md).
- **Latest list:** one `Query` on `pk = "TRENDING#1h"` with `ScanIndexForward=false` and `Limit=1`. ISO 8601 UTC strings sort in time order, so reading the partition backwards returns the newest window first.
- **Idempotent writes:** the window is the key, so writing the same window again replaces it instead of adding a copy.
- **Expiry:** `ttl` is `generated_at` plus 24 hours, in epoch seconds. It counts from when the list was written, not from `window_end`, because replayed or synthetic windows can already be days old. AWS deletes expired items lazily (typically within two days), so the latest-list query also filters on `#ttl > :now` and returns nothing rather than a stale list.
- **Reserved words:** `ttl` and `items` are DynamoDB reserved words, so expressions refer to them through placeholders such as `#ttl`.
- **One definition:** `infra/dynamodb/trending-table.json` is the only description of the table. `make db-init` creates it in DynamoDB Local, and `infra/terraform` reads the same file, so the two cannot drift.
- **Locally:** DynamoDB Local runs in Compose on port 8001 with `-sharedDb`, so every client sees one database whatever credentials or region it uses. Unlike AWS it deletes expired items within seconds.

### Checkpoints, restarts and exactly-once output

The job sets all of this itself (`flink_jobs/reliability.py`), so it travels with the job instead of depending on cluster settings.

- **Checkpoints:** every 10 seconds, exactly-once, with a 2-minute timeout, and kept when the job is cancelled. State (Kafka offsets, deduplication flags, window aggregates and open sink transactions) lives in the RocksDB state backend with incremental checkpoints, written to the `flink-checkpoints` volume at `/opt/flink/checkpoints`.
- **Exactly-once results:** the `trending-results` sink writes each checkpoint's rows in one Kafka transaction (transactional ID prefix `recommendation-pipeline-trending-results`) and commits it when the checkpoint completes. After a failure the job resumes from the last checkpoint, the open transaction is aborted, and its rows are written again, so a `read_committed` consumer sees each row exactly once. The cost is latency: rows become visible up to one checkpoint interval (10 s) after they are computed.
- **Transaction timeout:** 10 minutes, under the broker's `transaction.max.timeout.ms` of 15 minutes; the Kafka sink's default of 1 hour makes the job fail at start-up. A transaction is open for one interval plus at most the 2-minute checkpoint timeout, which leaves about 8 minutes for a restart to finish it before Kafka aborts it and those rows are lost.
- **Consumers:** downstream consumers read with `isolation.level=read_committed` (`common/kafka.py`). A `read_uncommitted` reader also sees rows from aborted transactions, so after a failure some windows can appear twice.
- **Dead letters:** the DLQ sink stays at-least-once. After a failure a few bad events can be dead-lettered twice, which is acceptable for a diagnostic topic and keeps it readable without transactions.
- **Parallelism:** the job's `--parallelism` argument (default 1) sets how many subtasks run the stages that handle each event: the Kafka source, validation and deduplication. The window aggregation, ranking and the `trending-results` sink always run as one subtask, so each window's rows come out of one sink in one transaction, as the results writer expects. [benchmarks.md](benchmarks.md) measures what parallelism 2 gains.
- **Restarts:** exponential backoff starting at 1 second, doubling up to 1 minute with 10% jitter, reset after 10 minutes without a failure.
- **Planned stops:** `make stop-job` stops the job with a savepoint under `/opt/flink/checkpoints/savepoints`, and `make submit-job SAVEPOINT=<path>` resumes from it. Stateful operators have fixed `uid`s so their state maps back on restore; the trending SQL must stay the same between stop and resume.
- **Known issue (PyFlink 1.20 with RocksDB):** cancelling the job, by `flink cancel` or as part of a failover, can crash the TaskManager JVM in RocksDB's native write path, because the Python worker's last state writes race with RocksDB shutting down. Stop-with-savepoint does not hit it. The TaskManager runs with `restart: unless-stopped`, so after a crash Docker restarts it and the restart strategy resumes the job from its last checkpoint. Use `make stop-job` rather than `flink cancel`. If the crashes matter more than RocksDB, `"state.backend.type": "hashmap"` avoids them; the state here is small.

### Results writer (Kafka to DynamoDB)

`writer/results_writer.py` (`make writer`) copies each window from `trending-results` into the `trending` table. Kafka already holds each committed row once; the writer has to keep it that way when it crashes and restarts.

- **Reading:** consumer group `results-writer` with `isolation.level=read_committed`, so rows from aborted Flink transactions are never seen, and auto-commit off. A new group starts from the earliest offset.
- **Restarting quickly:** the writer is a static group member (`group.instance.id=results-writer-1`). A writer that is killed never leaves its group, so without a fixed id the restarted one would wait for the dead one's 45-second session to time out before getting the partition. With it, the restarted writer takes the dead one's place at once. A second writer started with the same id fences off the first, so only one runs.
- **One item per window:** rows are buffered until their window is complete. Each window's rows come out of Flink in one transaction, and windows arrive in order on the topic's single partition, so a row from a different window, or 2 seconds without new rows, means the buffered window is complete. It is then written with one `put_item` keyed by `window_end`.
- **Idempotent:** the item depends only on the rows. `generated_at` is the window's latest `emitted_at`, when Flink emitted the list, not the writer's clock, so writing a replayed window again writes identical data, apart from `written_at`, the writer's own clock at the write, which freshness is measured from. `make check-writer` ignores `written_at`. If a rank appears twice the later row wins, so an item never repeats a rank.
- **Commit after write:** offsets are committed synchronously, only after the window's item is written, and only up to the first row of the window still being read. A crash therefore replays the partly read window, plus the window just written if the crash came between its write and its commit; that one is rewritten unchanged. This is at-least-once delivery made effectively once by idempotent writes. Stopping with Ctrl-C does not commit the partly read window either.
- **Retries:** throttling (`ThrottlingException`, `ProvisionedThroughputExceededException`, `RequestLimitExceeded`) and 5xx errors are retried with exponential backoff and full jitter, a random wait of up to 0.1 s × 2^(try − 1), capped at 5 s, for at most 5 tries. boto3's own retries are switched off for the writer so that count is exact.
- **Writer DLQ:** a window that still fails after 5 tries, or fails with an error retrying cannot fix (such as a validation error), goes to `trending-writer-dlq` with the item, the error and the number of tries. Its offsets are committed only once the DLQ has the record. If DynamoDB cannot be reached at all (refused or timed-out connections), the writer does not dead-letter anything: it waits with backoff from 1 s up to 30 s, commits nothing meanwhile, and carries on when DynamoDB is back. Its `max.poll.interval.ms` is 30 minutes, so it keeps its partition through an outage that long. The failure drills in [reliability.md](reliability.md) check this.
- **Checking it:** `make check-writer` rebuilds the items an uninterrupted writer would produce from `trending-results` and compares them with the table: no missing, extra or wrong windows, and no repeated ranks. Windows past their `ttl` are skipped, because DynamoDB deletes them.
- **Assumptions:** one partition and one writer. A window's rows are contiguous only within a partition, so with more partitions the buffer would have to track windows per partition. They also have to come from one sink subtask, which is why the job's ranking and sink stay at parallelism 1.

### Read API (Lambda handler)

`api/handler.py` is the read path. API Gateway (HTTP API, payload format 2.0) calls `handler(event, context)` for `GET /trending`, and it answers from the `trending` table with the one-item query above. It is a plain function, so it is built and tested before any AWS resource exists.

- **Request:** `GET /trending?limit=N`. `limit` is a whole number from 1 to 20 (the job ranks 20 programmes per window) and defaults to 10. Any other query parameter is rejected, so a typo such as `limt=5` is an error instead of being ignored. API Gateway joins a repeated parameter with commas, so `limit=5&limit=6` is rejected too. Methods other than GET get a 405.
- **Responses:** all JSON, with `content-type: application/json`.

  | Status | When | Body |
  |---|---|---|
  | 200 | A window exists and has not expired | `window_end`, `generated_at`, `limit`, and `items`: the first `limit` entries of `rank`, `programme_id`, `score` |
  | 400 | Invalid query | `{"error": "invalid_limit"}` or `{"error": "unknown_parameter"}`, with a `message` |
  | 404 | No window yet, or the newest one is past its `ttl` | `{"error": "no_trending_data", "message": ...}` |
  | 405 | Not a GET | `{"error": "method_not_allowed", "message": ...}`, with an `allow: GET` header |
  | 500 | DynamoDB failed: unreachable, throttled or missing table | `{"error": "internal_error", "message": ...}` |

- **Bugs are not hidden:** only DynamoDB errors (botocore's `ClientError` and `BotoCoreError`) become a 500 body. Anything else raises, so Lambda counts the invocation as failed and API Gateway returns its own 500.
- **Logs:** one JSON object per request on stdout, which Lambda sends to CloudWatch Logs: `level`, the API Gateway `request_id`, the Lambda `lambda_request_id`, `method`, `query`, `status`, `error`, `window_end`, `items` and `duration_ms`. A 500 is logged at `ERROR` with the exception's type and message.
- **Configuration:** `DYNAMODB_TABLE`, `DYNAMODB_PARTITION_KEY` and, locally, `DYNAMODB_ENDPOINT_URL` (see Configuration and logs). The DynamoDB resource is created once per Lambda container and reused across requests. Its client gives up quickly, with a 1 s connect timeout, a 2 s read timeout and 2 tries, instead of boto3's default 60 s read timeout, so a request fails within about 7 seconds and the Lambda timeout must be longer than that.
- **Dependencies:** boto3, which the Lambda Python runtime includes, and pydantic-settings for its settings, which the deployment package must bundle. It imports nothing from Kafka or Flink, and a test checks that, so the package stays small.
- **Testing:** unit tests call the handler with moto, on a table created from `infra/dynamodb/trending-table.json`, using an API Gateway HTTP API (payload 2.0) event fixture, `tests/fixtures/http_api_get_trending.json`. `make api-local LIMIT=5` builds the same kind of event and calls the handler against DynamoDB Local.

### Deployment (Terraform)

`infra/terraform` defines everything the pipeline runs in AWS, in one region, eu-west-2. Nothing is set up by hand in the console: `make deploy` builds the whole stack and `make destroy` deletes all of it. Kafka, Flink and the results writer still run locally.

- **Table:** created from `infra/dynamodb/trending-table.json`, the file `make db-init` uses, so it has the same keys, on-demand billing and TTL on `ttl`.
- **Read API:** a Lambda running `api/handler.py` on the project's Python version (`.python-version`, 3.11), behind an HTTP API with one route, `GET /trending`, using payload format 2.0. The URL is public, so the stage is throttled to 10 requests a second, with bursts of 20.
- **Logs:** Terraform creates the Lambda's log group with 7-day retention, rather than letting Lambda create one that keeps logs forever and outlives `make destroy`.
- **Least privilege:** the Lambda's role can only `dynamodb:Query` the table and write its own log streams. The writer's role can only `dynamodb:PutItem` to the table. The writer runs outside AWS, so the role trusts the account itself, and a profile with `source_profile` assumes it.
- **Package:** `make package-lambda` builds `build/lambda.zip`, about 3 MB. It holds the handler's modules (`settings.py`, `api/`, `common/`) and the `lambda` dependency group (pydantic-settings), which `poetry export` lists and `pip install --target` installs as Linux x86_64 wheels for the runtime. boto3 comes with the runtime. Timestamps and file order are fixed, and console scripts, which name the building machine's Python, are left out, so the same code gives the same zip on a laptop or in CI, and Terraform
- **State:** in S3, in a versioned bucket named after the account (`recommendation-pipeline-tfstate-<account ID>`), so a deploy from GitHub Actions and one from a laptop work on the same state. Terraform's S3 lock file (`use_lockfile`) stops two from running at once. `.terraform.lock.hcl` is committed, so every machine uses the same provider version.
- **Credentials:** locally, `make tf-plan`, `make deploy` and `make destroy` get the short-lived credentials of `AWS_PROFILE` (default `rp`, from `aws login`) from the AWS CLI. The deploy workflow runs `make deploy AWS_PROFILE=` with the credentials it already has. No access keys are stored anywhere.
- **CI:** `make tf-check` runs `terraform fmt -check` and `terraform validate` on every push, without AWS access.
- **Bootstrap:** `infra/bootstrap` holds what the stack needs before GitHub can deploy it: the state bucket, GitHub's OIDC identity provider and the deploy role. `make bootstrap` applies it once, with your own credentials. Its own state is local and gitignored: it rarely changes, and everything in it could be imported again.
- **Deploying from GitHub Actions:** `.github/workflows/deploy.yml` runs only when started by hand from the Actions tab (`workflow_dispatch`), on main. With `id-token: write`, the job gets a short-lived OIDC token from GitHub and exchanges it for the deploy role's credentials, valid for an hour. The repository holds no AWS keys, only the role's ARN in the `AWS_DEPLOY_ROLE_ARN` variable, which is not a secret.
- **Who can deploy:** the role's trust policy requires the token's audience to be `sts.amazonaws.com` and its subject to be `repo:Cwazycodes/recommendation-pipeline:ref:refs/heads/main`. Runs for pull requests, other branches, tags or environments have other subjects and are refused. A fork can't start the workflow, and GitHub gives workflows on pull requests from forks no OIDC token at all.
- **The deploy role's permissions:** only the stack's resources, by name: the table, the function and its log group, the region's HTTP APIs, the two stack roles (and passing the API's role to Lambda), and the state in the bucket. It can't change its own role, the identity provider or the bucket. It can still edit the stack roles' policies, so a deploy could give the Lambda more access than it has; a permissions boundary on those roles would close that.

### End-to-end demo

`make demo` runs every part of the pipeline on the local stack and prints the trending list the API returns. On a clean clone, `make install && make demo` is all it takes.

- **Steps:** `make up`, then `tools/demo.py`. It waits until Kafka, Flink and DynamoDB Local answer real requests (up to 5 minutes on a first start), rather than relying on Docker health checks, which start a JVM each time and can time out on a busy laptop. Then it does what `make topics` and `make db-init` do for its own topics and table: it creates the table if it is missing and creates empty demo topics. It submits the Flink job, starts the results writer, produces live events for 2 minutes and reports each window as it reaches DynamoDB. Finally it calls the Lambda handler for `GET /trending?limit=10` and prints the run's throughput, consumer lag and latency percentiles from Prometheus (metrics.md). At the end, or on Ctrl-C, it stops its job with a savepoint.
- **Short, live windows:** the job runs with `--window-size-seconds 120 --window-slide-seconds 10`, and the producer runs with `--live`, which stamps each event with the time it is sent. Generated event times start on a fixed date and run about 500 times faster than real time at 500 events a second, so without `--live` 2-minute windows would each cover a fraction of a second of producing. With it, windows follow the wall clock and the first list should appear in under a minute.
- **Kept apart from the 1-hour pipeline:** the demo has its own topics (`viewing-events-demo`, `trending-results-demo`, `viewing-events-dlq-demo`, `trending-writer-dlq-demo`), deleted and created empty on every run, a new writer consumer group each run, and its own partition key, `TRENDING#2m`, in the same table. The exactly-once sink's transactional ID prefix includes the topic, so the demo job can run beside the 1-hour job without fencing its transactions. The 1-hour job's topics, offsets and `TRENDING#1h` items are never touched.
- **Agreed time:** the first window must reach DynamoDB within 90 seconds of the first event. By construction it takes about 50: the first window ends within 10 s, fires once the watermark (newest event time minus 30 s) passes its end, and becomes visible to `read_committed` readers at the next checkpoint, at most 10 s later. `make test-e2e` runs the demo for 60 seconds on the running stack and asserts this.
- **Rate:** 50 events a second. Validation and deduplication run as Python functions; in the RP-19 restart test the job worked through about 110 events a second while sharing the laptop with a second stack. If the producer outpaces the job, the watermark falls behind the wall clock and windows fire late.
- **Memory:** the stack needs most of a 4 GB Docker VM (Kafka, the Flink JobManager and TaskManager, DynamoDB Local, Prometheus and kafka-exporter), so run one stack at a time. Two stacks got containers killed for running out of memory.

### Configuration and logs

Every setting that differs between machines comes from the environment, read with pydantic-settings (`settings.py`). Nothing configurable is written into the code, and `make test` checks that: it fails if a string literal in `src/` or `jobs/` looks like a host or port, or equals a configured table or topic name.

- **Where values come from:** environment variables first, then a `.env` file in the working directory. `.env.example` is committed and lists every variable with the local stack's value and what it is for. `.env` is ignored by git; `make install` and `make up` create it from `.env.example` when it is missing, so a fresh clone runs without editing anything. make never reads `.env` itself, so a variable set in the shell still wins when a tool runs through make. In AWS the same variables are set on the Lambda function and no file is read.
- **No defaults for addresses or names:** hosts, ports, topics, consumer groups, the table and the partition key have no default in the code. A missing one stops the program with an error that names the variable and points at `.env.example`, instead of a silent fallback to a local address. The log level (`INFO`) is the only default.
- **One class per component:** `KafkaSettings` (`KAFKA_*`), `FlinkSettings` (`FLINK_*`), `DynamoDBSettings` (`DYNAMODB_*`), `MetricsSettings` (`METRICS_*`) and `LogSettings` (`LOG_LEVEL`). Each program loads only what it uses, so the Lambda handler needs nothing but the `DYNAMODB_*` variables. `settings.py` imports nothing else from the package, so the Flink job can load it without boto3.
- **Inside the Compose network:** the Flink job's main runs in the JobManager container, where Kafka is `kafka:29092` rather than `localhost:9092`, so the job uses `FLINK_KAFKA_BOOTSTRAP_SERVERS`. Compose passes `.env` to both Flink containers (`env_file`), and the Flink image includes pydantic-settings.
- **Tests read no `.env`:** a fixture in `tests/conftest.py` turns the file off, so each test sees only the variables it sets, and a laptop behaves like CI. The end-to-end tests are the exception, because they run against the local stack.
- **Logs:** the producer, the results writer and the read API write one JSON object per line (`common/logs.py`): `timestamp`, `level`, `service`, `logger` and `message`, plus fields for whatever the line is about. Lines about an event carry its `event_id`; lines about a window carry its `window_end`. `LOG_LEVEL=DEBUG` adds a line for every event the producer delivers.
- **Tracing one result:** the API's line for a request names the `window_end` it served. The writer's `wrote window` line for that `window_end` gives its `generated_at`: the latest `emitted_at` of the window's rows on the trending topic, where the Flink job wrote them. The window holds the events whose `event_time` falls in the hour before `window_end`, and the producer's `event delivered` lines give each one's `event_id`, partition and offset. Events that failed validation are on the DLQ topic with their payload, `event_id` included. For example:

  ```bash
  LOG_LEVEL=DEBUG make produce 2> producer.log
  make writer 2> writer.log
  jq -c 'select(.window_end == "2026-09-21T14:15:00Z")' writer.log
  jq -c 'select(.event_id == "<an event_id>")' producer.log
  ```

### Deduplication

`DeduplicateEvents` keys the validated stream by `event_id` and keeps a `seen` flag per key in Flink `ValueState`. The first copy of an event is forwarded; later copies are dropped and counted in the `duplicate_events` metric.

**State TTL: 1 hour, processing time, counted from the first sighting** (`OnCreateAndWrite`, expired flags never returned).

- **Why not shorter:** duplicates come from client resends and producer retries (seconds) and from Kafka redelivery after a consumer restart or rebalance (seconds to minutes). One hour covers both with a wide margin.
- **Why not longer:** state grows with unique events per second multiplied by the TTL. At the 500 events/s test rate one hour is about 1.8 million keys (500 × 3,600), and every extra hour adds the same again. The heap state backend keeps all of it in TaskManager memory.
- **What it does not catch:** a copy that arrives more than an hour after the first one is counted again, for example a backlog replayed after an outage longer than the TTL. TTL uses processing time, so it measures time since Flink saw the event, not the event's `event_time`.
- **Interaction with checkpoints:** on failure, Flink restores the `seen` state and the Kafka offsets from the same checkpoint, so replayed events are judged against consistent state. Rows emitted after that checkpoint are emitted again; the trending sink writes them in a Kafka transaction that only commits with a checkpoint, so `read_committed` consumers still see each row once.
- **If the trade-off changes:** state lives in RocksDB on disk, so a longer TTL costs disk space rather than TaskManager memory, and event-time timers could expire state by `event_time` instead of wall-clock time.

### Testing against an oracle

`trending_oracle.py` is a plain-Python reference implementation of trending. It imports nothing from the Flink job and models the job's rules directly: drop repeat `event_id`s (first copy wins), keep a watermark of the newest `event_time` minus 30 seconds, fire a window once the watermark reaches `window_end` minus 1 ms, and count an event only in windows that have not fired when it arrives.

The slow tests run the job's own deduplication, rowtime table and trending SQL on an in-memory source in local PyFlink, with no Kafka, and require the output to equal the oracle's on every fixture: ties, late events, duplicates, empty windows, and a generated stream with duplicates and late events. One deliberate difference from production: the tests define the watermark in SQL and emit it after every record, because a bounded source finishes before Flink's periodic watermark fires and no event would ever be late.

`make test` skips these; `make test-flink` runs them, and CI runs them as their own job with Java installed.

## 6. Performance and acceptance criteria

The initial performance target is **p95 event-to-result latency of 30 seconds or less under a documented local test workload**, measured from event ingestion to the corresponding updated result becoming available. This is a provisional engineering target, not a production service-level agreement.

The implementation will measure:
- Sustained event throughput in events per second.
- p50, p95, and p99 processing latency.
- Duplicate-event handling and count correctness.
- Recovery behaviour following a simulated processing failure.
- Result freshness and successful DynamoDB writes.

[metrics.md](metrics.md) defines throughput, consumer lag, freshness and processing latency, and how to read them for a live run. Freshness always includes the 30-second watermark delay, so the latency target applies to processing latency: freshness minus that delay.

[benchmarks.md](benchmarks.md) has the measured results. On a 4-core laptop the job sustains 150 events/s at parallelism 1 and 201 events/s at parallelism 2, with processing p95 within the target up to those rates. Deduplication, a Python operator with per-event state, is the bottleneck.

Correctness takes priority over throughput. The project is successful when automated tests validate aggregation, window boundaries, ranking, duplicate handling, and recovery, and a repeatable benchmark documents the achieved performance.
