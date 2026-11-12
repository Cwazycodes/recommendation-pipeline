# Reliability drills

"Reliable" only counts if the pipeline survives things going wrong, so `make chaos` breaks it on purpose in six ways and checks the outcome the design promises for each. Below, each drill has the script that runs it, the expected outcome and the reason for it, and the result recorded by the last full run.

## Running them

```bash
make up
make chaos                                   # all six, about 25 minutes; records the results below
make chaos CHAOS_DRILLS="writer dynamodb"    # some of them; prints the results without recording them
```

The drills have their own topics (`viewing-events-chaos`, `trending-results-chaos`, `viewing-events-dlq-chaos`, `trending-writer-dlq-chaos`, and for drill 5 `viewing-events-cw-chaos`, `continue-watching-chaos` and `continue-watching-writer-dlq-chaos`), their own partition key (`CHAOS#1h`) and, for drill 5, their own table (`continue-watching-chaos`), so they never read or change the pipeline's data. They do kill, restart and stop the shared containers, so anything else running on the stack is disrupted while they run. Whatever a drill stops is started again, even if the drill fails. The script is [`tools/chaos.py`](../src/realtime_recommendation_pipeline/tools/chaos.py).

## 1. TaskManager killed mid-stream (`taskmanager`)

- **What the script does:** runs the production job (1-hour windows every 5 minutes) on the chaos topics and produces 6,000 generated events plus 276 duplicates, 50 a second. Once a checkpoint has completed and results have been committed, it kills the TaskManager with SIGKILL and starts it again 10 seconds later. Docker's `restart: unless-stopped` does not apply to a container stopped with `docker kill`, so the script restarts it the way an orchestrator would; the restart policy covers a JVM that crashes on its own.
- **Expected:** the job restores from its last completed checkpoint and carries on. Every row committed to `trending-results-chaos` equals what `trending_oracle.py` computes from the same events, up to the last window the job emitted, with none missing, changed or repeated.
- **Why:** a checkpoint holds the Kafka offsets, the deduplication flags and the window state together, so replayed events are judged against consistent state. The Kafka transaction that was open when the TaskManager died is aborted and its rows are written again in a new one, so `read_committed` readers see each row once. The oracle's last windows are left out of the comparison because it fires every window at the end of its input, while the job only fires windows its watermark has passed.

## 2. Kafka broker restarted mid-stream (`kafka`)

- **What the script does:** the same job and events as drill 1, but instead of the TaskManager it restarts the Kafka broker (`docker compose restart kafka`).
- **Expected:** the producer delivers all 6,276 records with no failures and the topic holds exactly that many, the job recovers, and its committed output matches the oracle as in drill 1.
- **Why:** the producer is idempotent with `acks=all`, so it holds unsent records while the broker is down and retries them without creating duplicates. Flink's Kafka source reconnects, and if the sink's transaction fails the job restarts from its last checkpoint as in drill 1. The recorded result says whether the job had to restart.

## 3. Writer killed mid-batch (`writer`)

- **What the script does:** writes 60 windows of ranked rows to `trending-results-chaos`, five a second, each in its own Kafka transaction as the Flink job does, and runs the results writer as a separate process. Right after its 10th, 25th and 40th write it kills the writer with SIGKILL and starts it again at once. At that moment the writer has read the first row of the next window, and may or may not have committed the window it just wrote.
- **Expected:** the table ends with exactly one complete item per window under `CHAOS#1h`, identical to what a writer that never stopped would leave, with no window missing and no rank repeated (`check_writer.compare`).
- **Why:** offsets are committed only after a window's item is written, and only up to the window still being read. A restart therefore replays the partly read window, plus the window just written if the kill came before its commit, and writing a window again writes identical data.
- **Timing:** a commit takes milliseconds, so in practice the kill nearly always lands after it, and the recorded result says how many windows were written twice. The rarer case, a kill between a window's write and its commit, is covered deterministically by `test_kill_between_write_and_commit_rewrites_the_same_item`.

## 4. DynamoDB unreachable (`dynamodb`)

- **What the script does:** the same kind of input at one window a second. After 10 writes it stops DynamoDB Local for 30 seconds, then starts it again.
- **Expected:** while DynamoDB is down the writer writes nothing and commits nothing (its committed offset is the same 5 s and 30 s into the outage), logs a backed-off retry each time, and dead-letters nothing. Once DynamoDB is back it catches up, and the table matches the topic as in drill 3.
- **Why:** refused and timed-out connections are not `ClientError`s, so they never count towards the five tries that lead to the DLQ. The writer waits them out with backoff from 1 s up to 30 s, with jitter, and commits nothing meanwhile. It does not poll Kafka while it waits; heartbeats keep it in its group, and `max.poll.interval.ms` is 30 minutes, so an outage up to that long does not cost it its partition.

## 5. Continue watching: TaskManager and writer killed (`continue-watching`)

- **What the script does:** runs the continue-watching job and its writer, as a separate process, on topics and a table of their own, with the same 6,276 events as drills 1 and 2. Once a checkpoint has completed and changes have been committed, it kills the TaskManager with SIGKILL and starts it again 10 seconds later. Once the writer has written three more batches, it kills the writer with SIGKILL and starts it again at once.
- **Expected:** the table ends with exactly the programmes `continue_watching_oracle.py` has in progress for the same events: the same users and programmes, with the same position, duration and last-watched time, and nothing missing or extra.
- **Why:** the job's keyed state and the source offsets come back from the same checkpoint, and the open transaction is aborted and its changes published again, as in drill 1, so the committed changes are what an uninterrupted job would publish. The writer commits after it writes, so a restart replays at most a batch, and applying changes again in order leaves the same table. Unlike drills 1 to 4 it checks the end result, the table, rather than each step.

## 6. Producers move to schema v2 mid-stream (`schema`)

- **What the script does:** runs the production job on the chaos topics with the same 6,276 events as drill 1, but the producer writes the first half in v1 of the event schema and the second half in v2, which adds `session_id`, the way producers upgraded one by one would. The job is left alone: it is not stopped, redeployed or told about v2.
- **Expected:** the job reads through the switch without restarting, dead-letters nothing, and every committed row matches `trending_oracle.py`, as in drill 1.
- **Why:** each event carries the id of the schema it was written with. The job fetches v2 from the registry the first time it meets that id and reads the event with its own v1 schema, which drops `session_id`. v2 only adds an optional field, which `FULL_TRANSITIVE` allows, so a compatible change reaches the topic with nothing downstream redeployed. A breaking change never gets this far: the registry refuses it when the producer starts, which `make test-registry` and CI check ([design.md](design.md), section 3).

## What these drills do not show

- **One broker, one TaskManager, local disks.** Losing a disk loses the checkpoints or the Kafka log; surviving that needs replicated topics and remote checkpoint storage.
- **Outages longer than 30 minutes.** The writer would leave its group, its next commit would fail and it would stop. Restarted, it carries on from its last commit, so nothing is lost, but it does not do this by itself.
- **Real network failures.** The DynamoDB drill stops a container, which refuses connections. In AWS an outage is more likely to show up as timeouts; the writer treats both the same way, and its unit tests cover both.

## Recorded results

<!-- chaos-results:start -->

Recorded with `make chaos` on 2026-11-12 21:07 UTC, commit `da47723 plus local changes`, Docker VM with 8 CPUs and 3.8 GB.

| Drill | Result | Observed |
|---|---|---|
| TaskManager killed mid-stream | Pass | TaskManager killed with 2 checkpoint(s) completed; running again 56 s later, restored from checkpoint 2<br>6276 of 6276 events delivered, 6276 in the topic<br>374 committed rows in 22 windows match the oracle, none repeated |
| Kafka broker restarted mid-stream | Pass | broker back after 22 s; the job rode it out without restarting<br>6276 of 6276 events delivered, 6276 in the topic<br>374 committed rows in 22 windows match the oracle, none repeated |
| Writer killed mid-batch | Pass | killed 3 times mid-stream; 60 writes for 60 windows, so 0 written again after a kill<br>60 items for 60 windows: all complete, none repeated |
| DynamoDB unreachable | Pass | outage 31 s: 8 backed-off retries, committed offset 252 -> 252, 0 writes<br>caught up 7 s after the last window<br>60 items for 60 windows: all complete, none repeated |
| Continue watching: TaskManager and writer killed | Pass | TaskManager killed with 1 checkpoint(s) completed; running again 28 s later, restored from checkpoint 1<br>writer killed after 5 batches and started again at once<br>104 in-progress programmes for 101 users match the oracle; the 4 finished ones are not listed |
| Producers move to schema v2 mid-stream | Pass | the first 3138 events written as v1 (schema id 27) and the other 3138 as v2 (id 28)<br>the job read both without restarting<br>6276 of 6276 events delivered, 6276 in the topic<br>374 committed rows in 22 windows match the oracle, none repeated<br>nothing dead-lettered |

<!-- chaos-results:end -->
