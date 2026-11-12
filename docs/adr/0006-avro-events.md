# 6. Avro events, with the Schema Registry as the judge of a change

**Status:** accepted, 2026-11.

## Context

Events went on `viewing-events` as JSON, checked against a Pydantic model in the jobs. Nothing stopped a producer from changing what it sent: a renamed or retyped field reached the jobs, which dead-lettered every event from then on, and the topic keeps events forever, so a job that later starts from the earliest offset meets every shape the events have ever had. The contract has to be able to grow, one optional field at a time, without breaking the jobs that already read it ([design.md](../design.md), section 3).

## Decision

- **Avro on the events topic,** in the Schema Registry's wire format: a zero byte, the 4-byte id of the schema the event was written with, then the event. The schemas are `schemas/viewing_event/v1.avsc`, `v2.avsc` and so on, inside the package so the Flink jobs have them. A released file is never edited; a change is a new version.
- **A Confluent Schema Registry** in Compose (`schema-registry`, host port 8085). The producer registers the version it writes, under one subject for the record whatever the topic (`recommendation_pipeline.ViewingEvent`), and the registry refuses a version that would break an earlier one.
- **`FULL_TRANSITIVE` compatibility:** every version must read, and be read by, every other. Backward, because a job that is upgraded still reads events written years ago; forward, because a job that has not been upgraded must read events from producers that have; transitive, because the topic holds every version at once.
- **The jobs read every event as v1.** A consumer decodes with the writer's schema, fetched from the registry by id and kept, and its own reader schema; Avro drops what the reader does not know. The jobs need nothing v2 adds, so they stay v1 readers, and v1 readers are what a compatible change must not break.
- **v2 adds one optional field,** `session_id`, null by default: the same id for each event of a sitting.
- **CI asks the registry.** A CI job starts Kafka and the registry from `docker-compose.yml`, and `make schema-check` registers main's versions and then the branch's, in order, in a subject of its own set to `FULL_TRANSITIVE`. A version the registry refuses fails the job with the registry's reasons. Registering main's versions first catches a released file edited in place.
- **`schema_version` leaves the payload.** The registry numbers the versions and every event carries its schema's id. A v2 producer would have had to write 2 in that field, which a v1 reader's `Literal[1]` rejects: a compatible change in Avro would still have broken every consumer.

## Alternatives considered

- **JSON with a JSON Schema in the registry:** keeps the payload readable, but JSON Schema's compatibility rules are looser and newer in the registry, and a JSON event still carries every field name.
- **Protobuf:** compatibility is just as checkable, and generated classes are typed. But Protobuf has no defaults to fill in and no notion of a reader schema, and the Python code generation adds a build step. Avro's schema resolution is the feature this ticket shows.
- **Checking compatibility in Python** (the `avro` package's checker): no service needed, but it cannot be installed beside PyFlink's `avro-python3`, which uses the same module name, and its answer might differ from the registry's, which is the one that refuses a producer.
- **The latest version only (`BACKWARD`, the registry's default):** lets a change break readers that have not been upgraded, or events already in the topic.
- **Reading JSON and Avro side by side** during the move: the usual way to migrate a live topic. The events here are synthetic and regenerated in seconds, so the local topic is emptied once instead, and the jobs only ever see Avro.
- **The Flink Avro format** (`avro-confluent`) in Flink SQL: would decode in Java, but the jobs validate and dead-letter in Python on the DataStream API, and moving that into SQL would lose the dead letters.

## Consequences

- An event is smaller, 104 bytes in v1 and 142 in v2 against 314 as JSON, and a field cannot be misspelt.
- The jobs need the registry once for each schema id they meet, then never again. If it cannot be reached at that moment, the job fails and Flink restarts it until it can, rather than dead-lettering good events.
- Malformed test events, with a string in a number field, cannot be written in Avro. The producer sends those as the JSON they are, the way a producer without the schema would, and the job dead-letters them as `not_avro`.
- PyFlink has no deserializer that hands Python raw bytes, so the Kafka sources decode each value as ISO-8859-1, which maps every byte to one character, and the jobs encode it back.
- The registry adds about 200 MB to a stack that already fills most of a 4 GB Docker VM, and one more container to start.
- `make schema-check` and the registry tests need the registry, so they run in a CI job of their own, not in `make test`.
