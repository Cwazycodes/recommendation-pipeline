[![CI](https://github.com/Cwazycodes/recommendation-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Cwazycodes/recommendation-pipeline/actions/workflows/ci.yml)

# Real-Time Recommendation Pipeline

A portfolio project demonstrating reliable, event-driven
recommendations using Python, Kafka, Flink, and AWS.

**Work in progress.**

## Quick start

You need Docker, Python 3.11 and Poetry.

```bash
make install
make demo
```

`make demo` starts Kafka, Flink and DynamoDB Local in Docker and runs the whole pipeline for two minutes. Live viewing events go into Kafka, and Flink ranks programmes by distinct viewers in 2-minute windows sliding every 10 seconds. The results writer copies each window into DynamoDB, and the read API's Lambda handler prints the latest trending list. The first run also builds the Flink image, which takes a few minutes.

It needs ports 9092, 8080, 8081 and 8001 to be free. `make help` lists every command, and [docs/design.md](docs/design.md) explains the design.
