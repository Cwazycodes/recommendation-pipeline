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

`make demo` starts Kafka, Flink, DynamoDB Local and Prometheus (with kafka-exporter) in Docker and runs the whole pipeline for two minutes. Live viewing events go into Kafka, and Flink ranks programmes by distinct viewers in 2-minute windows sliding every 10 seconds. The results writer copies each window into DynamoDB, and the read API's Lambda handler prints the latest trending list. The first run also builds the Flink image, which takes a few minutes.

`make install` also copies `.env.example` to `.env`, which holds every host, port, topic and table name; change `.env` to point the pipeline somewhere else. It needs ports 9092, 8080, 8081, 8001, 9090 and 9464 to be free. `make metrics` prints throughput, consumer lag and latency percentiles for a live run ([docs/metrics.md](docs/metrics.md)), and `make bench` measures them at several rates ([docs/benchmarks.md](docs/benchmarks.md)).

## Deploy to AWS

The trending table and the read API also run in AWS, in eu-west-2, defined in Terraform ([docs/design.md](docs/design.md), "Deployment"). You need Terraform 1.17 or later and the AWS CLI, signed in with `aws login --profile rp`.

Once per account, `make bootstrap` creates the Terraform state bucket and the role GitHub Actions deploys with. After that, run **Deploy** from the repository's Actions tab, which assumes the role through OIDC with no stored keys, or deploy from your machine:

```bash
make deploy    # builds the Lambda package, then creates the table, Lambda, HTTP API, logs and IAM roles
curl "$(terraform -chdir=infra/terraform output -raw api_url)"
make destroy   # deletes all of it
```

`make tf-plan` shows what `make deploy` would change without changing anything. Both ways share the state in S3, so either can update or delete what the other deployed.
