# 4. Hybrid: streaming on a laptop, storage and the API in AWS

**Status:** accepted, 2026-11.

## Context

The pipeline should run against real cloud services, so the storage, the API and the way they are deployed and secured are real rather than emulated. The budget is close to nothing, and the pipeline is idle almost all the time. Managed Kafka and Flink bill by the hour whether or not events flow ([design.md](../design.md), "Production mapping").

## Decision

- **On the laptop, in Docker:** Kafka, Flink, Prometheus, and the results writer.
- **In AWS (eu-west-2), defined in Terraform:** the DynamoDB table, the read API (Lambda behind an HTTP API), its logs, and least-privilege IAM roles for the API and the writer.
- **Deployed from GitHub Actions** through OIDC, with a role that only `main` of this repository can assume, so no AWS keys are stored anywhere.
- **The writer** reaches the real table as its own role, which can only put items (`make writer-aws`).

## Alternatives considered

- **Everything local,** with DynamoDB Local and the handler called directly: free, and it is still how the demo, the tests and the drills run, but nothing would show the cloud half working.
- **Everything in AWS,** with Amazon MSK and Amazon Managed Service for Apache Flink: the production shape, but the smallest version costs about $280 a month, idle or not.

## Consequences

- The cloud half is pay-per-request and costs cents. The API is throttled to 1 request a second, which caps what reaches the Lambda.
- The streaming half is proven on a laptop, by the tests, the failure drills and the benchmarks, not on managed services. Its throughput is the laptop's.
- The network to AWS does not matter at this scale: a put to DynamoDB in London took 28 ms at the median, once per window ([benchmarks.md](../benchmarks.md), "DynamoDB in AWS").
- Moving to production means swapping the laptop's services for managed ones without changing the job's logic ([design.md](../design.md), "Production mapping").
