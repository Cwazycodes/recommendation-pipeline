
#!/usr/bin/env bash

set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

create_topic() {
    local topic="$1"
    local partitions="$2"

    echo "Creating topic: ${topic}"

    docker compose exec -T kafka \
        "${KAFKA_TOPICS}" \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --create \
        --if-not-exists \
        --topic "${topic}" \
        --partitions "${partitions}" \
        --replication-factor 1
}

create_topic viewing-events 6
create_topic trending-results 1
create_topic viewing-events-dlq 6

echo "All topics are ready."
