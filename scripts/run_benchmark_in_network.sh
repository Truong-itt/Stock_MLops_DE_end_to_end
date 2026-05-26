#!/usr/bin/env bash
# Run benchmark_pipeline_latency.py inside stock-network so all 3 Kafka
# brokers + ClickHouse + ScyllaDB hostnames are resolvable.
#
# Usage:
#   ./scripts/run_benchmark_in_network.sh                       # default args
#   ./scripts/run_benchmark_in_network.sh --from-earliest       # benchmark historical data
#   ./scripts/run_benchmark_in_network.sh --duration 60 --from-earliest
#
# All extra args are forwarded to the python script.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NETWORK="${STOCK_NETWORK:-stock-network}"

# Auto-detect if docker needs sudo. Override with USE_SUDO=1 or USE_SUDO=0.
if [[ -z "${USE_SUDO:-}" ]]; then
  if docker info >/dev/null 2>&1; then
    USE_SUDO=0
  else
    USE_SUDO=1
  fi
fi
DOCKER=(docker)
if [[ "$USE_SUDO" == "1" ]]; then
  DOCKER=(sudo docker)
  echo "Note: using sudo for docker (user not in docker group)"
fi

echo "Running benchmark in docker network: $NETWORK"
echo "Mounting repo: $REPO_ROOT -> /work"

exec "${DOCKER[@]}" run --rm \
  --network "$NETWORK" \
  -v "$REPO_ROOT":/work \
  -w /work \
  -e KAFKA_BOOTSTRAP="kafka-1:29092" \
  -e SCHEMA_REGISTRY_URL="http://schema-registry:8081" \
  -e CLICKHOUSE_HOST="clickhouse" \
  -e CLICKHOUSE_PORT="8123" \
  -e CLICKHOUSE_DB="stock_warehouse" \
  -e CLICKHOUSE_USER="default" \
  -e CLICKHOUSE_PASSWORD="truongittstock" \
  -e SCYLLA_CONTACT_POINTS="scylla-node1,scylla-node2,scylla-node3" \
  -e SCYLLA_PORT="9042" \
  -e SCYLLA_KEYSPACE="stock_data" \
  python:3.11-slim \
  bash -c '
    set -e
    pip install --quiet --root-user-action=ignore -r scripts/requirements.txt
    python3 scripts/benchmark_pipeline_latency.py "$@"
  ' _ "$@"
