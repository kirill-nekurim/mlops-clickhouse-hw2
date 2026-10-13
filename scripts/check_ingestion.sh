#!/usr/bin/env bash
# Сколько строк уже доехало из Kafka в обе таблицы
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T clickhouse clickhouse-client --format PrettyCompactMonoBlock --query "
SELECT 'transactions_baseline' AS table, count() AS rows FROM fraud.transactions_baseline
UNION ALL
SELECT 'transactions', count() FROM fraud.transactions
"
