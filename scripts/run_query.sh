#!/usr/bin/env bash
# П.3: выполняет запрос из sql/03_query_top_category_by_state.sql
# и сохраняет результат в results/top_category_by_state.csv
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=results/top_category_by_state.csv
mkdir -p results

docker compose exec -T clickhouse clickhouse-client \
    --queries-file /sql/03_query_top_category_by_state.sql \
    --format CSVWithNames > "$OUT"

echo "Сохранено: $OUT ($(($(wc -l < "$OUT") - 1)) строк)"
head -n 6 "$OUT"
