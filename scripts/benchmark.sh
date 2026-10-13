#!/usr/bin/env bash
# П.4: сравнение базовой и оптимизированной таблиц —
# размер на диске и выполнение запроса из п.3 на каждой из них.
set -euo pipefail
cd "$(dirname "$0")/.."

RUNS=${RUNS:-20}
OUT=results/benchmark.txt
mkdir -p results

ch() { docker compose exec -T clickhouse clickhouse-client "$@"; }

QUERY_OPT=$(grep -v '^--' sql/03_query_top_category_by_state.sql)
QUERY_BASE=${QUERY_OPT//fraud.transactions/fraud.transactions_baseline}

# Слить куски, чтобы сравнение не зависело от того, как пришли батчи из Kafka
ch --query "OPTIMIZE TABLE fraud.transactions_baseline FINAL"
ch --query "OPTIMIZE TABLE fraud.transactions FINAL"

# Результаты на обеих таблицах должны совпадать
# (сумма приводится к одному формату, т.к. Float64 vs Decimal)
norm() { ch --format TSV --query "SELECT us_state, top_category, round(toFloat64(max_amount), 2) FROM ($1)"; }
if diff <(norm "$QUERY_BASE") <(norm "$QUERY_OPT") > /dev/null; then
    CHECK="результаты на обеих таблицах совпадают"
else
    CHECK="ВНИМАНИЕ: результаты различаются"
fi

TAG="bench_$(date +%s)"
for i in $(seq "$RUNS"); do
    ch --log_comment "${TAG}_baseline" --use_query_cache 0 --format Null --query "$QUERY_BASE"
    ch --log_comment "${TAG}_optimized" --use_query_cache 0 --format Null --query "$QUERY_OPT"
done
ch --query "SYSTEM FLUSH LOGS"

{
    echo "=== Хранение (после OPTIMIZE FINAL) ==="
    ch --format PrettyCompactMonoBlock --query "
        SELECT
            table,
            sum(rows)                                        AS rows,
            count()                                          AS parts,
            formatReadableSize(sum(data_compressed_bytes))   AS compressed,
            formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
            round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
        FROM system.parts
        WHERE database = 'fraud' AND active
        GROUP BY table
        ORDER BY table"

    echo
    echo "=== Запрос п.3, $RUNS прогонов на каждую таблицу ==="
    ch --format PrettyCompactMonoBlock --query "
        SELECT
            replaceOne(log_comment, '${TAG}_', '')   AS variant,
            count()                                  AS runs,
            round(avg(query_duration_ms), 2)         AS avg_ms,
            round(quantile(0.5)(query_duration_ms), 2) AS p50_ms,
            any(read_rows)                           AS read_rows,
            formatReadableSize(any(read_bytes))      AS read_bytes,
            formatReadableSize(avg(memory_usage))    AS avg_memory,
            any(projections)                         AS projections
        FROM system.query_log
        WHERE type = 'QueryFinish' AND log_comment LIKE '${TAG}_%'
        GROUP BY variant
        ORDER BY variant"

    echo
    echo "Проверка: $CHECK"
} | tee "$OUT"

echo "Сохранено: $OUT"
