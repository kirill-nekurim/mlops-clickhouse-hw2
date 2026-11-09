## ДЗ4: dbt-проект поверх транзакций ClickHouse

Добавлен каталог `dbt/`: raw source → staging → шесть аналитических витрин.
Используется полный датасет из ДЗ2, `data/train.csv.xz` — 786 431 транзакция.
Есть отдельный воспроизводимый стенд ClickHouse и подключение к исходной
`fraud.transactions` через переменные окружения.

Версии: dbt-core **1.10.15**, dbt-clickhouse **1.10.3**, ClickHouse **24.8.14.39**,
SQLFluff **3.5.0**.

Витрины:

- `mart_daily_state_metrics` — дневные метрики по штатам, P95 и доля крупных операций;
- `mart_fraud_by_category` — количество и суммы фрода по категориям;
- `mart_fraud_by_state` — география фрода, уникальные клиенты и мерчанты;
- `mart_customer_risk_profile` — история клиента и HIGH/MEDIUM/LOW;
- `mart_hourly_fraud_pattern` — фрод по дню недели и часу;
- `mart_merchant_analytics` — оборот, fraud_rate и флаг подозрительности.

Собственный макрос `amount_bucket`, пакеты `dbt_utils`, `dbt_date`,
`dbt_expectations`, metadata и тесты описаны в YAML. Добавлены SQLFluff,
`.pre-commit-config.yaml` и `Makefile`.

Проверки на полном датасете:

- `dbt seed` — 51 строка справочника штатов;
- `dbt run` — успешно построены staging и 6 витрин;
- `dbt test` — **PASS=49, WARN=0, ERROR=0, SKIP=0**;
- из них 45 data tests, включая 5 singular SQL-тестов, и 4 native unit tests;
- проверены уникальность простых/составных ключей, обязательные поля,
  категории, диапазоны, связи штатов и сверка количества/сумм/фродов со staging;
- unit tests покрывают очистку и границы суммы, ISO день недели,
  P95/средний чек/долю крупных операций, проценты фрода и сегменты риска;
- `sqlfluff lint models tests` — успешно;
- `pre-commit run --all-files` — успешно;
- `dbt docs generate` — успешно.

Лог: [dbt/docs/dbt-test.log](dbt/docs/dbt-test.log).
Запуск и определения метрик: [dbt/README.md](dbt/README.md).

Скриншот dbt Docs:

![Граф raw → staging → marts и тесты](dbt/docs/dbt-dag.jpg)
