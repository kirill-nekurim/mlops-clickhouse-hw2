# ДЗ №2: Kafka → ClickHouse

Транзакции из CSV загружаются в топик Kafka, ClickHouse забирает их через
Kafka engine + materialized view в таблицы MergeTree. Есть SQL-запрос
«категория наибольшей транзакции по каждому штату» и сравнение базовой
и оптимизированной схем хранения.

```
data/train.csv.xz ──► producer.py ──► Kafka topic `transactions` (3 партиции)
                                        │
                ┌───────────────────────┴───────────────────────┐
                ▼ consumer group clickhouse_baseline            ▼ consumer group clickhouse_optimized
   fraud.kafka_transactions_baseline (Kafka engine)   fraud.kafka_transactions (Kafka engine)
                ▼ mv_transactions_baseline                      ▼ mv_transactions
   fraud.transactions_baseline (MergeTree, п.2)       fraud.transactions (MergeTree, п.4)
```

Обе схемы читают один и тот же топик разными consumer group, поэтому
данные в них одинаковые и их можно честно сравнивать.

## Структура

| Путь | Что это | Пункт ДЗ |
|---|---|---|
| `producer/producer.py` | загрузка CSV в топик Kafka (JSON на строку) | 1 |
| `sql/01_ddl_baseline.sql` | DDL: Kafka-таблица → MV → MergeTree, базовая схема | 2 |
| `sql/03_query_top_category_by_state.sql` | запрос: категория наибольшей транзакции по штату | 3 |
| `results/top_category_by_state.csv` | результат запроса | 3 |
| `sql/02_ddl_optimized.sql` | оптимизированный DDL | 4 |
| `results/benchmark.txt` | сравнение схем: размер и выполнение запроса | 4 |
| `scripts/*.sh` | запуск запроса, проверка загрузки, бенчмарк | |
| `docker-compose.yml` | Kafka (KRaft), создание топика, ClickHouse, producer | |

## Запуск

Нужны Docker и Docker Compose v2. Свободные порты: `8123` (ClickHouse HTTP) и `9094` (Kafka для хоста).

```bash
git clone https://github.com/kirill-nekurim/mlops-clickhouse-hw2.git
cd mlops-clickhouse-hw2
docker compose up -d --build
```

Что происходит при старте:
1. `kafka` поднимается в режиме KRaft (без ZooKeeper);
2. `kafka-init` создаёт топик `transactions` и завершается;
3. `clickhouse` при первом старте выполняет `sql/01_ddl_baseline.sql` и
   `sql/02_ddl_optimized.sql` (смонтированы в `/docker-entrypoint-initdb.d`);
4. `producer` отправляет `data/train.csv.xz` (786 431 строка) в топик и завершается с кодом 0.

Проверить, что данные доехали (через ~15–20 секунд после старта):

```bash
./scripts/check_ingestion.sh
```

```
   ┌─table─────────────────┬───rows─┐
1. │ transactions_baseline │ 786431 │
2. │ transactions          │ 786431 │
   └───────────────────────┴────────┘
```

Запрос из п.3 с сохранением результата в `results/top_category_by_state.csv`:

```bash
./scripts/run_query.sh
```

Сравнение схем (п.4) с сохранением в `results/benchmark.txt`:

```bash
./scripts/benchmark.sh
```

Остановить и удалить данные:

```bash
docker compose down -v
```

### Полезное

```bash
# консоль ClickHouse
docker compose exec clickhouse clickhouse-client

# логи загрузки
docker compose logs producer

# загрузить данные ещё раз (строки задублируются)
docker compose run --rm producer
```

Producer можно запустить и с хоста (Kafka доступна на `localhost:9094`):

```bash
pip install -r requirements.txt
python producer/producer.py --csv data/train.csv.xz --bootstrap localhost:9094
```

### Данные

`data/train.csv.xz` — полный `train.csv` из соревнования
[teta_ml_1_2025](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394)
(786 431 строка, 145 MB), сжатый xz до 15 MB: GitHub не принимает файлы больше 100 MB.
Распаковывать не нужно, producer читает `.xz`, `.gz` и обычный `.csv`.

Загрузить другой файл с теми же колонками (например, `test.csv`, где нет `target` — будет 0):

```bash
docker compose run --rm -e CSV_PATH=/app/data/<файл>.csv producer
```

## П.1. CSV → Kafka

`producer/producer.py`:
- ждёт готовности Kafka и создаёт топик, если его нет;
- читает CSV (в том числе сжатый `.xz`/`.gz`) через `csv.DictReader`, корректно обрабатывая кавычки и запятые внутри полей
  (например, `"Engineer, production"`);
- приводит числовые поля к числам и отправляет каждую строку JSON-сообщением
  (формат `JSONEachRow` для ClickHouse);
- использует idempotent producer, батчирование и lz4-сжатие; при ошибке доставки
  завершается с ненулевым кодом.

## П.2. Таблицы в ClickHouse

`sql/01_ddl_baseline.sql` — классическая связка из трёх объектов:

1. `fraud.kafka_transactions_baseline` (`ENGINE = Kafka`) — подписка на топик;
2. `fraud.transactions_baseline` (`ENGINE = MergeTree`) — хранение;
3. `fraud.mv_transactions_baseline` — materialized view, которая на каждый
   прочитанный из Kafka блок вставляет строки в MergeTree.

Типы колонок взяты «как в CSV» (`String`, `Float64`, `Int64`), `ORDER BY tuple()`.
Это точка отсчёта для оптимизации.

`clickhouse/config.d/kafka.xml` задаёт `auto_offset_reset = earliest`:
новая consumer group читает топик с начала, так что сообщения, отправленные
до создания таблиц, не теряются.

## П.3. Категория наибольшей транзакции по штату

```sql
SELECT
    us_state,
    argMax(cat_id, amount) AS top_category,
    max(amount)            AS max_amount
FROM fraud.transactions
GROUP BY us_state
ORDER BY us_state
```

`argMax(cat_id, amount)` возвращает `cat_id` из строки с максимальным `amount`,
поэтому подзапрос и JOIN не нужны. Результат (51 строка: 50 штатов и DC) лежит в
[`results/top_category_by_state.csv`](results/top_category_by_state.csv) и
сверен с независимым расчётом на Python по исходному CSV. Ничьих (двух транзакций
с одинаковой максимальной суммой в штате) в данных нет, иначе `argMax` вернул бы одну из них.

Запрос одинаково работает и на `fraud.transactions_baseline`. `benchmark.sh`
выполняет его на обеих таблицах и проверяет, что результаты совпадают.

## П.4. Оптимизация хранения

`sql/02_ddl_optimized.sql` — изменённый DDL из п.2:

| Что | Было | Стало | Зачем |
|---|---|---|---|
| `transaction_time` | `String` | `DateTime` + `CODEC(Delta, ZSTD)` | 4 байта вместо 16, работают функции дат |
| `amount` | `Float64` | `Decimal32(2)` | точная сумма в 4 байтах, см. ниже |
| `us_state`, `cat_id`, `merch`, `one_city`, `jobs`, `name_*` | `String` | `LowCardinality(String)` | словарное кодирование: 14–900 уникальных значений |
| `gender` | `String` | `Enum8('F','M')` | 1 байт |
| координаты | `Float64` | `Float32` | точности 6–7 знаков хватает |
| `post_code`, `population_city`, `target` | `Int64` | `UInt32` / `UInt8` | меньше байт |
| ключ сортировки | `tuple()` | `(us_state, cat_id, transaction_time)` | строки штата лежат рядом: лучше сжатие, отсечение гранул по штату/категории |
| проекция | — | `prj_max_amount_by_state` | предагрегат под запрос п.3 |

**Проекция** хранит внутри таблицы `argMax(cat_id, amount)` и `max(amount)`
по каждому штату. Она заполняется автоматически при вставке и пересчитывается
при слиянии кусков. Запрос из п.3 менять не нужно: ClickHouse сам видит, что
его можно ответить из проекции, и читает 51 строку вместо всей таблицы.

**Почему `Decimal`, а не `Float`.** При конвертации Float64 → Decimal
ClickHouse отбрасывает хвост, и `556.18` (в двоичном виде `556.17999…`)
превращается в `556.17`. Поэтому `amount` объявлен как `Decimal32(2)` уже в
Kafka-таблице и разбирается из JSON-текста точно, без промежуточного Float.

**Почему без `PARTITION BY`.** Я пробовал `PARTITION BY toYYYYMM(transaction_time)`.
Запрос п.3 по дате не фильтрует, поэтому прироста скорости нет, а данные
(январь 2019 — март 2020) дробятся на 15 кусков, и каждый хранит свои словари
`LowCardinality` и свою проекцию. На полном датасете таблица выросла с 18.0 до
20.2 MiB. Партиционирование стоит включать на объёмах в десятки миллионов строк и
при регулярных запросах или удалении данных по периодам.

### Результаты (`RUNS=30 ./scripts/benchmark.sh`, 786 431 строка, после `OPTIMIZE FINAL`)

| | baseline (п.2) | optimized (п.4) | |
|---|---|---|---|
| Размер на диске | 58.4 MiB | **18.0 MiB** | в 3.2 раза меньше |
| Размер без сжатия | 136.5 MiB | 50.1 MiB | в 2.7 раза меньше |
| Прочитано строк запросом п.3 | 786 431 | **51** | в ~15 000 раз меньше |
| Прочитано байт | 28.9 MiB | **4.8 KiB** | в ~6 000 раз меньше |
| Память на запрос | 2.3 MiB | 62 KiB | |
| Среднее время, 30 прогонов | 7.2 мс | **1.1 мс** | в 7 раз быстрее |
| Использована проекция | — | `prj_max_amount_by_state` | |

Для baseline объём прочитанных данных и время растут линейно с размером таблицы,
а оптимизированная схема читает ~51 строку на кусок данных при любом объёме.
Время на оптимизированной таблице — это в основном накладные расходы на
парсинг и планирование запроса.

Метрики берутся из `system.parts` и `system.query_log`; кэш запросов отключён (`use_query_cache = 0`).
