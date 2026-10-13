# ДЗ №2: Kafka → ClickHouse

Транзакции из CSV загружаются в топик Kafka, ClickHouse забирает их через
Kafka engine + materialized view в таблицы MergeTree. Есть SQL-запрос
«категория наибольшей транзакции по каждому штату» и сравнение базовой
и оптимизированной схем хранения.

```
data/train.csv ──► producer.py ──► Kafka topic `transactions` (3 партиции)
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
4. `producer` отправляет `data/train.csv` в топик и завершается с кодом 0.

Проверить, что данные доехали (через 5–10 секунд после старта):

```bash
./scripts/check_ingestion.sh
```

```
   ┌─table─────────────────┬──rows─┐
1. │ transactions_baseline │ 19000 │
2. │ transactions          │ 19000 │
   └───────────────────────┴───────┘
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
python producer/producer.py --csv data/train.csv --bootstrap localhost:9094
```

### Другой CSV

В `data/train.csv` лежат 19 000 строк из датасета соревнования. Чтобы загрузить
полный файл с Kaggle, положите его в `data/` и укажите путь:

```bash
docker compose run --rm -e CSV_PATH=/app/data/<файл>.csv producer
```

Нужны те же колонки. Колонка `target` необязательна: без неё будет 0.

## П.1. CSV → Kafka

`producer/producer.py`:
- ждёт готовности Kafka и создаёт топик, если его нет;
- читает CSV через `csv.DictReader`, корректно обрабатывая кавычки и запятые внутри полей
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
поэтому подзапрос и JOIN не нужны. Результат (50 штатов) лежит в
[`results/top_category_by_state.csv`](results/top_category_by_state.csv) и
сверен с независимым расчётом на Python по исходному CSV.

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
его можно ответить из проекции, и читает 50 строк вместо всей таблицы.

**Почему `Decimal`, а не `Float`.** При конвертации Float64 → Decimal
ClickHouse отбрасывает хвост, и `556.18` (в двоичном виде `556.17999…`)
превращается в `556.17`. Поэтому `amount` объявлен как `Decimal32(2)` уже в
Kafka-таблице и разбирается из JSON-текста точно, без промежуточного Float.

**Почему без `PARTITION BY`.** Я пробовал `PARTITION BY toYYYYMM(transaction_time)`.
Запрос п.3 по дате не фильтрует, поэтому прироста скорости нет, а 19 000 строк
дробятся на 15 мелких кусков. В итоге таблица занимала 1.04 MiB вместо 596 KiB.
Партиционирование имеет смысл на объёмах в миллионы строк и при регулярных
запросах или удалении данных по периодам.

### Результаты (`./scripts/benchmark.sh`, 19 000 строк, после `OPTIMIZE FINAL`)

| | baseline (п.2) | optimized (п.4) | |
|---|---|---|---|
| Размер на диске | 1.42 MiB | **596 KiB** | в 2.4 раза меньше |
| Размер без сжатия | 3.30 MiB | 1.29 MiB | в 2.6 раза меньше |
| Прочитано строк запросом п.3 | 19 000 | **50** | в 380 раз меньше |
| Прочитано байт | 714 KiB | **4.7 KiB** | в 150 раз меньше |
| Среднее время, 20 прогонов | 2.2 мс | 1.0 мс | |
| Использована проекция | — | `prj_max_amount_by_state` | |

На 19 000 строк время запроса почти полностью уходит на накладные расходы
(парсинг, планирование), поэтому главный показатель — объём прочитанных данных.
Для baseline он растёт линейно с размером таблицы, а для оптимизированной схемы
остаётся около 50 строк на кусок данных.

Метрики берутся из `system.parts` и `system.query_log`; кэш запросов отключён (`use_query_cache = 0`).
