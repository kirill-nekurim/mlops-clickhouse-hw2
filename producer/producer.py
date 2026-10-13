"""Загрузка транзакций из CSV в топик Kafka.

Каждая строка CSV отправляется отдельным JSON-сообщением (формат JSONEachRow,
который читает Kafka engine в ClickHouse). Числовые поля приводятся к числам,
transaction_time остаётся строкой — в DateTime её переводит ClickHouse.
"""
import argparse
import csv
import gzip
import json
import logging
import lzma
import os
import sys
import time

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

INT_FIELDS = {"post_code", "population_city", "target"}
# float -> json.dumps даёт кратчайшее представление (148.04 остаётся 148.04),
# поэтому ClickHouse может разобрать amount сразу в Decimal без потери точности
FLOAT_FIELDS = {"amount", "lat", "lon", "merchant_lat", "merchant_lon"}


def convert(row: dict) -> dict:
    out = {}
    for key, value in row.items():
        if value == "":
            out[key] = None
        elif key in INT_FIELDS:
            out[key] = int(float(value))
        elif key in FLOAT_FIELDS:
            out[key] = float(value)
        else:
            out[key] = value
    return out


def open_csv(path: str):
    """CSV можно передать как есть или сжатым (.xz / .gz)."""
    if path.endswith(".xz"):
        return lzma.open(path, "rt", newline="", encoding="utf-8")
    if path.endswith(".gz"):
        return gzip.open(path, "rt", newline="", encoding="utf-8")
    return open(path, newline="", encoding="utf-8")


def wait_for_kafka(bootstrap: str, timeout: int = 120) -> AdminClient:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.time() + timeout
    while True:
        try:
            admin.list_topics(timeout=5)
            return admin
        except KafkaException as exc:
            if time.time() > deadline:
                raise
            log.info("Kafka ещё недоступна (%s), жду...", exc)
            time.sleep(3)


def ensure_topic(admin: AdminClient, topic: str, partitions: int) -> None:
    if topic in admin.list_topics(timeout=10).topics:
        log.info("Топик %s уже существует", topic)
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    for name, fut in futures.items():
        try:
            fut.result()
            log.info("Создан топик %s (%d партиций)", name, partitions)
        except KafkaException as exc:
            # топик мог успеть создаться параллельно
            log.warning("create_topics(%s): %s", name, exc)


def main() -> int:
    parser = argparse.ArgumentParser(description="CSV -> Kafka")
    parser.add_argument("--csv", default=os.getenv("CSV_PATH", "data/train.csv.xz"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9094"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions"))
    parser.add_argument("--partitions", type=int, default=int(os.getenv("KAFKA_PARTITIONS", "3")))
    args = parser.parse_args()

    admin = wait_for_kafka(args.bootstrap)
    ensure_topic(admin, args.topic, args.partitions)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "enable.idempotence": True,
    })

    errors = 0

    def on_delivery(err, _msg):
        nonlocal errors
        if err is not None:
            errors += 1
            log.error("Ошибка доставки: %s", err)

    sent = 0
    started = time.time()
    with open_csv(args.csv) as f:
        for row in csv.DictReader(f):
            payload = json.dumps(convert(row), ensure_ascii=False).encode("utf-8")
            while True:
                try:
                    producer.produce(args.topic, value=payload, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.5)  # локальная очередь заполнена — ждём отправки
            sent += 1
            if sent % 100000 == 0:
                producer.poll(0)
                log.info("Отправлено %d сообщений", sent)

    producer.flush(60)
    log.info("Готово: %d сообщений в топик %s за %.1f c, ошибок: %d",
             sent, args.topic, time.time() - started, errors)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
