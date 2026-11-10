"""Заливаем исходный CSV в пустую raw-таблицу, повторно строки не добавляем."""

import base64
import csv
import json
import lzma
import os
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

HOST = os.getenv('CLICKHOUSE_HOST', '127.0.0.1')
PORT = os.getenv('CLICKHOUSE_PORT', '18123')
USER = os.getenv('CLICKHOUSE_USER', 'dbt')
PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'dbt_local')


def query(sql, body=b''):
    url = f'http://{HOST}:{PORT}/?{urlencode({"query": sql})}'
    auth = base64.b64encode(f'{USER}:{PASSWORD}'.encode()).decode()
    request = Request(url, data=body, headers={'Authorization': f'Basic {auth}'})
    try:
        with urlopen(request, timeout=120) as response:
            return response.read().decode().strip()
    except HTTPError as error:
        raise RuntimeError(error.read().decode()) from error


def main():
    count = int(query('SELECT count() FROM transactions_db.transactions'))
    if count:
        print(f'В raw уже {count} строк, ничего не дописываем')
        return

    path = Path(__file__).resolve().parents[2] / 'data' / 'train.csv.xz'
    numeric = {'amount', 'lat', 'lon', 'population_city', 'merchant_lat', 'merchant_lon'}
    # Частичная загрузка остаётся в отдельной таблице. Raw меняется только в конце.
    query('CREATE TABLE IF NOT EXISTS transactions_db.transactions_loading AS transactions_db.transactions')
    query('TRUNCATE TABLE transactions_db.transactions_loading')
    payload, total = [], 0
    with lzma.open(path, 'rt', newline='') as stream:
        for row in csv.DictReader(stream):
            row['transaction_time'] = datetime.fromisoformat(row['transaction_time']).strftime('%Y-%m-%d %H:%M:%S')
            for column in numeric:
                row[column] = float(row[column]) if row[column] else None
            row['target'] = int(row['target']) if row['target'] else None
            payload.append(json.dumps(row, ensure_ascii=False, allow_nan=False))
            total += 1
            if len(payload) >= 10000:
                insert_batch(payload)
                payload = []
    if payload:
        insert_batch(payload)
    query('EXCHANGE TABLES transactions_db.transactions AND transactions_db.transactions_loading')
    query('DROP TABLE transactions_db.transactions_loading')
    print(f'Загружено {total} строк из data/train.csv.xz')


def insert_batch(rows):
    query('INSERT INTO transactions_db.transactions_loading FORMAT JSONEachRow',
          ('\n'.join(rows) + '\n').encode())


if __name__ == '__main__':
    main()
