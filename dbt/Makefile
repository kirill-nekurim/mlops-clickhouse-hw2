PYTHON ?= python3
DBT ?= dbt
COMPOSE = docker compose -f compose.yml

.PHONY: install up load deps seed run test unit-test build docs docs-serve lint format check down

install:
	$(PYTHON) -m pip install -r requirements.txt

up:
	$(COMPOSE) up -d --wait

load:
	$(PYTHON) scripts/load_demo.py

deps:
	$(DBT) deps --profiles-dir .

seed:
	$(DBT) seed --profiles-dir .

run:
	$(DBT) run --profiles-dir .

test:
	$(DBT) test --profiles-dir .

unit-test:
	$(DBT) test --select test_type:unit --profiles-dir .

build:
	$(DBT) build --profiles-dir .

docs:
	$(DBT) docs generate --profiles-dir .

docs-serve:
	$(DBT) docs serve --profiles-dir . --port 8084

lint:
	sqlfluff lint models tests

format:
	sqlfluff fix models tests

check: deps seed run test lint docs

down:
	$(COMPOSE) down
