SHELL := /bin/bash
DATE ?= 2026-05-06

.PHONY: setup lint format test minio-up minio-down ensure-bucket check-config run-sample \
	smoke-test-apis fetch-live run-live build-gold

setup:
	python3.11 -m venv .venv
	. .venv/bin/activate && python -m pip install --upgrade pip
	. .venv/bin/activate && pip install -e ".[dev]"

lint:
	. .venv/bin/activate && ruff check .
	. .venv/bin/activate && ruff format --check .

format:
	. .venv/bin/activate && ruff format .

test:
	. .venv/bin/activate && pytest -q

minio-up:
	docker compose up -d

minio-down:
	docker compose down

ensure-bucket:
	. .venv/bin/activate && python -m crypto_belief_pipeline.cli ensure-bucket

check-config:
	. .venv/bin/activate && python -m crypto_belief_pipeline.cli check-config

run-sample: ensure-bucket
	. .venv/bin/activate && python -m crypto_belief_pipeline.cli run-sample --date $(DATE)

smoke-test-apis:
	. .venv/bin/activate && python -m crypto_belief_pipeline.cli smoke-test-apis

fetch-live:
	. .venv/bin/activate && python -m crypto_belief_pipeline.cli fetch-live --date $${RUN_DATE:-$(DATE)}

run-live: ensure-bucket
	. .venv/bin/activate && python -m crypto_belief_pipeline.cli run-live --date $${RUN_DATE:-$(DATE)}

build-gold:
	. .venv/bin/activate && python -m crypto_belief_pipeline.cli build-gold --date $${RUN_DATE:-$(DATE)}
