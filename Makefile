SHELL := /bin/bash
DATE ?= 2026-05-06

.PHONY: setup lint format test minio-up minio-down ensure-bucket check-config run-sample \
	smoke-test-apis fetch-live run-live build-gold dq detect-data-issues dagster-dev \
	dagster-materialize-sample generate-reports full-sample \
	docker-build build-control-plane docker-up docker-down dagster-up dagster-down dagster-ensure-bucket \
	deploy deploy-persist deploy-validate deploy-rollback

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

docker-build:
	docker compose build \
		--build-arg BUILD_DATE="$$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
		--build-arg VCS_REF="$$(git rev-parse --short HEAD)" \
		--build-arg VERSION="$$(python -c 'import tomllib;print(tomllib.load(open(\"pyproject.toml\",\"rb\"))[\"project\"][\"version\"])')" \
		dagster-webserver dagster-daemon dagster-user-code-crypto-belief

build-control-plane:
	docker compose build \
		--build-arg BUILD_DATE="$$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
		--build-arg VCS_REF="$$(git rev-parse --short HEAD)" \
		--build-arg VERSION="$$(python -c 'import tomllib;print(tomllib.load(open(\"pyproject.toml\",\"rb\"))[\"project\"][\"version\"])')" \
		dagster-webserver dagster-daemon

docker-up: docker-build
	docker compose up -d

docker-down:
	docker compose down

dagster-up: docker-up
	@echo "Dagster UI: http://localhost:3000"

dagster-down: docker-down

deploy:
	@chmod +x scripts/deploy_dagster.sh
	@./scripts/deploy_dagster.sh

deploy-persist:
	@chmod +x scripts/deploy_dagster.sh
	@./scripts/deploy_dagster.sh --persist-tag

deploy-validate:
	@chmod +x scripts/deploy_dagster.sh
	@./scripts/deploy_dagster.sh --validate

# Usage: make deploy-rollback TAG=<previous_pipeline_tag>
deploy-rollback:
	@test -n "$(TAG)" || (echo "Usage: make deploy-rollback TAG=<previous_pipeline_image_tag>"; exit 1)
	@chmod +x scripts/deploy_dagster.sh
	@./scripts/deploy_dagster.sh --rollback TAG=$(TAG)

dagster-ensure-bucket:
	docker compose exec -T dagster-daemon python -m crypto_belief_pipeline.cli ensure-bucket

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

dq:
	. .venv/bin/activate && python -m crypto_belief_pipeline.cli dq run --date $${RUN_DATE:-$(DATE)}

detect-data-issues:
	. .venv/bin/activate && python -m crypto_belief_pipeline.cli issues detect --date $${RUN_DATE:-$(DATE)}

dagster-dev:
	. .venv/bin/activate && dagster dev -m crypto_belief_pipeline.orchestration.definitions

dagster-materialize-sample:
	. .venv/bin/activate && python -m crypto_belief_pipeline.cli pipeline run --date $${RUN_DATE:-$(DATE)} --mode sample

generate-reports:
	. .venv/bin/activate && python -m crypto_belief_pipeline.cli generate-reports --date $${RUN_DATE:-$(DATE)}

full-sample:
	$(MAKE) ensure-bucket
	$(MAKE) run-sample
	$(MAKE) build-gold
	$(MAKE) dq
	$(MAKE) detect-data-issues
	$(MAKE) generate-reports
