#!/usr/bin/env bash
# Build pipeline image, update PIPELINE_TAG, render dagster.yaml, rolling-restart Dagster services.
# Usage:
#   ./scripts/deploy_dagster.sh              # build with git short SHA as tag, deploy, no smoke
#   ./scripts/deploy_dagster.sh --validate   # deploy + smoke materialize ingestion jobs
#   ./scripts/deploy_dagster.sh --rollback TAG=abc1234
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VALIDATE=false
ROLLBACK=false
ROLLBACK_TAG=""

for arg in "$@"; do
  case "$arg" in
    --validate) VALIDATE=true ;;
    --rollback) ROLLBACK=true ;;
    TAG=*) ROLLBACK_TAG="${arg#TAG=}" ;;
  esac
done

_update_env_pipeline_tag() {
  local tag="$1"
  if grep -q '^PIPELINE_TAG=' .env; then
    sed -i.bak "s/^PIPELINE_TAG=.*/PIPELINE_TAG=${tag}/" .env && rm -f .env.bak
  else
    echo "PIPELINE_TAG=${tag}" >> .env
  fi
}

_update_env_control_plane_defaults_if_needed() {
  # Earlier versions of this repo used CONTROL_PLANE_IMAGE=dagster/dagster which is not pullable.
  # If .env still has that value, move to the locally-built control-plane image.
  if grep -q '^CONTROL_PLANE_IMAGE=dagster/dagster$' .env 2>/dev/null; then
    sed -i.bak "s/^CONTROL_PLANE_IMAGE=.*/CONTROL_PLANE_IMAGE=crypto-belief-dagster-control-plane/" .env && rm -f .env.bak
  fi
  if ! grep -q '^CONTROL_PLANE_IMAGE=' .env 2>/dev/null; then
    echo "CONTROL_PLANE_IMAGE=crypto-belief-dagster-control-plane" >> .env
  fi
  if ! grep -q '^CONTROL_PLANE_TAG=' .env 2>/dev/null; then
    echo "CONTROL_PLANE_TAG=local" >> .env
  fi
}

if [[ "$ROLLBACK" == true ]]; then
  if [[ -z "$ROLLBACK_TAG" ]]; then
    echo "Usage: $0 --rollback TAG=<image_tag>" >&2
    exit 1
  fi
  if [[ ! -f .env ]]; then
    echo "Missing .env; copy from .env.example" >&2
    exit 1
  fi
  _update_env_pipeline_tag "$ROLLBACK_TAG"
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
  export PIPELINE_TAG="$ROLLBACK_TAG"
  ./scripts/render_dagster_yaml.sh
  docker compose up -d --no-deps dagster-user-code-crypto-belief
  docker compose up -d --no-deps dagster-daemon
  docker compose up -d --no-deps dagster-webserver
  echo "Rollback complete: PIPELINE_TAG=${ROLLBACK_TAG}"
  exit 0
fi

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example — review before production."
fi

_update_env_control_plane_defaults_if_needed

set -a
# shellcheck disable=SC1091
source .env
set +a

VERSION="$(python3 -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")"
VCS_REF="$(git rev-parse --short HEAD)"
BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
NEW_TAG="${DEPLOY_TAG:-$VCS_REF}"

export PIPELINE_IMAGE="${PIPELINE_IMAGE:-crypto-belief-pipeline}"
export PIPELINE_TAG="$NEW_TAG"

echo "Building ${PIPELINE_IMAGE}:${PIPELINE_TAG} ..."

docker compose build \
  --build-arg "BUILD_DATE=${BUILD_DATE}" \
  --build-arg "VCS_REF=${VCS_REF}" \
  --build-arg "VERSION=${VERSION}" \
  dagster-webserver dagster-daemon dagster-user-code-crypto-belief

_update_env_pipeline_tag "$NEW_TAG"
export PIPELINE_TAG="$NEW_TAG"
./scripts/render_dagster_yaml.sh

echo "Rolling restart: user-code -> daemon -> webserver ..."
docker compose up -d --no-deps dagster-user-code-crypto-belief
docker compose up -d --no-deps dagster-daemon
docker compose up -d --no-deps dagster-webserver

for i in $(seq 1 30); do
  if curl -fsS "http://localhost:3000/server_info" >/dev/null 2>&1; then
    echo "Dagster webserver healthy (server_info OK)."
    break
  fi
  if [[ "$i" -eq 30 ]]; then
    echo "Warning: webserver did not respond on /server_info within 30s" >&2
  fi
  sleep 1
done

if [[ "$VALIDATE" == true ]]; then
  PARTITION="${RUN_DATE:-$(date -u +%F)}"
  echo "Smoke validate: materializing ingestion jobs for partition ${PARTITION} (via pipeline image) ..."
  for job in raw_to_silver__binance__1m_job raw_to_silver__polymarket__5m_job raw_to_silver__gdelt__1h_job; do
    echo "  -> ${job}"
    docker compose run --rm \
      dagster-user-code-crypto-belief \
      dagster asset materialize \
      -m crypto_belief_pipeline.orchestration.definitions \
      --job "$job" \
      --partition "$PARTITION"
  done
  echo "Validate complete."
fi

echo "Deploy complete: ${PIPELINE_IMAGE}:${PIPELINE_TAG}"
