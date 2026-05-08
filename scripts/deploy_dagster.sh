#!/usr/bin/env bash
# Build pipeline image, rolling-restart Dagster services. Dagster instance YAML is rendered at
# container startup (see dagster-control-plane-entrypoint); no host-side render.
#
# Usage:
#   ./scripts/deploy_dagster.sh                    # build user-code only, tag=git SHA, no .env write
#   ./scripts/deploy_dagster.sh --validate         # + smoke ingestion jobs
#   ./scripts/deploy_dagster.sh --persist-tag    # also write PIPELINE_TAG to .env
#   ./scripts/deploy_dagster.sh --rebuild-control-plane
#   ./scripts/deploy_dagster.sh --rollback TAG=abc1234
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VALIDATE=false
ROLLBACK=false
ROLLBACK_TAG=""
PERSIST_TAG=false
REBUILD_CONTROL_PLANE=false

for arg in "$@"; do
  case "$arg" in
    --validate) VALIDATE=true ;;
    --rollback) ROLLBACK=true ;;
    --persist-tag) PERSIST_TAG=true ;;
    --rebuild-control-plane) REBUILD_CONTROL_PLANE=true ;;
    TAG=*) ROLLBACK_TAG="${arg#TAG=}" ;;
  esac
done

_update_env_pipeline_tag() {
  local tag="$1"
  if [[ ! -f .env ]]; then
    echo "PIPELINE_TAG=${tag}" >>.env
    return
  fi
  if grep -q '^PIPELINE_TAG=' .env; then
    sed -i.bak "s/^PIPELINE_TAG=.*/PIPELINE_TAG=${tag}/" .env && rm -f .env.bak
  else
    echo "PIPELINE_TAG=${tag}" >>.env
  fi
}

_fix_legacy_control_plane_image_in_env() {
  [[ -f .env ]] || return 0
  if grep -q '^CONTROL_PLANE_IMAGE=dagster/dagster$' .env 2>/dev/null; then
    sed -i.bak "s/^CONTROL_PLANE_IMAGE=.*/CONTROL_PLANE_IMAGE=crypto-belief-dagster-control-plane/" .env && rm -f .env.bak
    echo "Updated legacy CONTROL_PLANE_IMAGE in .env"
  fi
}

_compose() {
  # shellcheck disable=SC2068
  docker compose "$@"
}

_validate_ingestion_job() {
  local job="$1"
  local partition="$2"
  echo "  -> ${job} partition=${partition}"
  _compose run --rm \
    -e "PIPELINE_TAG=${PIPELINE_TAG}" \
    -e "PIPELINE_IMAGE=${PIPELINE_IMAGE}" \
    -e "JOB_NAME=${job}" \
    -e "PARTITION_TAG=${partition}" \
    dagster-user-code-crypto-belief \
    bash -lc 'set -euo pipefail
      rm -rf /tmp/dagster_smoke_home
      mkdir -p /tmp/dagster_smoke_home
      export DAGSTER_HOME=/tmp/dagster_smoke_home
      printf "telemetry:\n  enabled: false\n" >"$DAGSTER_HOME/dagster.yaml"
      dagster job execute -m crypto_belief_pipeline.orchestration.definitions \
        -j "${JOB_NAME}" \
        --tags "{\"dagster/partition\":\"${PARTITION_TAG}\"}"'
}

_validation_run_date() {
  echo "${RUN_DATE:-$(date -u +%F)}"
}

_validation_hour() {
  echo "${RUN_HOUR:-$(date -u +%H)}"
}

_validation_minute() {
  echo "${RUN_MINUTE:-$(date -u +%M)}"
}

_validation_hourly_partition() {
  printf "%s-%s:00" "$(_validation_run_date)" "$(_validation_hour)"
}

_validation_minute_partition() {
  printf "%sT%s:%s" "$(_validation_run_date)" "$(_validation_hour)" "$(_validation_minute)"
}

if [[ "$ROLLBACK" == true ]]; then
  if [[ -z "${ROLLBACK_TAG:-}" ]]; then
    echo "Usage: $0 --rollback TAG=<image_tag>" >&2
    exit 1
  fi
  if [[ "$PERSIST_TAG" == true ]]; then
    [[ -f .env ]] || cp .env.example .env
    _update_env_pipeline_tag "$ROLLBACK_TAG"
  fi
  export PIPELINE_TAG="$ROLLBACK_TAG"
  _compose up -d --no-deps dagster-user-code-crypto-belief
  _compose up -d --no-deps dagster-daemon
  _compose up -d --no-deps dagster-webserver
  echo "Rollback complete: PIPELINE_TAG=${ROLLBACK_TAG} (compose uses this for the current shell only unless --persist-tag)"
  exit 0
fi

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example — review before production."
fi

_fix_legacy_control_plane_image_in_env

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

BUILD_SERVICES=(dagster-user-code-crypto-belief)
if [[ "$REBUILD_CONTROL_PLANE" == true ]]; then
  BUILD_SERVICES=(dagster-webserver dagster-daemon dagster-user-code-crypto-belief)
fi

echo "Building ${PIPELINE_IMAGE}:${PIPELINE_TAG} (${BUILD_SERVICES[*]}) ..."

_compose build \
  --build-arg "BUILD_DATE=${BUILD_DATE}" \
  --build-arg "VCS_REF=${VCS_REF}" \
  --build-arg "VERSION=${VERSION}" \
  "${BUILD_SERVICES[@]}"

if [[ "$PERSIST_TAG" == true ]]; then
  _update_env_pipeline_tag "$NEW_TAG"
fi

echo "Rolling restart: user-code -> daemon -> webserver ..."
_compose up -d --no-deps dagster-user-code-crypto-belief
_compose up -d --no-deps dagster-daemon
_compose up -d --no-deps dagster-webserver

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
  MINUTE_PARTITION="$(_validation_minute_partition)"
  HOURLY_PARTITION="$(_validation_hourly_partition)"
  echo "Smoke validate: executing minute staging jobs for partition ${MINUTE_PARTITION} ..."
  for job in raw_staging__binance__1m_job raw_staging__polymarket__5m_job raw_staging__gdelt__1h_job; do
    _validate_ingestion_job "$job" "$MINUTE_PARTITION"
  done
  echo "Smoke validate: executing hourly canonical jobs for partition ${HOURLY_PARTITION} ..."
  for job in raw_to_silver__binance__1m_job raw_to_silver__polymarket__5m_job raw_to_silver__gdelt__1h_job; do
    _validate_ingestion_job "$job" "$HOURLY_PARTITION"
  done
  echo "Validate complete."
fi

echo "Deploy complete: ${PIPELINE_IMAGE}:${PIPELINE_TAG}"
if [[ "$PERSIST_TAG" != true ]]; then
  echo "Note: PIPELINE_TAG was not written to .env. Next compose without env override uses .env value."
  echo "      Re-run with --persist-tag or: PIPELINE_TAG=${NEW_TAG} docker compose up -d"
fi
