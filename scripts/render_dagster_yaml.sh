#!/usr/bin/env bash
# Render dagster/dagster.yaml from dagster/dagster.yaml.template using PIPELINE_IMAGE and PIPELINE_TAG.
# Compose does not substitute env vars inside bind-mounted YAML; this keeps run launcher in sync.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

export PIPELINE_IMAGE="${PIPELINE_IMAGE:-crypto-belief-pipeline}"
export PIPELINE_TAG="${PIPELINE_TAG:-local}"

python3 <<'PY'
import os
from pathlib import Path

root = Path.cwd()
template = (root / "dagster" / "dagster.yaml.template").read_text(encoding="utf-8")
out = template.replace("${PIPELINE_IMAGE}", os.environ["PIPELINE_IMAGE"]).replace(
    "${PIPELINE_TAG}", os.environ["PIPELINE_TAG"]
)
(root / "dagster" / "dagster.yaml").write_text(out, encoding="utf-8")
print(f"Rendered dagster/dagster.yaml -> {os.environ['PIPELINE_IMAGE']}:{os.environ['PIPELINE_TAG']}")
PY
