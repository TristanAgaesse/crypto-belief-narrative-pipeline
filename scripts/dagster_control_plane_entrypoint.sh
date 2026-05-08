#!/usr/bin/env bash
set -euo pipefail

# Render $DAGSTER_HOME/dagster.yaml from the repo template at container startup.
# This avoids committing rendered config that pins environment-specific image tags.

export DAGSTER_HOME_DIR="${DAGSTER_HOME:-/opt/dagster/dagster_home}"
export PIPELINE_IMAGE_NAME="${PIPELINE_IMAGE:-crypto-belief-pipeline}"
export PIPELINE_IMAGE_TAG="${PIPELINE_TAG:-local}"
export TEMPLATE_PATH="/opt/dagster/app/dagster/dagster.yaml.template"

mkdir -p "$DAGSTER_HOME_DIR"

python - <<'PY'
import os
from pathlib import Path

dagster_home = Path(os.environ["DAGSTER_HOME_DIR"])
template_path = Path(os.environ["TEMPLATE_PATH"])
pipeline_image = os.environ["PIPELINE_IMAGE_NAME"]
pipeline_tag = os.environ["PIPELINE_IMAGE_TAG"]

raw = template_path.read_text(encoding="utf-8")
out = raw.replace("${PIPELINE_IMAGE}", pipeline_image).replace("${PIPELINE_TAG}", pipeline_tag)
(dagster_home / "dagster.yaml").write_text(out, encoding="utf-8")
PY

exec "$@"

