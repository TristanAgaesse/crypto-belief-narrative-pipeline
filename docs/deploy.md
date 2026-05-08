# Dagster Docker deploy

This repo separates **control plane** (Dagster webserver + daemon) from **user code + run workers** (pipeline image built from the repo `Dockerfile`).

## Environment variables

Set in `.env` (copy from [`.env.example`](../.env.example)):

| Variable | Role |
|----------|------|
| `CONTROL_PLANE_IMAGE` | Image for `dagster-webserver` and `dagster-daemon` (default: `crypto-belief-dagster-control-plane`, built locally). |
| `CONTROL_PLANE_TAG` | Control-plane image tag (default: `local`). |
| `PIPELINE_IMAGE` | Name for the built pipeline image (default: `crypto-belief-pipeline`). |
| `PIPELINE_TAG` | Immutable tag for each release (default: `local`; deploy script sets this to the git short SHA unless overridden). |

`dagster/dagster.yaml` is **generated** from [`dagster/dagster.yaml.template`](../dagster/dagster.yaml.template) so `DockerRunLauncher` uses the same `PIPELINE_IMAGE:PIPELINE_TAG` as the user-code container. Docker Compose does not substitute variables inside bind-mounted YAML; run [`scripts/render_dagster_yaml.sh`](../scripts/render_dagster_yaml.sh) after changing image tags.

If you previously had `CONTROL_PLANE_IMAGE=dagster/dagster` in your `.env`, update it to `crypto-belief-dagster-control-plane` (or just re-copy `.env.example`). The deploy script also auto-corrects this legacy value.

## Commands

| Goal | Command |
|------|---------|
| Render `dagster.yaml` only | `make dagster-render-config` |
| Build pipeline image + start stack | `make docker-up` or `make dagster-up` |
| Build, tag with git SHA, rolling restart | `make deploy` |
| Deploy + smoke three ingestion jobs | `make deploy-validate` |
| Roll back pipeline tag | `make deploy-rollback TAG=<previous_tag>` |

Deploy script: [`scripts/deploy_dagster.sh`](../scripts/deploy_dagster.sh)

Optional: set `DEPLOY_TAG=mytag` when calling the script to force a tag instead of the git short SHA.

## Rollout sequence

1. `make deploy` (or `./scripts/deploy_dagster.sh`)
2. Script builds `dagster-user-code-crypto-belief`, updates `PIPELINE_TAG` in `.env`, renders `dagster.yaml`, then restarts **user-code → daemon → webserver**.
3. After deploy, confirm in Dagster UI that the code location loads and schedules are healthy.

## Ingestion / no-gap notes

- Cursors and collection windows recover after short outages; after any deploy, optionally run `make deploy-validate` to immediately materialize `raw_to_silver__*` jobs for today’s partition (`RUN_DATE` overrides partition date).
- If live APIs are unavailable, `deploy-validate` will fail — run `make deploy` without validate in that case.

## Rollback

```bash
make deploy-rollback TAG=<previous_git_sha_or_tag>
```

Then confirm runs in the UI and re-enable schedules if needed.

## Control plane vs pipeline version

Keep the **Dagster package versions** installed in the control-plane image aligned with the pipeline image (same minor is ideal). Check with:

```bash
. .venv/bin/activate && pip show dagster | grep Version
```

If you update Dagster dependencies in `pyproject.toml`, rebuild both images (`make docker-up` or `make deploy`) so control plane and user code stay in sync.
