# Dagster deploy (Docker Compose)

## what you run

```bash
# First time
cp .env.example .env
make dagster-up
```

```bash
# Deploy new assets/jobs / pipeline code (build user-code only, tag=git SHA)
make deploy

# Same, but also write PIPELINE_TAG into .env
make deploy-persist

# Deploy + smoke-run three ingestion jobs (optional)
make deploy-validate

# Rebuild control plane (only when Dagster/control-plane deps change)
make build-control-plane && make deploy

# Roll back pipeline image tag
make deploy-rollback TAG=<previous_tag>
```

## What “deploy” means here

- **Assets/jobs live in the user-code image** (`dagster-user-code-crypto-belief`). Deploying new assets/jobs is deploying a new pipeline image.
- **`dagster/dagster.yaml` is rendered at container startup** from `dagster/dagster.yaml.template` (via `dagster-control-plane-entrypoint`). There is **no** host-side render step and the rendered file is gitignored.
- `make deploy` is **stateless by default**: it passes `PIPELINE_TAG`/`PIPELINE_IMAGE` to Compose for that invocation, but does not mutate `.env`. Use `make deploy-persist` if you want `.env` pinned.

## Script interface (optional)

Everything above is a wrapper around [`scripts/deploy_dagster.sh`](../scripts/deploy_dagster.sh):

- **`--persist-tag`**: write the computed `PIPELINE_TAG` to `.env`
- **`--validate`**: run smoke `dagster job execute` for three `raw_to_silver__*` jobs (partition = `RUN_DATE` or today UTC)
- **`--rebuild-control-plane`**: also build `dagster-webserver` and `dagster-daemon`
- **`--rollback TAG=<tag>`**: restart with a specific tag (add `--persist-tag` to also update `.env`)

## Notes / troubleshooting

- If you changed Dagster versions in `pyproject.toml`, rebuild **both**: `make build-control-plane && make deploy`.
- If your `.env` still has `CONTROL_PLANE_IMAGE=dagster/dagster`, update it to `crypto-belief-dagster-control-plane` (the deploy script also auto-corrects this when it loads `.env`).
