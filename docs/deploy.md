# Deploy (Docker Compose)

```bash
cp .env.example .env   # first time
make dagster-up
```

| Make target | Effect |
|-------------|--------|
| `make deploy` | Build/push user-code image; `PIPELINE_TAG` for this run only (default). |
| `make deploy-persist` | Same + write `PIPELINE_TAG` to `.env`. |
| `make deploy-validate` | Deploy + smoke three `raw_to_silver__*` jobs. |
| `make build-control-plane && make deploy` | After Dagster stack version bumps in deps. |
| `make deploy-rollback TAG=…` | Roll back image tag. |

**Contract:** Jobs/assets ship in **`dagster-user-code-crypto-belief`**. `dagster/dagster.yaml` is rendered **inside** the container from `dagster/dagster.yaml.template` at startup (gitignored on host).

**Script:** [`scripts/deploy_dagster.sh`](../scripts/deploy_dagster.sh) — `--persist-tag`, `--validate`, `--rebuild-control-plane`, `--rollback TAG=…`.

**Troubles:** Dagster version change → rebuild control plane **and** deploy user code. `CONTROL_PLANE_IMAGE` should be `crypto-belief-dagster-control-plane` (script can rewrite legacy `dagster/dagster`).
