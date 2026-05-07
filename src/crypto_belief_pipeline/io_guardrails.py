"""Bucket isolation guardrails for sample/live I/O.

Sample isolation is bucket-based: sample writes/reads go to a dedicated
``sample_lake_bucket`` configured separately from the live ``S3_BUCKET``.

This module is the single source of truth for that policy so CLI, sample
runner, and Dagster helpers cannot drift in their checks.
"""

from __future__ import annotations

from crypto_belief_pipeline.config import get_runtime_config, get_settings


class SampleConfigError(ValueError):
    """Raised when sample I/O configuration is missing or unsafe.

    Subclasses ``ValueError`` so existing test patterns that assert
    ``ValueError`` keep working; CLI callers can catch this specifically and
    re-raise as ``typer.BadParameter`` for nicer operator output.
    """


def resolve_sample_bucket() -> str:
    """Validate sample I/O config and return the dedicated sample bucket.

    Sample writes/reads all use canonical ``raw/...``, ``silver/...``,
    ``gold/...`` keys; isolation lives at the bucket layer. The sample bucket
    must:

    - exist in ``config/runtime.yaml`` under ``sample.sample_lake_bucket``,
    - have ``sample.sample_enabled=true``,
    - be **distinct** from the live ``S3_BUCKET`` so a misconfigured run
      cannot accidentally land in production data.
    """

    rt = get_runtime_config()
    if not rt.sample.sample_enabled:
        raise SampleConfigError(
            "Sample I/O is disabled. Enable via config/runtime.yaml: sample.sample_enabled=true"
        )
    bucket = (rt.sample.sample_lake_bucket or "").strip()
    if not bucket:
        raise SampleConfigError(
            "sample.sample_lake_bucket must be set (and distinct from the live bucket) "
            "when sample I/O is enabled"
        )
    live_bucket = get_settings().s3_bucket
    if bucket == live_bucket:
        raise SampleConfigError(
            "sample.sample_lake_bucket must differ from the live S3_BUCKET to keep sample I/O "
            "isolated from production data."
        )
    return bucket


__all__ = ["SampleConfigError", "resolve_sample_bucket"]
