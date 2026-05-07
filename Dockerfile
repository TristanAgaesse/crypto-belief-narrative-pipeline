FROM python:3.11-slim

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

LABEL org.opencontainers.image.title="crypto-belief-narrative-pipeline" \
      org.opencontainers.image.description="Dagster user-code image for the crypto belief + narrative pipeline (collectors, transforms, DQ, features)." \
      org.opencontainers.image.source="crypto-belief-narrative-pipeline" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.created=$BUILD_DATE \
      org.opencontainers.image.revision=$VCS_REF \
      org.opencontainers.image.version=$VERSION

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/dagster/app

COPY pyproject.toml README.md ./
COPY src ./src
COPY dq ./dq
COPY config ./config

RUN python -m pip install --upgrade pip \
  && pip install .

ENV DAGSTER_HOME=/opt/dagster/dagster_home

EXPOSE 4000

CMD ["dagster", "api", "grpc", "-m", "crypto_belief_pipeline.orchestration.definitions", "-h", "0.0.0.0", "-p", "4000"]
