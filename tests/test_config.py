from crypto_belief_pipeline.config import Settings


def test_settings_load_from_env(monkeypatch) -> None:
    monkeypatch.setenv("ENV", "local")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("S3_BUCKET", "crypto-belief-lake")

    s = Settings()
    assert s.env == "local"
    assert s.aws_endpoint_url == "http://localhost:9000"
    assert s.aws_access_key_id == "minioadmin"
    assert s.aws_secret_access_key == "minioadmin"
    assert s.aws_region == "us-east-1"
    assert s.s3_bucket == "crypto-belief-lake"
