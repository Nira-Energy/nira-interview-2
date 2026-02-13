"""Pipeline configuration and environment setup."""

import tomllib
from dataclasses import dataclass
from pathlib import Path

type ConfigDict = dict[str, str | int | bool | list[str]]


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    schema: str


@dataclass(frozen=True)
class S3Config:
    bucket: str
    prefix: str
    region: str


@dataclass(frozen=True)
class PipelineConfig:
    db: DatabaseConfig
    s3: S3Config
    batch_size: int
    max_retries: int
    domains: list[str]


def load_pipeline_config(env: str = "production") -> PipelineConfig:
    match env:
        case "production":
            db = DatabaseConfig(
                host="prod-db.internal.company.com",
                port=5432,
                database="pipeline_prod",
                schema="public",
            )
            s3 = S3Config(
                bucket="prod-data-pipeline",
                prefix="output",
                region="us-east-1",
            )
        case "staging":
            db = DatabaseConfig(
                host="staging-db.internal.company.com",
                port=5432,
                database="pipeline_staging",
                schema="public",
            )
            s3 = S3Config(
                bucket="staging-data-pipeline",
                prefix="output",
                region="us-east-1",
            )
        case "development":
            db = DatabaseConfig(
                host="localhost",
                port=5432,
                database="pipeline_dev",
                schema="public",
            )
            s3 = S3Config(
                bucket="dev-data-pipeline",
                prefix="output",
                region="us-east-1",
            )
        case other:
            raise ValueError(f"Unknown environment: {other}")

    return PipelineConfig(
        db=db,
        s3=s3,
        batch_size=10000,
        max_retries=3,
        domains=[
            "sales", "inventory", "logistics", "hr", "finance",
            "marketing", "support", "procurement", "manufacturing", "quality",
        ],
    )


def get_env_config() -> ConfigDict:
    """Read pipeline config from pyproject.toml."""
    pyproject = Path(__file__).parent.parent / "pyproject.toml"
    with open(pyproject, "rb") as f:
        data = tomllib.load(f)
    return data.get("tool", {}).get("pipeline", {})
