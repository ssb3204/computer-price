"""Centralized configuration loaded from environment variables."""

from pydantic_settings import BaseSettings


class PostgresSettings(BaseSettings):
    model_config = {"env_prefix": "POSTGRES_"}

    user: str = "computer_price"
    password: str = "changeme"
    host: str = "postgres"
    port: int = 5432
    db: str = "computer_price"

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


class KafkaSettings(BaseSettings):
    model_config = {"env_prefix": "KAFKA_"}

    bootstrap_servers: str = "kafka:9092"


class SnowflakeSettings(BaseSettings):
    model_config = {"env_prefix": "SNOWFLAKE_"}

    account: str = ""
    user: str = ""
    password: str = ""
    warehouse: str = "COMPUTE_WH"
    database: str = "COMPUTER_PRICE"
    schema_name: str = "RAW"  # 'schema' is reserved by pydantic


class AppSettings(BaseSettings):
    postgres: PostgresSettings = PostgresSettings()
    kafka: KafkaSettings = KafkaSettings()
    snowflake: SnowflakeSettings = SnowflakeSettings()
    dash_debug: bool = False


def load_settings() -> AppSettings:
    return AppSettings()
