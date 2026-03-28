"""Centralized configuration loaded from environment variables."""

from pydantic_settings import BaseSettings


class SnowflakeSettings(BaseSettings):
    model_config = {"env_prefix": "SNOWFLAKE_"}

    account: str
    user: str
    password: str
    warehouse: str = "COMPUTE_WH"
    database: str = "COMPUTER_PRICE"
    schema_name: str = "RAW"  # 'schema' is reserved by pydantic


class SlackSettings(BaseSettings):
    model_config = {"env_prefix": "SLACK_"}

    webhook_url: str = ""


class AppSettings(BaseSettings):
    snowflake: SnowflakeSettings = SnowflakeSettings()
    slack: SlackSettings = SlackSettings()
    dash_debug: bool = False


def load_settings() -> AppSettings:
    return AppSettings()
