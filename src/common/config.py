"""Centralized configuration loaded from environment variables."""

from pydantic_settings import BaseSettings


class SnowflakeSettings(BaseSettings):
    model_config = {"env_prefix": "SNOWFLAKE_", "env_file": ".env", "extra": "ignore"}

    account: str
    user: str
    password: str
    warehouse: str = "COMPUTE_WH"
    database: str = "COMPUTER_PRICE"
    schema_name: str = "RAW"  # 'schema' is reserved by pydantic


class MySQLSettings(BaseSettings):
    """로컬 MySQL 8.0+ 연결 설정.

    3-Layer(RAW/STAGING/ANALYTICS)는 단일 데이터베이스(computer_price) 안에서
    테이블 접두사(raw_/stg_/ans_)로 구분하므로, 별도 스키마 수식 없이
    기본 database(computer_price)에 그대로 접속한다.

    접속 계정은 price_app만 사용한다(root 금지). 값은 .env에서 주입한다.
    """

    model_config = {"env_prefix": "MYSQL_", "env_file": ".env", "extra": "ignore"}

    host: str = "localhost"
    port: int = 3306
    user: str = "price_app"
    password: str
    database: str = "computer_price"
