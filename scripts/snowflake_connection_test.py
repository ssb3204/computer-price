"""Snowflake 연결 테스트 — 접속, DDL 실행, 테이블 확인."""

import sys
from pathlib import Path

from dotenv import load_dotenv

# .env 로드
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection


def main() -> None:
    settings = SnowflakeSettings()
    print(f"Account : {settings.account}")
    print(f"User    : {settings.user}")
    print(f"Warehouse: {settings.warehouse}")
    print(f"Database : {settings.database}")
    print()

    # 1) 연결 테스트
    print("=== 1. 연결 테스트 ===")
    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()[0]
        print(f"Snowflake version: {version}")

        cur.execute("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        row = cur.fetchone()
        print(f"Warehouse: {row[0]}, Database: {row[1]}, Schema: {row[2]}")
        cur.close()
        print("연결 성공!\n")

    print("=== 완료 ===")


if __name__ == "__main__":
    main()
