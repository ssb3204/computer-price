"""Snowflake DDL 실행 — 데이터베이스, 스키마, 테이블 생성."""

from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection

DDL_DIR = Path(__file__).resolve().parent.parent / "snowflake" / "ddl"


def execute_ddl_file(cursor, filepath: Path) -> None:
    print(f"\n--- {filepath.name} ---")
    sql_text = filepath.read_text(encoding="utf-8")

    # 세미콜론으로 분리, 빈 문장 제외
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    for stmt in statements:
        # 주석 줄 제거 후 실제 SQL만 추출
        sql_lines = [line for line in stmt.split("\n") if not line.strip().startswith("--")]
        sql_body = "\n".join(sql_lines).strip()
        if not sql_body:
            continue
        first_line = sql_body.split("\n")[0].strip()
        print(f"  실행: {first_line[:80]}...")
        cursor.execute(sql_body)
    print(f"  {filepath.name} 완료")


def main() -> None:
    settings = SnowflakeSettings()

    with get_connection(settings) as conn:
        cur = conn.cursor()

        # DDL 파일 순서대로 실행
        for ddl_file in sorted(DDL_DIR.glob("*.sql")):
            execute_ddl_file(cur, ddl_file)

        # 검증: 생성된 테이블 목록
        print("\n=== 생성된 테이블 확인 ===")
        for schema_name in ["RAW", "STAGING", "ANALYTICS"]:
            cur.execute(f"SHOW TABLES IN COMPUTER_PRICE.{schema_name}")
            tables = cur.fetchall()
            table_names = [row[1] for row in tables]
            print(f"  {schema_name}: {table_names}")

        cur.close()

    print("\nDDL 실행 완료!")


if __name__ == "__main__":
    main()
