"""Snowflake QUERY_HISTORY에서 transform_staging 실행의 실제 BYTES_SCANNED를 측정.

사용법:
    python scripts/benchmark_stream_scan.py          # 최근 14일 실행 분석
    python scripts/benchmark_stream_scan.py --days 7 # 최근 7일
    python scripts/benchmark_stream_scan.py --limit 100
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection


def fetch_scan_stats(settings: SnowflakeSettings, days: int, limit: int) -> list[dict]:
    """QUERY_HISTORY에서 CRAWLED_PRICES_STREAM을 읽는 쿼리의 스캔 통계를 가져온다."""
    with get_connection(settings) as conn:
        cur = conn.cursor()
        # CRAWLED_PRICES_STREAM을 참조하는 CTAS만 필터 (transform_staging 해당)
        cur.execute(f"""
            SELECT
                START_TIME,
                QUERY_TEXT,
                BYTES_SCANNED,
                ROWS_PRODUCED,
                TOTAL_ELAPSED_TIME,
                QUERY_ID
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                DATEADD('day', -{days}, CURRENT_TIMESTAMP()),
                CURRENT_TIMESTAMP()
            ))
            WHERE QUERY_TEXT ILIKE '%CRAWLED_PRICES_STREAM%'
              AND QUERY_TYPE = 'CREATE_TABLE_AS_SELECT'
              AND EXECUTION_STATUS = 'SUCCESS'
            ORDER BY START_TIME DESC
            LIMIT {limit}
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()
    return [dict(zip(cols, row)) for row in rows]


def print_report(records: list[dict]) -> None:
    if not records:
        print("측정된 쿼리 없음. 파이프라인 실행 후 다시 시도하세요.")
        return

    bytes_list = [r["BYTES_SCANNED"] or 0 for r in records]
    rows_list  = [r["ROWS_PRODUCED"] or 0 for r in records]
    elapsed    = [r["TOTAL_ELAPSED_TIME"] or 0 for r in records]

    print(f"\n{'='*60}")
    print(f"  Stream Scan Benchmark  (n={len(records)}회 실행)")
    print(f"{'='*60}")
    print(f"  BYTES_SCANNED")
    print(f"    평균:   {sum(bytes_list)/len(bytes_list):>12,.0f} bytes")
    print(f"    최소:   {min(bytes_list):>12,} bytes")
    print(f"    최대:   {max(bytes_list):>12,} bytes")
    print(f"    합계:   {sum(bytes_list):>12,} bytes")
    print(f"  ROWS_PRODUCED (신규 처리 건수)")
    print(f"    평균:   {sum(rows_list)/len(rows_list):>12,.1f} 건")
    print(f"    최소:   {min(rows_list):>12,} 건")
    print(f"    최대:   {max(rows_list):>12,} 건")
    print(f"  TOTAL_ELAPSED_TIME")
    print(f"    평균:   {sum(elapsed)/len(elapsed):>12,.0f} ms")
    print(f"{'='*60}")

    print(f"\n최근 10회 상세:")
    print(f"  {'START_TIME':<22} {'BYTES_SCANNED':>14} {'ROWS':>6} {'MS':>8}")
    print(f"  {'-'*54}")
    for r in records[:10]:
        print(
            f"  {str(r['START_TIME']):<22} "
            f"{(r['BYTES_SCANNED'] or 0):>14,} "
            f"{(r['ROWS_PRODUCED'] or 0):>6,} "
            f"{(r['TOTAL_ELAPSED_TIME'] or 0):>8,.0f}"
        )

    # bytes-per-row: 스캔 효율 지표
    valid = [(b, r) for b, r in zip(bytes_list, rows_list) if r > 0]
    if valid:
        bpr = [b / r for b, r in valid]
        print(f"\n  bytes/row (스캔 효율): 평균 {sum(bpr)/len(bpr):,.0f}")
        print(f"  → 값이 작을수록 신규 데이터만 효율적으로 처리 중")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream scan benchmark")
    parser.add_argument("--days",  type=int, default=6, help="조회 기간(일, 최대 6, 기본 6)")
    parser.add_argument("--limit", type=int, default=50,  help="최대 조회 건수(기본 50)")
    args = parser.parse_args()

    settings = SnowflakeSettings()

    print(f"Snowflake QUERY_HISTORY 조회 중 (최근 {args.days}일, 최대 {args.limit}건)...")
    records = fetch_scan_stats(settings, args.days, args.limit)
    print_report(records)


if __name__ == "__main__":
    main()
