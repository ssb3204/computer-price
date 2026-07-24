"""Watch list CRUD 함수 실제 DB 동작 확인 (수동 실행용, pytest 대상 아님)."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from src.common.config import MySQLSettings
from src.common.mysql_client import get_connection
from src.dashboard.data_access.mysql_queries import (
    add_watch_product,
    get_watch_products,
    remove_watch_product,
)

settings = MySQLSettings()

with get_connection(settings) as conn:
    # 1. 현재 목록 확인
    print("=== [1] 현재 Watch 목록 ===")
    df = get_watch_products(conn)
    print(df.to_string(index=False) if not df.empty else "  (비어있음)")

    # 2. 테스트 상품 추가
    TEST_PCODE = "TEST_PCODE_9999"
    print("\n=== [2] 테스트 상품 추가 ===")
    add_watch_product(
        conn,
        query="RTX 5090",
        pcode=TEST_PCODE,
        product_name="[테스트] ASUS ROG RTX 5090",
        category="GPU",
        brand="NVIDIA",
    )
    print("  추가 완료")

    # 3. 추가 후 목록 확인
    print("\n=== [3] 추가 후 Watch 목록 ===")
    df = get_watch_products(conn)
    print(df.to_string(index=False) if not df.empty else "  (비어있음)")

    # 4. 방금 추가한 항목 ID 확인 후 삭제
    test_row = df[df["pcode"] == TEST_PCODE]
    if test_row.empty:
        print("\n  ERROR: 추가된 항목을 찾을 수 없음")
        sys.exit(1)

    watch_id = int(test_row.iloc[0]["id"])
    print(f"\n=== [4] ID={watch_id} 항목 삭제 ===")
    remove_watch_product(conn, watch_id)
    print("  삭제(비활성화) 완료")

    # 5. 삭제 후 목록 확인
    print("\n=== [5] 삭제 후 Watch 목록 ===")
    df = get_watch_products(conn)
    print(df.to_string(index=False) if not df.empty else "  (비어있음)")

    assert TEST_PCODE not in df["pcode"].values, "삭제 후에도 항목이 남아있음"
    print("\n모든 CRUD 동작 정상 확인.")
