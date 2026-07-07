"""search_products() 실제 동작 확인."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.crawlers.danawa import search_products

queries = ["RX 9070 XT", "RTX 5080", "라이젠 7800X3D"]

for query in queries:
    print(f"\n=== '{query}' 검색 결과 ===")
    results = search_products(query, max_results=5)
    if not results:
        print("  결과 없음")
    for r in results:
        print(f"  pcode={r.pcode}  {r.product_name}")
        print(f"    url: {r.url}")
