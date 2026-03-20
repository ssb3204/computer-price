"""Step 1c: Danawa category listing page HTML reconnaissance.

Fetches category pages for RAM and SSD to analyze ranking/listing structure.
"""

import sys
import time
from pathlib import Path

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Danawa category codes (from site navigation)
# RAM: DDR5 16GB category
# SSD: NVMe 1TB category
TARGETS = [
    {
        "url": "https://prod.danawa.com/list/?cate=112752&15main_11_02",
        "filename": "category_ram.html",
        "desc": "RAM: DDR5 category",
    },
    {
        "url": "https://prod.danawa.com/list/?cate=112760&15main_11_02",
        "filename": "category_ssd.html",
        "desc": "SSD: NVMe category",
    },
]

TMP_DIR = Path(__file__).resolve().parent.parent / "tmp"


def fetch_and_save(url: str, filename: str, desc: str) -> bool:
    print(f"[{desc}] Fetching: {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ERROR: {e}")
        return False

    out_path = TMP_DIR / filename
    out_path.write_text(resp.text, encoding="utf-8")

    size_kb = out_path.stat().st_size / 1024
    print(f"  OK: {resp.status_code}, saved {size_kb:.1f} KB -> {out_path}")
    return size_kb > 5


def main() -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    results = []
    for target in TARGETS:
        ok = fetch_and_save(**target)
        results.append((target["desc"], ok))
        time.sleep(2)

    print("\n=== Results ===")
    for desc, ok in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {desc}")

    if not all(ok for _, ok in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
