"""Step 1b: Danawa product detail page HTML reconnaissance.

Fetches individual product pages to analyze the price comparison table structure.
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

# Known pcodes for our target products
TARGETS = [
    {
        "pcode": "19627934",
        "filename": "product_7800x3d.html",
        "desc": "CPU: AMD Ryzen 7 7800X3D",
    },
]

PRODUCT_URL = "https://prod.danawa.com/info/"

TMP_DIR = Path(__file__).resolve().parent.parent / "tmp"


def fetch_and_save(pcode: str, filename: str, desc: str) -> bool:
    params = {"pcode": pcode}
    print(f"[{desc}] Fetching: {PRODUCT_URL}?pcode={pcode}")

    try:
        resp = requests.get(PRODUCT_URL, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ERROR: {e}")
        return False

    out_path = TMP_DIR / filename
    out_path.write_text(resp.text, encoding="utf-8")

    size_kb = out_path.stat().st_size / 1024
    print(f"  OK: {resp.status_code}, saved {size_kb:.1f} KB -> {out_path}")
    return size_kb > 10


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
