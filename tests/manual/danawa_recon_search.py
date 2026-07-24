"""Step 1: Danawa search page HTML reconnaissance.

Fetches search result pages from Danawa and saves raw HTML for offline analysis.
No parsing — just download and save.
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

TARGETS = [
    {
        "query": "라이젠 7800X3D",
        "filename": "search_7800x3d.html",
        "desc": "CPU: AMD Ryzen 7 7800X3D",
    },
    {
        "query": "RTX 5070",
        "filename": "search_rtx5070.html",
        "desc": "GPU: RTX 5070",
    },
    {
        "query": "RTX 5070 Ti",
        "filename": "search_rtx5070ti.html",
        "desc": "GPU: RTX 5070 Ti",
    },
    {
        "query": "RX 9070 XT",
        "filename": "search_rx9070xt.html",
        "desc": "GPU: RX 9070 XT",
    },
]

SEARCH_URL = "https://search.danawa.com/dsearch.php"

TMP_DIR = Path(__file__).resolve().parent.parent / "tmp"


def fetch_and_save(query: str, filename: str, desc: str) -> bool:
    """Fetch a Danawa search page and save HTML to tmp/."""
    params = {"query": query, "tab": "goods"}
    print(f"[{desc}] Fetching: {SEARCH_URL}?query={query}&tab=goods")

    try:
        resp = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ERROR: {e}")
        return False

    out_path = TMP_DIR / filename
    out_path.write_text(resp.text, encoding="utf-8")

    size_kb = out_path.stat().st_size / 1024
    print(f"  OK: {resp.status_code}, saved {size_kb:.1f} KB -> {out_path}")
    return size_kb > 10  # expect at least 10 KB for a real page


def main() -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {TMP_DIR}\n")

    results = []
    for target in TARGETS:
        ok = fetch_and_save(**target)
        results.append((target["desc"], ok))
        time.sleep(2)  # rate limiting

    print("\n=== Results ===")
    all_ok = True
    for desc, ok in results:
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {desc}")
        if not ok:
            all_ok = False

    if not all_ok:
        print("\nSome fetches failed. Check errors above.")
        sys.exit(1)
    else:
        print("\nAll pages fetched successfully. Ready for Step 2 analysis.")


if __name__ == "__main__":
    main()
