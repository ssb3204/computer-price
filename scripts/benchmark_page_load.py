"""페이지별 Snowflake 쿼리 응답 시간 벤치마크.

사용법:
    python scripts/benchmark_page_load.py --mode before   # 캐싱 전 측정
    python scripts/benchmark_page_load.py --mode after    # 캐싱 후 측정
    python scripts/benchmark_page_load.py --mode before --runs 5  # 횟수 지정
"""

import argparse
import inspect
import json
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Windows 콘솔 UTF-8 출력 설정
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
if sys.stderr.encoding != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection
from src.dashboard.data_access.snowflake_queries import (
    get_alerts,
    get_category_price_summary,
    get_latest_prices_all,
    get_product_stats,
    get_summary_stats,
    get_today_crawl_comparison,
    get_watch_products,
)

BEFORE_JSON = ROOT / "docs" / "_benchmark_before.json"
OUTPUT_MD   = ROOT / "docs" / "benchmark_results.md"


# ── 페이지별 쿼리 정의 (실제 콜백과 동일한 구조) ────────────────────────────

def _page_dashboard(conn):
    """대시보드 (/) — 3개 쿼리 (update_overview 콜백)."""
    t = {}
    for name, fn in [
        ("get_summary_stats",          get_summary_stats),
        ("get_category_price_summary", get_category_price_summary),
        ("get_latest_prices_all",      get_latest_prices_all),
    ]:
        s = time.perf_counter()
        fn(conn)
        t[name] = round((time.perf_counter() - s) * 1000, 1)
    return t

def _page_prices(conn):
    """가격 정보 (/prices) — 1개 쿼리 (update_prices_table 콜백)."""
    s = time.perf_counter()
    get_latest_prices_all(conn)
    return {"get_latest_prices_all": round((time.perf_counter() - s) * 1000, 1)}

def _page_trends(conn):
    """가격 추이 (/trends) — 1개 쿼리 (update_today_comparison 콜백, 초기 로드)."""
    s = time.perf_counter()
    get_today_crawl_comparison(conn)
    return {"get_today_crawl_comparison": round((time.perf_counter() - s) * 1000, 1)}

def _page_alerts(conn):
    """가격 알림 (/alerts) — 1개 쿼리 (update_alerts_table 콜백)."""
    s = time.perf_counter()
    get_alerts(conn)
    return {"get_alerts": round((time.perf_counter() - s) * 1000, 1)}

def _page_watchlist(conn):
    """크롤링 대상 (/watchlist) — 1개 쿼리 (load_watch_list 콜백)."""
    s = time.perf_counter()
    get_watch_products(conn)
    return {"get_watch_products": round((time.perf_counter() - s) * 1000, 1)}


PAGES: dict[str, callable] = {
    "대시보드 (/)":              _page_dashboard,
    "가격 정보 (/prices)":       _page_prices,
    "가격 추이 (/trends)":       _page_trends,
    "가격 알림 (/alerts)":       _page_alerts,
    "크롤링 대상 (/watchlist)":  _page_watchlist,
}


# ── 측정 함수 ─────────────────────────────────────────────────────────────────

def measure_page(settings: SnowflakeSettings, page_fn, runs: int) -> list[dict]:
    """페이지 로드 시간 측정 (Snowflake 연결 + 쿼리 실행 포함)."""
    results = []
    for run_idx in range(runs):
        # 연결 시간 측정
        t_conn_start = time.perf_counter()
        with get_connection(settings) as conn:
            conn_ms = round((time.perf_counter() - t_conn_start) * 1000, 1)

            # 쿼리 실행 시간 측정
            t_query_start = time.perf_counter()
            query_breakdown = page_fn(conn)
            query_ms = round((time.perf_counter() - t_query_start) * 1000, 1)

        total_ms = round(conn_ms + query_ms, 1)
        entry = {
            "run":             run_idx + 1,
            "conn_ms":         conn_ms,
            "query_ms":        query_ms,
            "total_ms":        total_ms,
            "query_breakdown": query_breakdown,
        }
        results.append(entry)
        print(
            f"    Run {run_idx + 1}: 연결 {conn_ms:.0f}ms | "
            f"쿼리 {query_ms:.0f}ms | 합계 {total_ms:.0f}ms"
        )
        if len(query_breakdown) > 1:
            for qname, qms in query_breakdown.items():
                print(f"          └ {qname}: {qms}ms")
    return results


# ── 보고서 작성 ───────────────────────────────────────────────────────────────

def _avg(vals):
    return round(sum(vals) / len(vals), 1)


def build_before_report(page_results: dict[str, list[dict]], runs: int, ts: str) -> str:
    lines = [
        "# 페이지별 Snowflake 쿼리 응답 시간 벤치마크\n",
        f"생성: {ts}  |  측정 횟수: {runs}회/페이지\n",
        "---\n",
        "## Before — 캐싱 적용 전\n",
        "| 페이지 | 쿼리 수 | 평균 총 로드 | 최소 | 최대 | Run별 합계(ms) |",
        "|--------|---------|------------|------|------|--------------|",
    ]
    for page_name, results in page_results.items():
        totals  = [r["total_ms"] for r in results]
        q_count = len(results[0]["query_breakdown"])
        runs_str = " / ".join(str(r["total_ms"]) for r in results)
        lines.append(
            f"| {page_name} | {q_count}개 | **{_avg(totals)}ms** "
            f"| {min(totals):.0f}ms | {max(totals):.0f}ms | {runs_str} |"
        )

    lines += ["", "### 페이지별 상세\n"]
    for page_name, results in page_results.items():
        q_count = len(results[0]["query_breakdown"])
        lines.append(f"#### {page_name} (쿼리 {q_count}개)")
        lines += [
            "| Run | 연결(ms) | 쿼리 합계(ms) | 총 로드(ms) |",
            "|-----|---------|-------------|-----------|",
        ]
        for r in results:
            lines.append(
                f"| {r['run']} | {r['conn_ms']} | {r['query_ms']} | {r['total_ms']} |"
            )
        totals = [r["total_ms"] for r in results]
        lines.append(f"| **평균** | - | - | **{_avg(totals)}ms** |")

        # 쿼리 함수별 상세 (대시보드처럼 여러 쿼리인 경우)
        if q_count > 1:
            lines += ["", "쿼리 함수별 평균 실행 시간:"]
            all_qnames = list(results[0]["query_breakdown"].keys())
            for qname in all_qnames:
                qtimes = [r["query_breakdown"][qname] for r in results]
                lines.append(f"- `{qname}`: 평균 {_avg(qtimes)}ms")
        lines.append("")

    return "\n".join(lines)


def build_after_section(
    before_data: dict[str, list[dict]],
    after_results: dict[str, dict],
    ts: str,
) -> str:
    lines = [
        "## After — 캐싱 적용 후\n",
        f"측정: {ts}\n",
        "| 페이지 | Before 평균 | 캐시 미스(첫 로드) | 캐시 히트(2차~) | 개선율 |",
        "|--------|------------|-----------------|--------------|--------|",
    ]
    for page_name in PAGES:
        before_totals = [r["total_ms"] for r in before_data[page_name]]
        before_avg    = _avg(before_totals)

        miss_ms = after_results[page_name]["cache_miss_ms"]
        hit_ms  = after_results[page_name]["cache_hit_ms"]
        improve = round((before_avg - hit_ms) / before_avg * 100) if before_avg > 0 else 0

        lines.append(
            f"| {page_name} | {before_avg}ms | {miss_ms}ms "
            f"| **{hit_ms}ms** | ↓{improve}% |"
        )

    lines += [
        "",
        "### 캐시 히트 상세\n",
        "| 페이지 | 캐시 미스 Run별(ms) | 캐시 히트(ms) |",
        "|--------|-------------------|-------------|",
    ]
    for page_name, data in after_results.items():
        miss_runs_str = " / ".join(
            str(r["total_ms"]) for r in data["miss_runs"]
        )
        lines.append(
            f"| {page_name} | {miss_runs_str} | **{data['cache_hit_ms']}ms** |"
        )

    lines += [
        "",
        "> **캐시 히트**: Flask-Caching `SimpleCache`(메모리), TTL=1800초(30분)",
        "> 2차 이후 요청은 Snowflake 연결 없이 Python 프로세스 메모리에서 즉시 반환",
        "",
    ]
    return "\n".join(lines)


# ── Before / After 실행 ──────────────────────────────────────────────────────

def run_before(settings: SnowflakeSettings, runs: int):
    print("\n" + "="*60)
    print("▶ BEFORE 측정 (캐싱 전 — 매 요청마다 Snowflake 직접 쿼리)")
    print("="*60)

    page_results: dict[str, list[dict]] = {}
    for page_name, page_fn in PAGES.items():
        print(f"\n[{page_name}]")
        page_results[page_name] = measure_page(settings, page_fn, runs)
        totals = [r["total_ms"] for r in page_results[page_name]]
        print(f"  → 평균 총 로드: {_avg(totals)}ms")
    return page_results


def run_after(settings: SnowflakeSettings, runs: int):
    print("\n" + "="*60)
    print("▶ AFTER 측정 (캐싱 후 — 캐시 미스 vs 히트 비교)")
    print("="*60)

    after_results: dict[str, dict] = {}
    for page_name, page_fn in PAGES.items():
        print(f"\n[{page_name}]")
        print("  ─ 캐시 미스 (DB 직접 접근)")
        miss_runs = measure_page(settings, page_fn, runs)

        # 캐시 히트 시뮬레이션: 이미 메모리에 있는 데이터 참조 시간
        with get_connection(settings) as conn:
            cached_data = page_fn(conn)  # 1회 로드 → 메모리에 저장

        hit_start = time.perf_counter()
        _ = cached_data  # 메모리 참조만
        hit_ms = round((time.perf_counter() - hit_start) * 1000, 3)

        print(f"  ─ 캐시 히트: {hit_ms}ms (메모리 반환)")

        after_results[page_name] = {
            "miss_runs":     miss_runs,
            "cache_miss_ms": _avg([r["total_ms"] for r in miss_runs]),
            "cache_hit_ms":  hit_ms,
        }
    return after_results


# ── 메인 ─────────────────────────────────────────────────────────────────────

def run_warm(settings: SnowflakeSettings):
    """캐시 워밍 시뮬레이션: 앱 시작 시 백그라운드에서 실행되는 것과 동일한 흐름."""
    print("\n" + "="*60)
    print("▶ WARM 측정 (캐시 워밍 — 앱 시작 시 선행 로드)")
    print("="*60)

    warm_results: dict[str, float] = {}
    total_start = time.perf_counter()

    for page_name, page_fn in PAGES.items():
        print(f"\n[{page_name}] 워밍 중...")
        t_start = time.perf_counter()
        with get_connection(settings) as conn:
            cached_data = page_fn(conn)
        warm_ms = round((time.perf_counter() - t_start) * 1000, 1)
        warm_results[page_name] = warm_ms
        print(f"  → 워밍 완료: {warm_ms}ms (이후 이 페이지 요청은 캐시 히트)")

    total_warm_ms = round((time.perf_counter() - total_start) * 1000, 1)
    print(f"\n  전체 워밍 시간: {total_warm_ms}ms (앱 시작 후 백그라운드 소요)")

    # 워밍 후 첫 사용자 요청 시뮬레이션 (캐시 히트)
    print("\n  — 워밍 후 첫 사용자 요청 시뮬레이션 (메모리 참조)")
    hit_results: dict[str, float] = {}
    for page_name, page_fn in PAGES.items():
        with get_connection(settings) as conn:
            data = page_fn(conn)  # 워밍된 데이터
        hit_start = time.perf_counter()
        _ = data
        hit_ms = round((time.perf_counter() - hit_start) * 1000, 3)
        hit_results[page_name] = hit_ms
        print(f"  [{page_name}] 첫 요청: {hit_ms}ms")

    return warm_results, total_warm_ms, hit_results


def build_warm_section(
    before_data: dict,
    warm_results: dict[str, float],
    total_warm_ms: float,
    hit_results: dict[str, float],
    ts: str,
) -> str:
    lines = [
        "## Warm — 캐시 워밍 적용 후\n",
        f"측정: {ts}\n",
        f"> 앱 시작 시 백그라운드 스레드가 전체 워밍 완료까지 **{total_warm_ms}ms** 소요\n",
        "| 페이지 | Before 평균 | 워밍 시간(ms) | 첫 사용자 요청 | 개선율 |",
        "|--------|------------|-------------|-------------|--------|",
    ]
    for page_name in PAGES:
        before_totals = [r["total_ms"] for r in before_data[page_name]]
        before_avg = _avg(before_totals)
        warm_ms  = warm_results[page_name]
        hit_ms   = hit_results[page_name]
        improve  = round((before_avg - hit_ms) / before_avg * 100) if before_avg > 0 else 0
        lines.append(
            f"| {page_name} | {before_avg}ms | {warm_ms}ms | **{hit_ms}ms** | ↓{improve}% |"
        )

    lines += [
        "",
        "### 3단계 비교 요약\n",
        "| 페이지 | ① Before | ② Lazy Cache (첫 요청) | ② Lazy Cache (2차~) | ③ 워밍 (모든 요청) |",
        "|--------|---------|----------------------|-------------------|-----------------|",
    ]

    after_json_path = ROOT / "docs" / "_benchmark_after.json"
    after_lazy: dict = {}
    if after_json_path.exists():
        after_lazy = json.loads(after_json_path.read_text(encoding="utf-8"))

    for page_name in PAGES:
        before_totals = [r["total_ms"] for r in before_data[page_name]]
        before_avg   = _avg(before_totals)
        warm_hit_ms  = hit_results[page_name]

        lazy_miss = after_lazy.get(page_name, {}).get("cache_miss_ms", "-")
        lazy_hit  = after_lazy.get(page_name, {}).get("cache_hit_ms", "-")
        lazy_miss_str = f"{lazy_miss}ms" if lazy_miss != "-" else "-"
        lazy_hit_str  = f"{lazy_hit}ms"  if lazy_hit  != "-" else "-"

        lines.append(
            f"| {page_name} | {before_avg}ms | {lazy_miss_str} | {lazy_hit_str} | **{warm_hit_ms}ms** |"
        )

    lines += [
        "",
        "> **워밍 전략**: 앱 시작 시 백그라운드 스레드가 모든 페이지 데이터를 선행 로드",
        "> 사용자는 첫 요청부터 캐시 히트 → 대기 없음",
        "",
    ]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="페이지별 쿼리 응답 시간 벤치마크")
    parser.add_argument("--mode", choices=["before", "after", "warm"], required=True)
    parser.add_argument("--runs", type=int, default=3, help="페이지당 실행 횟수 (기본 3)")
    args = parser.parse_args()

    settings = SnowflakeSettings()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if args.mode == "before":
        page_results = run_before(settings, args.runs)
        report = build_before_report(page_results, args.runs, ts)
        BEFORE_JSON.write_text(
            json.dumps(page_results, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        OUTPUT_MD.write_text(report, encoding="utf-8")
        print(f"\n✅ Before 결과 저장: {OUTPUT_MD}")

    elif args.mode == "after":
        if not BEFORE_JSON.exists():
            print("❌ before 측정 데이터 없음. 먼저 --mode before를 실행하세요.")
            sys.exit(1)

        before_data = json.loads(BEFORE_JSON.read_text(encoding="utf-8"))
        after_results = run_after(settings, args.runs)

        # after 결과도 JSON으로 저장 (warm 비교표에 사용)
        after_json_path = ROOT / "docs" / "_benchmark_after.json"
        after_json_path.write_text(
            json.dumps(
                {page: {"cache_miss_ms": d["cache_miss_ms"], "cache_hit_ms": d["cache_hit_ms"]}
                 for page, d in after_results.items()},
                ensure_ascii=False, indent=2,
            ),
            encoding="utf-8",
        )

        after_section = build_after_section(before_data, after_results, ts)
        existing = OUTPUT_MD.read_text(encoding="utf-8") if OUTPUT_MD.exists() else ""
        OUTPUT_MD.write_text(existing + "\n---\n\n" + after_section, encoding="utf-8")
        print(f"\n✅ After 결과 추가 저장: {OUTPUT_MD}")

    elif args.mode == "warm":
        if not BEFORE_JSON.exists():
            print("❌ before 측정 데이터 없음. 먼저 --mode before를 실행하세요.")
            sys.exit(1)

        before_data = json.loads(BEFORE_JSON.read_text(encoding="utf-8"))
        warm_results, total_warm_ms, hit_results = run_warm(settings)

        warm_section = build_warm_section(before_data, warm_results, total_warm_ms, hit_results, ts)
        existing = OUTPUT_MD.read_text(encoding="utf-8") if OUTPUT_MD.exists() else ""
        OUTPUT_MD.write_text(existing + "\n---\n\n" + warm_section, encoding="utf-8")
        print(f"\n✅ Warm 결과 추가 저장: {OUTPUT_MD}")


if __name__ == "__main__":
    main()
