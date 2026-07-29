"""컴퓨존 크롤링 진단 — 3단계 경로 중 어디까지 살아 있는지 판정한다.

크롤링이 0건이 됐을 때 원인이 ①요청 계약 ②셀렉터 ③대상이 스캔 범위 밖
중 무엇인지 구분하기 위한 도구다. 운영 로그는 셋을 모두 "0건"으로만 보여준다.

파라미터·URL·상세 파서를 src.crawlers.compuzone에서 **그대로 import**한다.
여기서 값을 복사해 두면 크롤러가 바뀔 때 진단만 낡아 거짓 정상을 보고하게 된다.

DB는 읽기만 한다 (stg_watchlist). raw/stg에는 아무것도 쓰지 않는다.

실행:
    python scripts/diagnose_compuzone.py
    python scripts/diagnose_compuzone.py --save-html   # 응답을 tmp/에 저장(셀렉터 변경 추적용)
"""

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from src.common.config import MySQLSettings
from src.common.mysql_client import get_connection
from src.crawlers.compuzone import (
    CATEGORY_MEDIUM_DIV_NO,
    LIST_URL,
    MAX_CRAWL_PAGES,
    MAX_SEARCH_PAGES,
    SEARCH_URL,
    _build_list_form,
    _build_search_params,
    _fetch_price_from_detail,
)

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

REQUEST_DELAY_SECONDS = 2.0
TIMEOUT = (5.0, 20.0)
HTML_DIR = Path("tmp")


@dataclass(frozen=True)
class Target:
    """진단 대상 — stg_watchlist 한 행."""
    query: str
    product_no: str
    category: str


@dataclass(frozen=True)
class PathResult:
    """경로 하나의 진단 결과."""
    path: str            # "① 목록" | "② 검색" | "③ 상세"
    found: bool
    requests_made: int
    elapsed: float
    items_seen: int      # 파싱된 li.li-obj 총 개수
    rank: int | None     # 대상이 몇 번째로 나왔나 (1-based)
    note: str

    @property
    def verdict(self) -> str:
        if self.found:
            return "OK"
        if self.items_seen == 0:
            return "셀렉터/응답 이상"   # 항목 자체가 안 잡힘
        return "범위 밖"                # 항목은 있는데 대상이 없음


def load_targets(settings: MySQLSettings) -> list[Target]:
    """stg_watchlist에서 컴퓨존 활성 대상을 읽는다 (읽기 전용)."""
    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT `query`, `pcode`, `category` FROM `stg_watchlist` "
            "WHERE `is_active` = 1 AND `site` = '컴퓨존' ORDER BY `category`, `query`"
        )
        rows = cur.fetchall()
        cur.close()
    return [Target(query=r[0], product_no=r[1], category=r[2]) for r in rows]


def _save(html: str, name: str) -> None:
    HTML_DIR.mkdir(exist_ok=True)
    (HTML_DIR / f"compuzone_{name}.html").write_text(html, encoding="utf-8")


def _scan(html: str, product_no: str) -> tuple[int, int | None]:
    """페이지에서 li.li-obj 개수와 대상의 등장 위치를 센다."""
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("li.li-obj")
    for idx, el in enumerate(items, start=1):
        raw_id = el.get("id", "")
        if isinstance(raw_id, str) and raw_id.removeprefix("li-pno-") == product_no:
            return len(items), idx
    return len(items), None


def probe_list_path(session: requests.Session, target: Target, save: bool) -> PathResult:
    """① product_list.php POST — 카테고리 목록(검색어 없음)을 페이지네이션."""
    mdno = CATEGORY_MEDIUM_DIV_NO.get(target.category)
    if mdno is None:
        return PathResult("① 목록", False, 0, 0.0, 0, None,
                          f"미지원 카테고리 '{target.category}' — 영구 미수집")

    started, seen, reqs = time.perf_counter(), 0, 0
    for page in range(1, MAX_CRAWL_PAGES + 1):
        resp = session.post(LIST_URL, data=_build_list_form(mdno, page=page), timeout=TIMEOUT)
        resp.encoding = "euc-kr"
        reqs += 1
        if save:
            _save(resp.text, f"{target.product_no}_list_p{page}")
        count, idx = _scan(resp.text, target.product_no)
        seen += count
        if idx is not None:
            rank = (page - 1) * 20 + idx
            return PathResult("① 목록", True, reqs, time.perf_counter() - started,
                              seen, rank, f"{rank}위 / page {page}")
        if count == 0:
            break
        time.sleep(REQUEST_DELAY_SECONDS)

    return PathResult("① 목록", False, reqs, time.perf_counter() - started, seen, None,
                      f"앞 {seen}개 안에 없음")


def probe_search_path(session: requests.Session, target: Target, save: bool) -> PathResult:
    """② search_list.php GET — 검색어 기반 조회."""
    mdno = CATEGORY_MEDIUM_DIV_NO.get(target.category, "")
    started, seen, reqs = time.perf_counter(), 0, 0

    for page in range(1, MAX_SEARCH_PAGES + 1):
        params = _build_search_params(target.query, page)
        if mdno:
            params["MediumDivNo"] = mdno
        resp = session.get(SEARCH_URL, params=params, timeout=TIMEOUT)
        resp.encoding = "euc-kr"
        reqs += 1
        if save:
            _save(resp.text, f"{target.product_no}_search_p{page}")
        count, idx = _scan(resp.text, target.product_no)
        seen += count
        if idx is not None:
            rank = (page - 1) * 20 + idx
            return PathResult("② 검색", True, reqs, time.perf_counter() - started,
                              seen, rank, f"{rank}위 / page {page}")
        if count == 0:
            break
        time.sleep(REQUEST_DELAY_SECONDS)

    return PathResult("② 검색", False, reqs, time.perf_counter() - started, seen, None,
                      f"검색 결과 {seen}건 중 없음")


def probe_detail_path(target: Target) -> PathResult:
    """③ 상세페이지 정규식 — todayCookie에서 가격 추출."""
    started = time.perf_counter()
    detail = _fetch_price_from_detail(target.product_no)
    elapsed = time.perf_counter() - started
    if detail is None:
        return PathResult("③ 상세", False, 1, elapsed, 0, None, "todayCookie 미발견")
    name, price = detail
    return PathResult("③ 상세", True, 1, elapsed, 1, None, f"{price}원 | {name[:40]}")


def diagnose(target: Target, save: bool, all_paths: bool = False) -> list[PathResult]:
    """운영과 같은 순서로 경로를 시도한다.

    all_paths=True면 앞 경로가 성공해도 나머지를 계속 측정한다.
    운영 순서대로만 돌리면 1차가 늘 성공하는 동안 2·3차가 살아 있는지
    영영 알 수 없다 — 경로를 제거·재배치하려면 각 경로의 독립 성공률이 필요하다.
    """
    session = requests.Session()
    results = [probe_list_path(session, target, save)]
    if all_paths or not results[-1].found:
        results.append(probe_search_path(session, target, save))
    if all_paths or not results[-1].found:
        results.append(probe_detail_path(target))
    return results


def report(target: Target, results: list[PathResult]) -> None:
    print(f"\n[{target.category}] {target.query}  ProductNo={target.product_no}")
    for r in results:
        mark = "OK  " if r.found else "FAIL"
        print(f"  {mark} {r.path}  요청 {r.requests_made}회 / {r.elapsed:5.1f}초 / "
              f"항목 {r.items_seen:3d}개 / {r.verdict:12s} {r.note}")
    total_req = sum(r.requests_made for r in results)
    total_sec = sum(r.elapsed for r in results)
    status = "수집 성공" if any(r.found for r in results) else "*** 전 경로 실패 ***"
    print(f"  → {status} (총 요청 {total_req}회 / {total_sec:.1f}초)")


def main() -> int:
    parser = argparse.ArgumentParser(description="컴퓨존 크롤링 경로 진단")
    parser.add_argument("--save-html", action="store_true",
                        help="응답 HTML을 tmp/에 저장 (셀렉터 변경 시 diff용)")
    parser.add_argument("--all-paths", action="store_true",
                        help="앞 경로가 성공해도 모든 경로를 측정 (경로별 독립 성공률 확인)")
    args = parser.parse_args()

    load_dotenv()
    targets = load_targets(MySQLSettings())
    if not targets:
        print("컴퓨존 활성 대상이 없습니다 (stg_watchlist).")
        return 1

    print(f"=== 컴퓨존 진단 — 대상 {len(targets)}건 ===")
    failed = 0
    for target in targets:
        results = diagnose(target, args.save_html, args.all_paths)
        report(target, results)
        if not any(r.found for r in results):
            failed += 1

    print(f"\n=== 요약: {len(targets) - failed}/{len(targets)} 성공 ===")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
