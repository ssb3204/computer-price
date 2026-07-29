"""컴퓨존 크롤링 진단 — 수집이 0건일 때 원인을 구분한다.

운영 로그는 "0건"만 알려주지만 원인은 셋 중 하나다:
  ① 요청 계약   — 응답 자체가 안 오거나 형식이 바뀜
  ② 셀렉터      — 응답은 오는데 li.li-obj 가 하나도 안 잡힘
  ③ 스캔 범위   — 항목은 잡히는데 대상 ProductNo 가 그 안에 없음
셋을 구분해주는 것이 이 스크립트의 존재 이유다.

URL·파라미터·파싱 헬퍼를 src.crawlers.compuzone 에서 그대로 import 한다.
여기서 값을 복사해 두면 크롤러가 바뀔 때 진단만 낡아 거짓 정상을 보고하게 된다.

DB는 stg_watchlist 를 읽기만 한다. raw/stg 에는 아무것도 쓰지 않는다.

주의: 로컬에서는 거의 항상 통과한다. 운영(GitHub Actions) 실패는 컴퓨존이 러너
IP에 무응답인 성격이라 여기서 재현되지 않는다 — 실행 환경을 함께 봐야 한다.

실행:
    python scripts/diagnose_compuzone.py
    python scripts/diagnose_compuzone.py --save-html   # 응답 저장(셀렉터 변경 추적용)
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
from src.crawlers.base import DEFAULT_HEADERS, REQUEST_TIMEOUT
from src.crawlers.compuzone import (
    CATEGORY_MEDIUM_DIV_NO,
    MAX_SEARCH_PAGES,
    SEARCH_URL,
    _build_search_params,
    _extract_name_price,
    _extract_product_no,
)

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

REQUEST_DELAY_SECONDS = 2.0
HTML_DIR = Path("tmp")


@dataclass(frozen=True)
class Target:
    """진단 대상 — stg_watchlist 한 행."""
    query: str
    product_no: str
    category: str


@dataclass(frozen=True)
class Result:
    """대상 하나의 진단 결과."""
    target: Target
    found: bool
    requests_made: int
    elapsed: float
    items_seen: int          # 파싱된 li.li-obj 총 개수
    rank: int | None         # 대상이 검색 결과에서 몇 번째인가 (1-based)
    detail: str

    @property
    def verdict(self) -> str:
        if self.found:
            return "OK"
        if self.requests_made == 0:
            return "미지원 카테고리"
        if self.items_seen == 0:
            return "셀렉터/응답 이상"
        return "스캔 범위 밖"


def load_targets(settings: MySQLSettings) -> list[Target]:
    """stg_watchlist 에서 컴퓨존 활성 대상을 읽는다 (읽기 전용)."""
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


def probe(session: requests.Session, target: Target, save: bool) -> Result:
    """크롤러와 같은 검색 경로를 타며 각 단계의 관측값을 기록한다."""
    mdno = CATEGORY_MEDIUM_DIV_NO.get(target.category)
    if mdno is None:
        return Result(target, False, 0, 0.0, 0, None,
                      f"'{target.category}' 는 CATEGORY_MEDIUM_DIV_NO 에 없음 — 영구 미수집")

    started, seen, reqs = time.perf_counter(), 0, 0

    for page in range(1, MAX_SEARCH_PAGES + 1):
        params = _build_search_params(target.query, page) | {"MediumDivNo": mdno}
        try:
            resp = session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            resp.encoding = "euc-kr"
        except requests.RequestException as e:
            return Result(target, False, reqs + 1, time.perf_counter() - started, seen, None,
                          f"요청 실패 (page {page}): {type(e).__name__}")
        reqs += 1
        if save:
            _save(resp.text, f"{target.product_no}_p{page}")

        items = BeautifulSoup(resp.text, "html.parser").select("li.li-obj")
        seen += len(items)

        for idx, el in enumerate(items, start=1):
            if _extract_product_no(el) != target.product_no:
                continue
            parsed = _extract_name_price(el)
            rank = (page - 1) * 20 + idx
            if parsed is None:
                return Result(target, False, reqs, time.perf_counter() - started, seen, rank,
                              f"{rank}위에서 발견했으나 이름/가격 파싱 실패 (품절?)")
            return Result(target, True, reqs, time.perf_counter() - started, seen, rank,
                          f"{rank}위 / page {page} | {parsed[1]}원 | {parsed[0][:40]}")

        if not items:
            break
        time.sleep(REQUEST_DELAY_SECONDS)

    return Result(target, False, reqs, time.perf_counter() - started, seen, None,
                  f"검색 결과 {seen}건 안에 없음")


def report(r: Result) -> None:
    mark = "OK  " if r.found else "FAIL"
    print(f"{mark} [{r.target.category:4s}] {r.target.query:<12s} ProductNo={r.target.product_no}")
    print(f"       요청 {r.requests_made}회 / {r.elapsed:5.1f}초 / 항목 {r.items_seen:3d}개 / "
          f"{r.verdict:14s} {r.detail}")


def main() -> int:
    parser = argparse.ArgumentParser(description="컴퓨존 크롤링 진단")
    parser.add_argument("--save-html", action="store_true",
                        help="응답 HTML을 tmp/에 저장 (셀렉터 변경 시 diff용)")
    args = parser.parse_args()

    load_dotenv()
    targets = load_targets(MySQLSettings())
    if not targets:
        print("컴퓨존 활성 대상이 없습니다 (stg_watchlist).")
        return 1

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    print(f"=== 컴퓨존 진단 — 대상 {len(targets)}건 ===")
    results = []
    for target in targets:
        result = probe(session, target, args.save_html)
        report(result)
        results.append(result)

    ok = sum(1 for r in results if r.found)
    total_req = sum(r.requests_made for r in results)
    total_sec = sum(r.elapsed for r in results)
    print(f"\n=== {ok}/{len(results)} 성공 · 총 요청 {total_req}회 · {total_sec:.1f}초 ===")
    return 0 if ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
