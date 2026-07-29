"""크롤러 유닛 테스트 공용 픽스처.

3사(다나와/컴퓨존/견적왕) 크롤러는 모두 생성자로 pymysql Connection 을 받고,
stg_watchlist 를 (query, pcode, category, brand) 4튜플로 동일하게 조회한다.
그래서 워치리스트 mock 커넥션을 하나로 공유할 수 있다.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest


class FakeClock:
    """호출할 때마다 1초씩 증가하는 가짜 시계.

    크롤러가 datetime.now() 를 대상·페이지마다 부르면 한 회차 안에서 수집 시각이
    갈리는데, 실제 시계로는 마이크로초 차이라 테스트가 불안정하다. 호출마다 눈에
    띄게 다른 값을 돌려주어 "한 회차 = 한 시각"을 결정적으로 검증한다.

    사용 예:
        with patch("src.crawlers.danawa.datetime", FakeClock()):
    """

    def __init__(self) -> None:
        self.calls = 0

    def now(self, tz: object = None) -> datetime:
        self.calls += 1
        return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=self.calls)


@pytest.fixture
def make_watch_conn():
    """stg_watchlist 조회 결과를 흉내내는 mock 커넥션 팩토리.

    실제 _load_watch_products() 를 그대로 태우기 위해 커넥션만 가짜로 만든다.
    (SELECT 결과 → dict 변환 로직까지 검증 대상에 포함시키려는 의도)

    사용 예:
        conn = make_watch_conn([("라이젠 7800X3D", "19627934", "CPU", "AMD")])
    """

    def _make(rows: list[tuple[str, str, str, str | None]]) -> MagicMock:
        cursor = MagicMock()
        cursor.fetchall.return_value = list(rows)
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return conn

    return _make
