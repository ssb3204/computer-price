"""크롤러 유닛 테스트 공용 픽스처.

3사(다나와/컴퓨존/견적왕) 크롤러는 모두 생성자로 pymysql Connection 을 받고,
stg_watchlist 를 (query, pcode, category, brand) 4튜플로 동일하게 조회한다.
그래서 워치리스트 mock 커넥션을 하나로 공유할 수 있다.
"""

from unittest.mock import MagicMock

import pytest


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
