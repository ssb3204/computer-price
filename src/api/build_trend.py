"""조합 총액 추이 집계 (순수 함수).

DB 접근이 전혀 없다. 입력은 "부품별 · 일별 가격" 목록이고 출력은 "날짜별 총액"이다.
집계 규칙이 이 기능에서 가장 까다로운 부분이라, DB 없이 단위 테스트할 수 있도록
build_repo(SQL)와 분리했다.

집계 규칙:
  1. 하루를 한 점으로 본다. 같은 날 여러 번 크롤링됐으면 그날 마지막 값을 쓴다
     (이 선택은 SQL 쪽 ROW_NUMBER 에서 이미 끝나서 여기 입력은 하루 1개다).
  2. 수집이 없는 날은 직전 가격을 이어 쓴다(forward fill). 크롤링이 실패한 날
     총액이 0으로 꺼지거나 선이 끊기지 않게 하기 위함이다.
  3. 시작일은 "모든 부품이 가격을 가진 가장 이른 날"이다. 부품마다 워치리스트에
     담긴 시점이 달라 이력 시작일이 다른데, 있는 것만 더하면 뒤늦게 담긴 부품이
     합류하는 날 총액이 급등해 '가격이 올랐다'는 착시를 준다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class TrendPoint:
    """어느 하루의 조합 총액."""
    date: date
    total: int
    prices: dict[int, int]  # watchlist_id -> 그날 적용된 가격


def build_daily_totals(
    part_prices: dict[int, list[tuple[date, int]]],
) -> list[TrendPoint]:
    """부품별 일별 가격을 날짜별 총액으로 집계한다.

    Args:
        part_prices: {watchlist_id: [(날짜, 가격), ...]}. 날짜 순서는 상관없다.

    Returns:
        시작일부터 마지막 수집일까지 하루도 빠짐없이 채운 TrendPoint 목록.
        집계가 불가능하면 빈 목록:
          - 부품이 하나도 없는 조합
          - 가격 이력이 아예 없는 부품이 하나라도 있는 경우
            (그 부품 값을 모르니 '총액'을 만들 수 없다)
    """
    if not part_prices:
        return []
    if any(not points for points in part_prices.values()):
        return []

    # 부품별로 날짜 오름차순 정리. 같은 날짜가 중복돼 들어오면 뒤엣값이 이긴다.
    series: dict[int, list[tuple[date, int]]] = {
        wid: sorted(dict(points).items()) for wid, points in part_prices.items()
    }

    start = max(points[0][0] for points in series.values())   # 모두가 값을 갖는 첫날
    end = max(points[-1][0] for points in series.values())    # 마지막 수집일

    cursor = dict.fromkeys(series, 0)          # 부품별로 어디까지 소비했는지
    current: dict[int, int | None] = dict.fromkeys(series)    # 현재 적용 중인 가격

    result: list[TrendPoint] = []
    day = start
    while day <= end:
        for wid, points in series.items():
            i = cursor[wid]
            # 오늘까지의 값은 모두 소비한다 → 마지막으로 남은 게 오늘 적용 가격
            while i < len(points) and points[i][0] <= day:
                current[wid] = points[i][1]
                i += 1
            cursor[wid] = i

        # start 정의상 여기서 None 이 남을 수 없지만, 방어적으로 확인한다
        if all(v is not None for v in current.values()):
            snapshot = {wid: price for wid, price in current.items() if price is not None}
            result.append(
                TrendPoint(date=day, total=sum(snapshot.values()), prices=snapshot)
            )
        day += timedelta(days=1)

    return result


def take_last_days(points: list[TrendPoint], days: int | None) -> list[TrendPoint]:
    """최근 N일치만 남긴다. days 가 None 이면 전체를 반환한다.

    잘라내기를 집계 뒤에 하는 이유: 먼저 자르면 창 시작 직전의 가격을 잃어
    forward fill 이 깨진다(창 첫날 값이 비어버림).
    """
    if days is None or days <= 0 or not points:
        return points
    cutoff = points[-1].date - timedelta(days=days - 1)
    return [p for p in points if p.date >= cutoff]
