"""Unit tests: 조합 총액 추이 집계 (DB 없이 순수 함수만 검증).

집계 규칙이 이 기능의 핵심이라 엣지 케이스를 촘촘히 고정한다.
"""

from datetime import date

from src.api.build_trend import TrendPoint, build_daily_totals, take_last_days

D = date  # 가독성용 별칭


def _dates(points: list[TrendPoint]) -> list[date]:
    return [p.date for p in points]


def _totals(points: list[TrendPoint]) -> list[int]:
    return [p.total for p in points]


# ── 기본 집계 ────────────────────────────────────────────────────────────────


class TestBasicAggregation:
    def test_single_part_returns_its_own_prices(self):
        result = build_daily_totals({1: [(D(2026, 7, 1), 500), (D(2026, 7, 2), 480)]})

        assert _dates(result) == [D(2026, 7, 1), D(2026, 7, 2)]
        assert _totals(result) == [500, 480]

    def test_two_parts_are_summed_per_day(self):
        result = build_daily_totals({
            1: [(D(2026, 7, 1), 500), (D(2026, 7, 2), 480)],
            2: [(D(2026, 7, 1), 300), (D(2026, 7, 2), 310)],
        })

        assert _totals(result) == [800, 790]

    def test_point_carries_per_part_prices(self):
        """총액뿐 아니라 부품별 적용 가격도 함께 담는다(차트 툴팁·내역 표시용)."""
        result = build_daily_totals({
            10: [(D(2026, 7, 1), 500)],
            20: [(D(2026, 7, 1), 300)],
        })

        assert result[0].prices == {10: 500, 20: 300}
        assert result[0].total == 800

    def test_input_order_does_not_matter(self):
        """입력이 날짜순이 아니어도 결과는 같다."""
        shuffled = build_daily_totals({
            1: [(D(2026, 7, 3), 300), (D(2026, 7, 1), 500), (D(2026, 7, 2), 400)]
        })

        assert _dates(shuffled) == [D(2026, 7, 1), D(2026, 7, 2), D(2026, 7, 3)]
        assert _totals(shuffled) == [500, 400, 300]


# ── forward fill (수집 없는 날) ──────────────────────────────────────────────


class TestForwardFill:
    def test_missing_day_keeps_previous_price(self):
        """7/2 수집 실패 → 7/1 가격을 이어 쓴다. 총액이 꺼지면 안 된다."""
        result = build_daily_totals({
            1: [(D(2026, 7, 1), 500), (D(2026, 7, 3), 450)],
            2: [(D(2026, 7, 1), 300), (D(2026, 7, 2), 300), (D(2026, 7, 3), 300)],
        })

        assert _dates(result) == [D(2026, 7, 1), D(2026, 7, 2), D(2026, 7, 3)]
        assert _totals(result) == [800, 800, 750]

    def test_long_gap_is_filled_every_day(self):
        """긴 공백도 하루도 빠짐없이 채운다(선이 끊기지 않게)."""
        result = build_daily_totals({1: [(D(2026, 7, 1), 100), (D(2026, 7, 5), 200)]})

        assert _dates(result) == [D(2026, 7, i) for i in range(1, 6)]
        assert _totals(result) == [100, 100, 100, 100, 200]

    def test_no_extrapolation_past_last_collection(self):
        """마지막 수집일 이후로는 만들어내지 않는다."""
        result = build_daily_totals({
            1: [(D(2026, 7, 1), 100), (D(2026, 7, 2), 100)],
            2: [(D(2026, 7, 1), 200), (D(2026, 7, 5), 200)],
        })

        assert result[-1].date == D(2026, 7, 5)


# ── 시작일 결정 (부품별 이력 시작이 다를 때) ────────────────────────────────


class TestStartDate:
    def test_starts_when_all_parts_have_price(self):
        """뒤늦게 담긴 부품이 있으면 그 부품이 값을 갖는 날부터 그린다.

        있는 것만 더하면 7/3에 총액이 500→800으로 튀어 '급등'처럼 보인다.
        """
        result = build_daily_totals({
            1: [(D(2026, 7, 1), 500), (D(2026, 7, 2), 500), (D(2026, 7, 3), 500)],
            2: [(D(2026, 7, 3), 300)],
        })

        assert _dates(result) == [D(2026, 7, 3)], "모든 부품이 값을 갖는 날부터여야 한다"
        assert _totals(result) == [800]

    def test_earlier_part_price_is_carried_into_start(self):
        """시작일 이전의 가격도 forward fill 대상이다.

        부품1의 마지막 알려진 가격(7/2의 450)이 시작일 7/3에 적용돼야 한다.
        """
        result = build_daily_totals({
            1: [(D(2026, 7, 1), 500), (D(2026, 7, 2), 450)],
            2: [(D(2026, 7, 3), 300)],
        })

        assert _dates(result) == [D(2026, 7, 3)]
        assert result[0].prices[1] == 450, "시작일 이전 최신가를 못 끌고 왔다"
        assert result[0].total == 750


# ── 집계 불가 케이스 ─────────────────────────────────────────────────────────


class TestCannotAggregate:
    def test_empty_build_returns_empty(self):
        assert build_daily_totals({}) == []

    def test_part_without_any_price_blocks_aggregation(self):
        """가격 이력이 없는 부품이 하나라도 있으면 총액을 만들 수 없다."""
        result = build_daily_totals({
            1: [(D(2026, 7, 1), 500)],
            2: [],  # 방금 담겨서 아직 크롤링 전
        })

        assert result == []


# ── 같은 날 중복 입력 ────────────────────────────────────────────────────────


class TestDuplicateDates:
    def test_later_entry_wins_for_same_day(self):
        """같은 날짜가 두 번 들어오면 뒤엣값을 쓴다.

        SQL 이 ROW_NUMBER 로 하루 1개만 넘겨주지만, 방어적으로 고정해둔다.
        """
        result = build_daily_totals({1: [(D(2026, 7, 1), 500), (D(2026, 7, 1), 480)]})

        assert _totals(result) == [480]


# ── 기간 자르기 ──────────────────────────────────────────────────────────────


class TestTakeLastDays:
    def _week(self) -> list[TrendPoint]:
        return build_daily_totals({1: [(D(2026, 7, d), 100 * d) for d in range(1, 8)]})

    def test_takes_recent_window(self):
        result = take_last_days(self._week(), days=3)

        assert _dates(result) == [D(2026, 7, 5), D(2026, 7, 6), D(2026, 7, 7)]

    def test_none_returns_everything(self):
        assert len(take_last_days(self._week(), days=None)) == 7

    def test_window_larger_than_data_returns_everything(self):
        assert len(take_last_days(self._week(), days=365)) == 7

    def test_empty_input_is_safe(self):
        assert take_last_days([], days=7) == []

    def test_slicing_happens_after_fill(self):
        """자르기는 집계 뒤에 해야 창 첫날 값이 살아있다.

        7/1에만 가격이 있고 7/2~7/5는 공백인 데이터에서 최근 2일을 잘라도,
        forward fill 된 값이 남아 있어야 한다(먼저 자르면 값이 비어버린다).
        """
        filled = build_daily_totals({1: [(D(2026, 7, 1), 500), (D(2026, 7, 5), 500)]})
        result = take_last_days(filled, days=2)

        assert _dates(result) == [D(2026, 7, 4), D(2026, 7, 5)]
        assert _totals(result) == [500, 500]
