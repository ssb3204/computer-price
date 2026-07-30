"""Shared parsing utilities for Korean price text."""

import re


def parse_korean_price(text: str) -> int | None:
    text = text.strip().replace(",", "").replace(" ", "")

    # Handle "만원" notation (e.g., "15만원" = 150,000)
    match = re.search(r"(\d+)만(\d*)", text)
    if match:
        man = int(match.group(1))
        remainder = int(match.group(2)) if match.group(2) else 0
        return man * 10000 + remainder

    # Standard numeric price
    digits = re.sub(r"[^\d]", "", text)
    if digits:
        return int(digits)

    return None


# ── 카테고리별 유효 가격 범위 (원) ──────────────────────────────────────────
_PRICE_RANGE: dict[str, tuple[int, int]] = {
    "CPU": (10_000, 3_000_000),
    "GPU": (30_000, 6_000_000),
    "RAM": (3_000, 1_000_000),
    "SSD": (5_000, 2_000_000),
}
_DEFAULT_PRICE_RANGE: tuple[int, int] = (1_000, 10_000_000)


def validate_price(price: int, category: str) -> bool:
    """카테고리별 유효 가격 범위를 벗어난 이상치 여부 검사.

    Returns:
        True  — 정상 가격
        False — 이상치 (0 이하, 범위 초과)
    """
    if price <= 0:
        return False
    lo, hi = _PRICE_RANGE.get(category, _DEFAULT_PRICE_RANGE)
    return lo <= price <= hi
