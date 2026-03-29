"""Shared parsing utilities for Korean price/product text."""

import re
import unicodedata


def normalize_product_name(raw: str) -> str:
    text = unicodedata.normalize("NFC", raw)
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\s가-힣.-]", "", text)
    return text


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


CATEGORIES: tuple[str, ...] = ("CPU", "GPU", "RAM", "SSD")

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


CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "CPU": ["cpu", "프로세서", "라이젠", "ryzen", "코어", "core i"],
    "GPU": ["gpu", "그래픽카드", "지포스", "geforce", "라데온", "radeon", "rtx", "rx"],
    "RAM": ["ram", "메모리", "ddr4", "ddr5"],
    "SSD": ["ssd", "nvme", "m.2", "저장장치"],
    "HDD": ["hdd", "하드디스크", "hard disk"],
    "Mainboard": ["메인보드", "mainboard", "motherboard", "마더보드"],
    "Power": ["파워", "power supply", "psu"],
    "Case": ["케이스", "case", "pc case"],
    "Cooler": ["쿨러", "cooler", "cooling"],
}


def classify_category(product_name: str) -> str:
    name_lower = product_name.lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in name_lower for kw in keywords):
            return category
    return "Other"
