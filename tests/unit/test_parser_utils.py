"""Tests for parser utilities."""

import pytest

from src.crawlers.parser_utils import classify_category, normalize_product_name, parse_korean_price, validate_price


class TestNormalizeProductName:
    def test_basic_normalization(self):
        assert normalize_product_name("  AMD Ryzen 7  7800X3D  ") == "amd ryzen 7 7800x3d"

    def test_korean_preserved(self):
        result = normalize_product_name("삼성 DDR5 16GB")
        assert "삼성" in result
        assert "ddr5" in result

    def test_special_chars_removed(self):
        result = normalize_product_name("RTX 4090 [박스포장]")
        assert "[" not in result
        assert "]" not in result


class TestParseKoreanPrice:
    def test_standard_price(self):
        assert parse_korean_price("450,000원") == 450000

    def test_man_won_notation(self):
        assert parse_korean_price("15만원") == 150000

    def test_man_with_remainder(self):
        assert parse_korean_price("15만5000") == 155000

    def test_plain_digits(self):
        assert parse_korean_price("320000") == 320000

    def test_invalid_returns_none(self):
        assert parse_korean_price("가격미정") is None

    def test_empty_returns_none(self):
        assert parse_korean_price("") is None


class TestClassifyCategory:
    def test_cpu(self):
        assert classify_category("AMD 라이젠 7 7800X3D") == "CPU"

    def test_gpu(self):
        assert classify_category("NVIDIA GeForce RTX 4090") == "GPU"

    def test_ram(self):
        assert classify_category("삼성 DDR5 16GB") == "RAM"

    def test_ssd(self):
        assert classify_category("삼성 990 PRO NVMe M.2 SSD 1TB") == "SSD"

    def test_unknown(self):
        assert classify_category("알 수 없는 제품") == "Other"


class TestValidatePrice:
    # CPU: 10_000 ~ 3_000_000
    def test_cpu_valid(self):
        assert validate_price(450_000, "CPU") is True

    def test_cpu_zero(self):
        assert validate_price(0, "CPU") is False

    def test_cpu_negative(self):
        assert validate_price(-1, "CPU") is False

    def test_cpu_below_min(self):
        assert validate_price(9_999, "CPU") is False

    def test_cpu_boundary_low(self):
        assert validate_price(10_000, "CPU") is True

    def test_cpu_boundary_high(self):
        assert validate_price(3_000_000, "CPU") is True

    def test_cpu_above_max(self):
        assert validate_price(3_000_001, "CPU") is False

    # GPU: 30_000 ~ 6_000_000
    def test_gpu_valid(self):
        assert validate_price(1_500_000, "GPU") is True

    def test_gpu_above_max(self):
        assert validate_price(9_999_999, "GPU") is False

    # RAM: 3_000 ~ 1_000_000
    def test_ram_valid(self):
        assert validate_price(50_000, "RAM") is True

    def test_ram_below_min(self):
        assert validate_price(100, "RAM") is False

    # SSD: 5_000 ~ 2_000_000
    def test_ssd_valid(self):
        assert validate_price(120_000, "SSD") is True

    def test_ssd_below_min(self):
        assert validate_price(4_999, "SSD") is False

    # 알 수 없는 카테고리: 기본 범위(1_000 ~ 10_000_000) 적용
    def test_unknown_category_valid(self):
        assert validate_price(500_000, "OTHER") is True

    def test_unknown_category_zero(self):
        assert validate_price(0, "OTHER") is False
