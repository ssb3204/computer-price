"""Tests for parser utilities."""

from src.crawlers.parser_utils import normalize_product_name, parse_korean_price, classify_category


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
