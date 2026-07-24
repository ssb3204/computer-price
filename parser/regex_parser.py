"""GPU 제품명을 manufacturer/chipset_maker/model_name/distributor로 분해하는 정규식·사전 기반 파서."""

import re

# 매칭 우선순위: 긴 토큰(예: "ZOTAC GAMING")을 짧은 토큰("ZOTAC")보다 먼저 검사해야 함.
MANUFACTURERS = [
    "ZOTAC GAMING",
    "ZOTAC",
    "GIGABYTE",
    "ASRock",
    "SAPPHIRE",
    "COLORFUL",
    "MANLI",
    "AOOSTAR",
    "PALIT",
    "GALAX",
    "ASUS",
    "MSI",
    "XFX",
    "GPD",
    "이엠텍",
]

# ASUS/GIGABYTE 등 제조사 뒤에 붙는 제품 라인명 — model_name에 포함시키되 제조사로는 취급하지 않음.
PRODUCT_LINE_PREFIXES = ["ROG Astral", "PRIME", "DUAL", "AORUS"]

DISTRIBUTORS = [
    "하이퍼프로져",
    "대원씨티에스",
    "트라이프로져4",
    "제이씨현",
    "피씨디렉트",
    "이엠텍",
]

CHIPSET_TOKENS = {
    "지포스": "NVIDIA",
    "라데온": "AMD",
}

MEMORY_SPEC_RE = re.compile(r"\bD\d+\s+\d+GB\b")
TRAILING_TAG_RE = re.compile(r"\s*\([^)]*\)\s*$")


def _strip_manufacturer_alias(text: str) -> str:
    """'갤럭시 GALAX' 처럼 한글 별칭이 영문 브랜드명 앞에 붙는 경우 별칭을 제거."""
    return re.sub(r"^갤럭시\s+", "", text)


def parse_gpu_name(product_name: str) -> dict:
    """GPU 제품명 문자열을 4개 필드로 분해한다.

    Returns:
        {"manufacturer": str|None, "chipset_maker": str|None,
         "model_name": str|None, "distributor": str|None}
    """
    text = product_name.strip()
    text = TRAILING_TAG_RE.sub("", text)  # 말미의 "(해외구매)" 등 태그 제거

    chipset_maker = None
    for token, maker in CHIPSET_TOKENS.items():
        if token in text:
            chipset_maker = maker
            text = text.replace(token, " ")
            break

    distributor = None
    for dist in DISTRIBUTORS:
        if text.rstrip().endswith(dist):
            distributor = dist
            text = text.rstrip()[: -len(dist)].rstrip()
            break

    text = MEMORY_SPEC_RE.sub(" ", text)

    manufacturer = None
    normalized = _strip_manufacturer_alias(text.strip())
    for mfr in MANUFACTURERS:
        if normalized.startswith(mfr):
            manufacturer = mfr
            text = normalized[len(mfr):].strip()
            break

    model_name = re.sub(r"\s+", " ", text).strip() or None

    return {
        "manufacturer": manufacturer,
        "chipset_maker": chipset_maker,
        "model_name": model_name,
        "distributor": distributor,
    }
