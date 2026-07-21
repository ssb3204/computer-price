"""GPU 제품명을 4개 필드로 분해하는 Ollama qwen2.5:7b 기반 LLM 파서."""

import json
import re

import requests

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "qwen2.5:7b"

PROMPT_TEMPLATE = """다음은 한국 쇼핑몰에서 크롤링한 그래픽카드(GPU) 제품명이다.
이 제품명을 아래 4개 필드로 분해하라.

- manufacturer: 보드 제조사 (예: MSI, ASUS, GIGABYTE, ZOTAC GAMING, SAPPHIRE, 이엠텍 등)
- chipset_maker: GPU 칩셋 제조사. "지포스"가 있으면 NVIDIA, "라데온"이 있으면 AMD
- model_name: 칩셋 모델명 + 보드 변형명 (제조사명, 메모리 용량 스펙, 유통사명은 제외)
- distributor: 국내 유통사명 (없으면 null)

해당 정보가 제품명에 없으면 그 필드는 null로 응답하라. 제품명에 없는 값을 지어내지 마라.
반드시 아래 JSON 형식으로만 응답하라. 다른 설명은 추가하지 마라.

{{"manufacturer": "...", "chipset_maker": "...", "model_name": "...", "distributor": "..."}}

제품명: {product_name}
"""


def _extract_json(text: str) -> dict | None:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def _normalize(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "" or value.lower() == "null":
            return None
        return value
    return None


def parse_gpu_name(product_name: str, timeout: int = 60) -> dict:
    """Ollama qwen2.5:7b로 GPU 제품명을 4개 필드로 분해한다.

    Returns:
        {"manufacturer": str|None, "chipset_maker": str|None,
         "model_name": str|None, "distributor": str|None}
    """
    empty = {"manufacturer": None, "chipset_maker": None, "model_name": None, "distributor": None}
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL,
                "prompt": PROMPT_TEMPLATE.format(product_name=product_name),
                "stream": False,
                "options": {"temperature": 0},
            },
            timeout=timeout,
        )
        resp.raise_for_status()
    except requests.RequestException:
        return empty

    raw_text = resp.json().get("response", "")
    parsed = _extract_json(raw_text)
    if parsed is None:
        return empty

    return {
        "manufacturer": _normalize(parsed.get("manufacturer")),
        "chipset_maker": _normalize(parsed.get("chipset_maker")),
        "model_name": _normalize(parsed.get("model_name")),
        "distributor": _normalize(parsed.get("distributor")),
    }
