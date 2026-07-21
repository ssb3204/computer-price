"""정규식 파서(A) vs Ollama LLM 파서(B)의 GPU 제품명 분해 정확도를 비교 측정한다.

지표:
    - 필드별 정확도: 4개 필드 각각 정답과 정확히 일치하는 비율
    - 전체 4필드 완전 일치율: manufacturer/chipset_maker/model_name/distributor 모두 일치
    - 필드 누락률: 정답이 값을 가진 행 중 예측이 null인 비율
    - 환각률: 정답이 null인 행 중 예측이 값을 만들어낸(non-null) 비율

사용법:
    python scripts/eval_gpu_parsers.py
"""

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from parser.regex_parser import parse_gpu_name as parse_regex
from parser.llm_parser import parse_gpu_name as parse_llm

FIELDS = ["manufacturer", "chipset_maker", "model_name", "distributor"]
DATA_PATH = ROOT / "data" / "labeled_gpu_names_v2.csv"
CONSISTENCY_SAMPLE_SIZE = 10
CONSISTENCY_RUNS = 3


def load_dataset() -> list[dict]:
    with open(DATA_PATH, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        for field in FIELDS:
            row[field] = row[field] or None
    return rows


def normalize(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value if value else None


def evaluate(rows: list[dict], parse_fn) -> dict:
    field_correct = {f: 0 for f in FIELDS}
    field_total = {f: 0 for f in FIELDS}
    omission_count = {f: 0 for f in FIELDS}
    omission_total = {f: 0 for f in FIELDS}
    hallucination_count = {f: 0 for f in FIELDS}
    hallucination_total = {f: 0 for f in FIELDS}
    full_match = 0

    for row in rows:
        pred = parse_fn(row["product_name"])
        row_all_correct = True
        for field in FIELDS:
            truth = normalize(row[field])
            predicted = normalize(pred.get(field))

            field_total[field] += 1
            is_correct = predicted == truth
            if is_correct:
                field_correct[field] += 1
            else:
                row_all_correct = False

            if truth is not None:
                omission_total[field] += 1
                if predicted is None:
                    omission_count[field] += 1
            else:
                hallucination_total[field] += 1
                if predicted is not None:
                    hallucination_count[field] += 1

        if row_all_correct:
            full_match += 1

    def rate(count, total):
        return count / total if total else None

    return {
        "field_accuracy": {f: rate(field_correct[f], field_total[f]) for f in FIELDS},
        "full_match_rate": full_match / len(rows),
        "omission_rate": {f: rate(omission_count[f], omission_total[f]) for f in FIELDS},
        "hallucination_rate": {f: rate(hallucination_count[f], hallucination_total[f]) for f in FIELDS},
    }


def fmt_pct(value) -> str:
    return f"{value * 100:.1f}%" if value is not None else "N/A"


def print_report(name_a: str, result_a: dict, name_b: str, result_b: dict) -> None:
    print(f"\n=== 필드별 정확도 ===")
    print(f"{'필드':<16}{name_a:>14}{name_b:>14}")
    for field in FIELDS:
        print(f"{field:<16}{fmt_pct(result_a['field_accuracy'][field]):>14}{fmt_pct(result_b['field_accuracy'][field]):>14}")

    print(f"\n=== 전체 4필드 완전 일치율 ===")
    print(f"{name_a:<16}{fmt_pct(result_a['full_match_rate'])}")
    print(f"{name_b:<16}{fmt_pct(result_b['full_match_rate'])}")

    print(f"\n=== 필드 누락률 (정답 有 -> 예측 null) ===")
    print(f"{'필드':<16}{name_a:>14}{name_b:>14}")
    for field in FIELDS:
        print(f"{field:<16}{fmt_pct(result_a['omission_rate'][field]):>14}{fmt_pct(result_b['omission_rate'][field]):>14}")

    print(f"\n=== 환각률 (정답 null -> 예측 값 생성) ===")
    print(f"{'필드':<16}{name_a:>14}{name_b:>14}")
    for field in FIELDS:
        print(f"{field:<16}{fmt_pct(result_a['hallucination_rate'][field]):>14}{fmt_pct(result_b['hallucination_rate'][field]):>14}")


def sample_for_consistency(rows: list[dict], n: int) -> list[dict]:
    step = max(1, len(rows) // n)
    return rows[::step][:n]


def measure_consistency(rows: list[dict], parse_fn, runs: int) -> dict:
    field_agree = {f: 0 for f in FIELDS}
    full_agree = 0

    for row in rows:
        outputs = [parse_fn(row["product_name"]) for _ in range(runs)]
        row_all_agree = True
        for field in FIELDS:
            values = {normalize(o.get(field)) for o in outputs}
            if len(values) == 1:
                field_agree[field] += 1
            else:
                row_all_agree = False
        if row_all_agree:
            full_agree += 1

    n = len(rows)
    return {
        "field_consistency": {f: field_agree[f] / n for f in FIELDS},
        "full_consistency": full_agree / n,
    }


def print_consistency_report(name: str, result: dict, n: int, runs: int) -> None:
    print(f"\n=== {name} 일관성 ({n}건 x {runs}회 반복, 동일 입력에 동일 출력 비율) ===")
    for field in FIELDS:
        print(f"{field:<16}{fmt_pct(result['field_consistency'][field]):>14}")
    print(f"{'전체 4필드 일치':<16}{fmt_pct(result['full_consistency']):>14}")


def main() -> None:
    rows = load_dataset()
    print(f"평가셋: {len(rows)}건 ({DATA_PATH})")

    print("\n[A] 정규식 파서 평가 중...")
    result_regex = evaluate(rows, parse_regex)

    print("[B] Ollama qwen2.5:7b 파서 평가 중 (행마다 추론 호출, 시간 소요)...")
    result_llm = evaluate(rows, parse_llm)

    print_report("정규식(A)", result_regex, "LLM(B)", result_llm)

    sample = sample_for_consistency(rows, CONSISTENCY_SAMPLE_SIZE)
    print(f"\n[C] LLM 일관성 측정 중 ({len(sample)}건 x {CONSISTENCY_RUNS}회)...")
    llm_consistency = measure_consistency(sample, parse_llm, CONSISTENCY_RUNS)
    print_consistency_report("LLM(B)", llm_consistency, len(sample), CONSISTENCY_RUNS)


if __name__ == "__main__":
    main()
