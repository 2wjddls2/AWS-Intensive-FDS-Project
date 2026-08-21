"""
score_tier_candidates.py

목적: tier_test_candidates.json(정상 20 + 중간 후보 20 + 사기 20, 총 60건)을
실제 SageMaker 엔드포인트로 스코어링해서, 특히 "중간(Medium)" 합성 후보들이
실제로 0.3~0.7 구간(현재 확정된 임계값 기준)에 들어오는지 검증한다.

실행 환경: 로컬 PC (conda fds_env)

사용법:
  python score_tier_candidates.py --input tier_test_candidates.json --output tier_test_scored.csv
"""

import argparse
import csv
import json

import boto3

CATEGORIES = ["Clothing", "Electronics", "Food", "Grocery", "Travel"]

# 현재 Step Functions Choice 상태 기준 (FraudScoreChoice)
LOW_MAX = 0.3      # <= 0.3 : Low
HIGH_MIN = 0.7      # >= 0.7 : High (그 사이는 Medium)


def score(runtime, endpoint_name, r):
    row = [
        r["amount"], r["transaction_hour"], r["foreign_transaction"],
        r["location_mismatch"], r["device_trust_score"], r["velocity_last_24h"], r["cardholder_age"],
    ]
    row += [1 if r["merchant_category"] == c else 0 for c in CATEGORIES]
    body = ",".join(str(v) for v in row)
    resp = runtime.invoke_endpoint(EndpointName=endpoint_name, ContentType="text/csv", Body=body)
    return float(resp["Body"].read().decode().strip())


def actual_tier(p):
    if p <= LOW_MAX:
        return "LOW"
    if p >= HIGH_MIN:
        return "HIGH"
    return "MEDIUM"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="tier_test_candidates.json")
    parser.add_argument("--output", default="tier_test_scored.csv")
    parser.add_argument("--endpoint", default="fds-team2-endpoint")
    parser.add_argument("--region", default="ap-northeast-2")
    args = parser.parse_args()

    runtime = boto3.client("sagemaker-runtime", region_name=args.region)

    with open(args.input, encoding="utf-8") as f:
        records = json.load(f)

    results = []
    for r in records:
        try:
            p = score(runtime, args.endpoint, r)
        except Exception as e:
            print(f"[{r['transaction_id']}] 스코어링 실패: {e}")
            continue
        tier = actual_tier(p)
        results.append({**r, "actual_probability": p, "actual_tier": tier})

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        fieldnames = list(results[0].keys())
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(results)

    print(f"저장 완료: {args.output}\n")

    # transaction_id 접두사(test-low-/test-medium-/test-high-)로 기대 티어를 판정
    # (expected_tier 문자열은 후보 세대마다 부가 설명이 달라질 수 있어 접두사 기준이 더 안전함)
    prefix_map = {"test-low-": "LOW", "test-medium-": "MEDIUM", "test-high-": "HIGH"}
    for prefix, want_label in prefix_map.items():
        group = [r for r in results if r["transaction_id"].startswith(prefix)]
        if not group:
            continue
        hit = sum(1 for r in group if r["actual_tier"] == want_label)
        print(f"[{want_label}] {hit}/{len(group)}건이 실제로도 {want_label} 분기에 들어옴")
        misses = [r for r in group if r["actual_tier"] != want_label]
        if misses:
            print("  기대와 다르게 나온 건:")
            for r in misses:
                print(f"    {r['transaction_id']}: 실제 확률={r['actual_probability']:.4f} -> {r['actual_tier']}")
        print()

    medium_misses = [
        r for r in results
        if r["transaction_id"].startswith("test-medium-") and r["actual_tier"] != "MEDIUM"
    ]
    if medium_misses:
        print(f"⚠️ Medium 후보 중 {len(medium_misses)}건이 목표 구간을 벗어남 — 결과를 Claude에게 다시 보내주면 조정해서 대체 후보 만들어줄게.")
    else:
        print("✅ Medium 후보 20건 전부 목표 구간(0.3~0.7) 안에 들어옴.")


if __name__ == "__main__":
    main()