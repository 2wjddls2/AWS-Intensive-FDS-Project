"""
build_simulator_dataset.py

목적:
  팀원 A가 SageMaker 학습 시 사용한 S3 홀드아웃 test.csv
  (1,500행 x 13열, 헤더 없음, SageMaker XGBoost 학습 포맷:
   [is_fraud, amount, transaction_hour, foreign_transaction, location_mismatch,
    device_trust_score, velocity_last_24h, cardholder_age,
    merchant_category_Clothing, merchant_category_Electronics,
    merchant_category_Food, merchant_category_Grocery, merchant_category_Travel])
  를 우리 파이프라인이 쓰는 원본 스키마
   (transaction_id, amount, transaction_hour, merchant_category,
    foreign_transaction, location_mismatch, device_trust_score,
    velocity_last_24h, cardholder_age, is_fraud)
  로 복원하고, 사기:정상 = 1:2 비율로 리샘플링하여
  Phase 2 시뮬레이터가 그대로 읽어 1초에 1건씩 전송할 수 있는
  CSV / JSON 데이터셋을 생성한다.

입력: test.csv (헤더 없음, 13열, SageMaker 학습 포맷)
출력:
  - fds_simulator_dataset.csv  (원본 스키마, 전송 순서로 셔플됨)
  - fds_simulator_dataset.json (동일 데이터, JSON 배열, 전송 순서 동일)

transaction_id 부여 규칙 (기존 test-fraud-001 등과 동일한 명명 규칙 유지):
  - 사기(is_fraud=1): test.csv 등장 순서 기준 test-fraud-001 ~ test-fraud-022
  - 정상(is_fraud=0): 샘플링된 순서 기준 test-normal-001 ~ test-normal-044
  (전송 순서 자체는 별도로 셔플하되, ID는 원본 등장/샘플 순서를 보존해 추후
   test.csv 원본 행과 매핑 추적이 가능하도록 한다.)
"""

import argparse
import json
import random

import pandas as pd

SAGEMAKER_COLS = [
    "is_fraud",
    "amount",
    "transaction_hour",
    "foreign_transaction",
    "location_mismatch",
    "device_trust_score",
    "velocity_last_24h",
    "cardholder_age",
    "merchant_category_Clothing",
    "merchant_category_Electronics",
    "merchant_category_Food",
    "merchant_category_Grocery",
    "merchant_category_Travel",
]

CATEGORY_ORDER = ["Clothing", "Electronics", "Food", "Grocery", "Travel"]

OUTPUT_SCHEMA = [
    "transaction_id",
    "amount",
    "transaction_hour",
    "merchant_category",
    "foreign_transaction",
    "location_mismatch",
    "device_trust_score",
    "velocity_last_24h",
    "cardholder_age",
    "is_fraud",
]


def decode_merchant_category(row: pd.Series) -> str:
    onehot_cols = [f"merchant_category_{c}" for c in CATEGORY_ORDER]
    active = [c for c, col in zip(CATEGORY_ORDER, onehot_cols) if row[col] == 1]
    if len(active) != 1:
        # 학습 때 없던 값(5칸 전부 0) 등 예외 케이스 방어
        raise ValueError(f"merchant_category one-hot 이상: {row[onehot_cols].to_dict()}")
    return active[0]


def load_and_decode(test_csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(test_csv_path, header=None, names=SAGEMAKER_COLS)
    df["merchant_category"] = df.apply(decode_merchant_category, axis=1)
    return df


def build_dataset(
    df: pd.DataFrame,
    fraud_count: int,
    normal_count: int,
    seed: int,
) -> pd.DataFrame:
    fraud_df = df[df["is_fraud"] == 1].reset_index(drop=True)
    normal_df = df[df["is_fraud"] == 0].reset_index(drop=True)

    if fraud_count > len(fraud_df):
        raise ValueError(
            f"요청한 사기 건수({fraud_count})가 test.csv의 사기 건수({len(fraud_df)})보다 많습니다."
        )
    if normal_count > len(normal_df):
        raise ValueError(
            f"요청한 정상 건수({normal_count})가 test.csv의 정상 건수({len(normal_df)})보다 많습니다."
        )

    rng = random.Random(seed)

    # 사기: test.csv 등장 순서 그대로 앞에서부터 fraud_count건 사용 (기존 test-fraud-001 규칙과 호환)
    fraud_sample = fraud_df.iloc[:fraud_count].copy()
    fraud_sample["transaction_id"] = [f"test-fraud-{i+1:03d}" for i in range(len(fraud_sample))]

    # 정상: 전체 정상 풀에서 무작위 비복원추출
    normal_idx = rng.sample(range(len(normal_df)), normal_count)
    normal_sample = normal_df.iloc[normal_idx].reset_index(drop=True).copy()
    normal_sample["transaction_id"] = [f"test-normal-{i+1:03d}" for i in range(len(normal_sample))]

    combined = pd.concat([fraud_sample, normal_sample], ignore_index=True)

    # 전송 순서 셔플 (사기 케이스가 한쪽에 몰리지 않도록)
    shuffled = combined.sample(frac=1, random_state=seed).reset_index(drop=True)

    return shuffled[OUTPUT_SCHEMA]


def main():
    parser = argparse.ArgumentParser(description="시뮬레이터용 사기/정상 리샘플링 데이터셋 생성")
    parser.add_argument("--input", default="test.csv", help="원본 test.csv 경로")
    parser.add_argument("--fraud-count", type=int, default=22, help="사용할 사기 건수")
    parser.add_argument("--normal-count", type=int, default=44, help="사용할 정상 건수 (기본 1:2 비율)")
    parser.add_argument("--seed", type=int, default=42, help="샘플링/셔플 시드")
    parser.add_argument("--csv-out", default="fds_simulator_dataset.csv")
    parser.add_argument("--json-out", default="fds_simulator_dataset.json")
    args = parser.parse_args()

    df = load_and_decode(args.input)
    dataset = build_dataset(df, args.fraud_count, args.normal_count, args.seed)

    dataset.to_csv(args.csv_out, index=False)

    records = dataset.to_dict(orient="records")
    with open(args.json_out, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"[OK] 총 {len(dataset)}건 생성 (사기 {args.fraud_count} : 정상 {args.normal_count})")
    print(f"[OK] CSV  -> {args.csv_out}")
    print(f"[OK] JSON -> {args.json_out}")
    print("\n--- 검증 ---")
    print("is_fraud 분포:\n", dataset["is_fraud"].value_counts())
    print("transaction_id 중복 여부:", dataset["transaction_id"].duplicated().any())
    print("merchant_category 분포:\n", dataset["merchant_category"].value_counts())
    print("\n앞 5건 미리보기:")
    print(dataset.head(5).to_string(index=False))


if __name__ == "__main__":
    main()