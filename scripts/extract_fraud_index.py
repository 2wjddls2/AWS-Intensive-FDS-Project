import pandas as pd

df = pd.read_csv("credit_card_fraud_10k.csv")

fraud_df = df[df["is_fraud"] == 1] # 사기인 것만 추출
fraud_indices = fraud_df.index.tolist()

print(f"사기 건수: {len(fraud_indices)}건")

# Phase 5 테스트용으로 저장
fraud_df.to_csv("fraud_cases_for_test.csv", index=True)

with open("fraud_row_indices.txt", "w") as f:
    f.write(",".join(map(str, fraud_indices)))

print("저장 완료: fraud_cases_for_test.csv, fraud_row_indices.txt")