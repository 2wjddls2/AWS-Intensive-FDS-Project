"""
1차 Lambda (Fast Track) - AWS FDS PoC (팀원 C)
역할:
- Kinesis Trigger로 실행
- SageMaker 엔드포인트 호출 → 사기 확률 예측 (A가 전달한 CSV 입력 규격 사용)
- 임계값 기준 Approved / Blocked 이진 분기
- Blocked: DynamoDB 기록 + SNS 알림 + SQS 전달(2차 Lambda로)

✅ 확정 완료 (2026-08-13) — 아래 값 전부 확정되어 기본값에 반영함:
- SAGEMAKER_ENDPOINT_NAME : fds-team2-endpoint (A 최종 확정)
- FRAUD_THRESHOLD         : 0.5 (A 확정)

Lambda 콘솔의 환경변수에도 동일한 값을 설정해두는 것을 권장 (여기 기본값은 로컬 테스트/설정 누락 시 폴백용).
"""
import base64
import json
import os
import logging
from datetime import datetime, timezone
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── 환경변수 (Lambda 콘솔에서 동일한 값으로 설정 권장. 기본값은 확정값으로 채워둠) ──
SAGEMAKER_ENDPOINT_NAME = os.environ.get("SAGEMAKER_ENDPOINT_NAME", "fds-team2-endpoint")
THRESHOLD = float(os.environ.get("FRAUD_THRESHOLD", "0.5"))
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME", "team2-Fraud-Transactions")
SNS_TOPIC_ARN = os.environ.get(
    "SNS_TOPIC_ARN", "arn:aws:sns:ap-northeast-2:054422645032:team2-fds-alert"
)
SQS_QUEUE_URL = os.environ.get(
    "SQS_QUEUE_URL",
    "https://sqs.ap-northeast-2.amazonaws.com/054422645032/team2-fds-slow-track-queue",
)

sagemaker_runtime = boto3.client("sagemaker-runtime")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
sqs = boto3.client("sqs")

CATEGORIES = ["Clothing", "Electronics", "Food", "Grocery", "Travel"]  # A가 지정한 고정 순서


def get_fraud_probability(t: dict) -> float:
    """A가 전달한 규격 그대로: CSV(text/csv), 헤더 없음, 12개 컬럼 고정 순서."""
    row = [
        t["amount"], t["transaction_hour"], t["foreign_transaction"],
        t["location_mismatch"], t["device_trust_score"],
        t["velocity_last_24h"], t["cardholder_age"],
    ]
    row += [1 if t["merchant_category"] == c else 0 for c in CATEGORIES]
    body = ",".join(str(v) for v in row)
    resp = sagemaker_runtime.invoke_endpoint(
        EndpointName=SAGEMAKER_ENDPOINT_NAME,
        ContentType="text/csv",
        Body=body,
    )
    return float(resp["Body"].read().decode().strip())  # 0~1 사기 확률


def handle_transaction(transaction: dict):
    transaction_id = transaction["transaction_id"]

    # ── SageMaker 호출: 1회 재시도 후 실패 시 로그 남기고 예외 재발생 ──
    try:
        probability = get_fraud_probability(transaction)
    except Exception as e:
        logger.warning(f"[{transaction_id}] SageMaker 호출 1차 실패, 재시도: {e}")
        try:
            probability = get_fraud_probability(transaction)
        except Exception as e2:
            logger.error(f"[{transaction_id}] SageMaker 호출 재시도도 실패: {e2}")
            raise  # Kinesis 트리거 기본 재시도 정책에 맡김

    now_iso = datetime.now(timezone.utc).isoformat()
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    if probability >= THRESHOLD:
        # ── 위험(Blocked) 분기 ──
        table.put_item(Item={
            "transaction_id": transaction_id,
            "status": "Blocked",
            "fraud_probability": str(probability),  # Decimal 이슈 회피용 문자열 저장
            "updated_at": now_iso,
        })
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="[FDS] 사기 의심 거래 탐지",
            Message=f"거래 {transaction_id} 사기 확률 {probability:.2f}로 차단(Blocked) 처리됨.",
        )
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({
                "transaction_id": transaction_id,
                "transaction": transaction,
                "fraud_probability": probability,
            }),
        )
        logger.info(f"[{transaction_id}] Blocked (prob={probability:.4f})")
    else:
        # ── 정상(Approved) 분기 ──
        table.put_item(Item={
            "transaction_id": transaction_id,
            "status": "Approved",
            "fraud_probability": str(probability),
            "updated_at": now_iso,
        })
        logger.info(f"[{transaction_id}] Approved (prob={probability:.4f})")


def lambda_handler(event, context):
    for record in event["Records"]:
        raw = base64.b64decode(record["kinesis"]["data"])
        transaction = json.loads(raw)
        try:
            handle_transaction(transaction)
        except Exception as e:
            # 개별 레코드 실패가 배치 전체를 막지 않도록 로그만 남기고 계속 진행.
            # (실패 건을 반드시 재시도시키고 싶으면 이 try/except를 제거해서 예외를 밖으로 올리면 됨)
            logger.error(f"레코드 처리 실패: {e} / record={record}")
    return {"statusCode": 200}