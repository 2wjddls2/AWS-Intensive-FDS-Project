"""
2차 Lambda (Slow Track) - AWS FDS PoC (팀원 C)
역할:
- SQS Trigger로 실행 (1차 Lambda가 Blocked 판정한 거래를 SQS로 전달)
- Amazon Bedrock(LLM) 호출 -> "왜 사기로 판단됐는지" 설명(Reason) 생성
- DynamoDB의 기존 Blocked 레코드에 Reason 텍스트 업데이트

확정 완료 (2026-08-13):
- BEDROCK_MODEL_ID : anthropic.claude-3-haiku-20240307-v1:0 로 확정.
  Claude 3 Haiku는 구형 모델이라 ap-northeast-2(서울)에 인리전으로 정식 지원됨
  (Claude 4/5 계열과 달리 global. 크로스리전 추론 프로파일 불필요).
- build_prompt() 내부 문구 : 팀 C 자체 확정 (A와 별도 협의 없이 진행하기로 함)

DynamoDB 테이블명은 1차 Lambda와 동일 (B 확정): team2-Fraud-Transactions

참고: SQS 이벤트 소스 매핑에서 "부분 배치 응답(ReportBatchItemFailures)"을 활성화해야
실패한 메시지만 재시도되고 성공한 메시지는 재처리되지 않음 (B가 트리거 연결할 때 같이 설정 필요).
"""
import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경변수
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME", "team2-Fraud-Transactions")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")

dynamodb = boto3.resource("dynamodb")
bedrock_runtime = boto3.client("bedrock-runtime")


def build_prompt(transaction: dict, fraud_probability: float) -> str:
    """팀 C 확정 프롬프트 (2026-08-13). 모든 필드 나열이 아니라 설득력 있는 근거 1~2개만
    골라 2문장 이내로 설명하도록 제약을 걸어, 모델이 데이터를 단순 반복하지 않게 함."""
    return (
        f"당신은 카드사의 이상거래 분석 시스템입니다. 아래 거래가 AI 모델에 의해 "
        f"사기 의심(확률 {fraud_probability:.4f})으로 차단되었습니다.\n"
        "거래 정보를 참고해, 이 거래가 왜 사기로 의심되는지 가장 설득력 있는 근거 1~2가지를 골라 "
        "2문장 이내의 한국어로 설명하세요. 모든 항목을 나열하지 말고, 위험 신호가 뚜렷한 항목 위주로 "
        "설명하세요.\n\n"
        "거래 정보:\n"
        f"- 금액: {transaction.get('amount')}\n"
        f"- 거래 시각: {transaction.get('transaction_hour')}시\n"
        f"- 해외 거래 여부: {transaction.get('foreign_transaction')}\n"
        f"- 위치 불일치 여부: {transaction.get('location_mismatch')}\n"
        f"- 기기 신뢰도 점수: {transaction.get('device_trust_score')}\n"
        f"- 최근 24시간 거래 횟수: {transaction.get('velocity_last_24h')}\n"
        f"- 카드 소유자 연령: {transaction.get('cardholder_age')}\n"
        f"- 가맹점 카테고리: {transaction.get('merchant_category')}\n"
    )


def get_fraud_reason(transaction: dict, fraud_probability: float) -> str:
    prompt = build_prompt(transaction, fraud_probability)
    resp = bedrock_runtime.converse(
        modelId=BEDROCK_MODEL_ID,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
    )
    return resp["output"]["message"]["content"][0]["text"].strip()


def handle_message(body: dict):
    transaction_id = body["transaction_id"]
    transaction = body["transaction"]
    fraud_probability = body["fraud_probability"]

    # Bedrock 호출: 1회 재시도 후 실패 시 예외 재발생 (SQS 재시도 정책에 맡김)
    try:
        reason = get_fraud_reason(transaction, fraud_probability)
    except Exception as e:
        logger.warning(f"[{transaction_id}] Bedrock 호출 1차 실패, 재시도: {e}")
        try:
            reason = get_fraud_reason(transaction, fraud_probability)
        except Exception as e2:
            logger.error(f"[{transaction_id}] Bedrock 호출 재시도도 실패: {e2}")
            raise

    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    table.update_item(
        Key={"transaction_id": transaction_id},
        UpdateExpression="SET reason = :r",
        ExpressionAttributeValues={":r": reason},
    )
    logger.info(f"[{transaction_id}] Reason 업데이트 완료: {reason[:60]}...")


def lambda_handler(event, context):
    """
    SQS 이벤트 소스 매핑에서 'ReportBatchItemFailures' 응답 타입을 활성화했다는 가정하에
    부분 배치 실패(batchItemFailures)를 반환 -- 실패한 메시지만 재시도되도록 함.
    (활성화 안 돼있으면 이 필드는 무시되고 기존처럼 동작하니 문제없음)
    """
    batch_item_failures = []
    for record in event["Records"]:
        message_id = record["messageId"]
        try:
            body = json.loads(record["body"])
            handle_message(body)
        except Exception as e:
            logger.error(f"메시지 처리 실패 (messageId={message_id}): {e}")
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}