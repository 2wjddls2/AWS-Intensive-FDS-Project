"""
2차 Lambda (Slow Track) - AWS FDS PoC (팀원 C)

⚠️ Phase 6 아키텍처 개편 (2026-08-14): 트리거가 SQS → Step Functions로 변경됨.
   B가 SQS Event Source Mapping을 비활성화하고, Step Functions 상태머신의
   "중간(Medium)" 분기에서 이 Lambda를 Task로 직접 동기 호출하도록 재구성.
   1차 Lambda(fast_track_router_lambda.py)가 만든 원본 페이로드가 Step Functions를
   거쳐 이 Lambda의 event로 그대로 들어옴 (SQS body/Records 래퍼 없음, 거래 1건 = event 1개).

역할:
- Step Functions Task로 실행 (중간 위험도로 분류된 거래를 넘겨받음)
- Amazon Bedrock(LLM) 호출 -> "왜 사기로 의심되는지" 설명(Reason) 생성
- 생성한 reason을 반환 -> DynamoDB 기록은 Step Functions의 다음 태스크(B 설계)가 담당
  (⚠️ 이 Lambda는 더 이상 DynamoDB에 직접 쓰지 않음 — 아키텍처 개편으로 책임 분리.
   B의 상태머신에 "이 Lambda 다음에 DynamoDB 저장 태스크"가 실제로 있는지 확인 필요.
   없다면 이 파일에 dynamodb.update_item() 호출을 다시 추가해야 함.)

확정 완료 (2026-08-13):
- BEDROCK_MODEL_ID : anthropic.claude-3-haiku-20240307-v1:0 로 확정.
  Claude 3 Haiku는 구형 모델이라 ap-northeast-2(서울)에 인리전으로 정식 지원됨
  (Claude 4/5 계열과 달리 global. 크로스리전 추론 프로파일 불필요).

TODO (아직 확정 안 된 값):
- build_prompt() 내부 문구 : A와 협의 후 교체 예정 (지금은 임시 초안)

입력 event 계약 (1차 Lambda·Step Functions와 합의한 구조, SQS 래퍼 없음):
  {
    "transaction_id": "...",
    "transaction": {...원본 거래 필드 전부...},
    "fraud_score": 0.xx,
    "scored_at": "..."
  }

반환값: {"reason": "..."} — Step Functions ResultPath를 통해 다음 태스크로 전달됨.
"""

import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경변수
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")

bedrock_runtime = boto3.client("bedrock-runtime")


def build_prompt(transaction: dict, fraud_probability: float) -> str:
    """TODO: A와 협의한 프롬프트로 교체 예정. 지금은 거래 필드를 나열하는 임시 초안. (변경 없음)"""
    return (
        "다음은 사기 탐지 시스템이 '사기 의심(Blocked)'으로 분류한 거래입니다. "
        "아래 거래 정보를 근거로 왜 이 거래가 사기로 의심되는지 2~3문장으로 한국어로 설명해주세요.\n\n"
        f"- 거래 ID: {transaction.get('transaction_id')}\n"
        f"- 금액: {transaction.get('amount')}\n"
        f"- 거래 시각(시): {transaction.get('transaction_hour')}\n"
        f"- 해외 거래 여부: {transaction.get('foreign_transaction')}\n"
        f"- 위치 불일치 여부: {transaction.get('location_mismatch')}\n"
        f"- 기기 신뢰도 점수: {transaction.get('device_trust_score')}\n"
        f"- 최근 24시간 거래 빈도: {transaction.get('velocity_last_24h')}\n"
        f"- 카드 소유자 연령: {transaction.get('cardholder_age')}\n"
        f"- 가맹점 카테고리: {transaction.get('merchant_category')}\n"
        f"- 모델 사기 확률: {fraud_probability:.4f}\n"
    )


def get_fraud_reason(transaction: dict, fraud_probability: float) -> str:
    """(변경 없음)"""
    prompt = build_prompt(transaction, fraud_probability)
    resp = bedrock_runtime.converse(
        modelId=BEDROCK_MODEL_ID,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
    )
    return resp["output"]["message"]["content"][0]["text"].strip()


def lambda_handler(event, context):
    """Step Functions Task로 동기 호출됨 (SQS 배치 아님 — event 자체가 거래 1건의 페이로드).

    기존 SQS 버전의 event["Records"] 루프, messageId, batchItemFailures는
    전부 제거 — 더 이상 SQS 이벤트 소스가 아니므로 필요 없음.
    """
    transaction_id = event["transaction_id"]
    transaction = event["transaction"]
    fraud_probability = event["fraud_score"]  # ⚠️ Step Functions 입력 필드명은 fraud_score (Choice 상태와 통일)

    # Bedrock 호출: 1회 재시도 후 실패 시 예외 재발생
    # (Step Functions의 Retry/Catch 설정에 맡김 — 필요하면 B가 상태머신에서 재시도 정책 추가)
    try:
        reason = get_fraud_reason(transaction, fraud_probability)
    except Exception as e:
        logger.warning(f"[{transaction_id}] Bedrock 호출 1차 실패, 재시도: {e}")
        try:
            reason = get_fraud_reason(transaction, fraud_probability)
        except Exception as e2:
            logger.error(f"[{transaction_id}] Bedrock 호출 재시도도 실패: {e2}")
            raise

    logger.info(f"[{transaction_id}] Reason 생성 완료: {reason[:60]}...")

    return {"reason": reason}