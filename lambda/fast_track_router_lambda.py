"""
1차 Lambda (Router) - AWS FDS PoC (팀원 C) — Phase 6 Step Functions 연동판

⚠️ 이 파일은 기존 team2-fds-fasttrack-lambda 코드를 대체하는 새 버전입니다.
   (기존 버전은 SQS 전송 + SNS 발행 + DynamoDB PutItem까지 이 Lambda 안에서 전부 처리했음)

역할 변경 (Phase 6 아키텍처 개편 — B가 설계하는 Step Functions 3분기 구조로 이관):
- Kinesis Trigger로 실행 → SageMaker 호출해서 사기 확률만 구함
- 원본 거래 데이터 + 확률값을 하나의 JSON으로 묶어 Step Functions 상태머신
  실행(start_execution)을 시작시키고 종료.
- 낮음/중간/높음 3단계 분기, DynamoDB 기록, SNS/Bedrock 호출은 전부
  Step Functions(B) + 상태머신 내 태스크 책임 — 이 Lambda는 관여하지 않음.

⚠️ 아직 채워야 할 값 (TODO):
- STATE_MACHINE_ARN : B가 Step Functions 상태머신 생성 후 ARN 전달
- team2-fds-lambda-role에 states:StartExecution 권한 추가 필요
  (Resource: 아래 상태머신 ARN, B의 "IAM 통합 권한 부여" 작업과 별개로 이 Lambda 쪽에도 필요)
"""

import base64
import json
import os
import logging
import uuid
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── 환경변수 ──
SAGEMAKER_ENDPOINT_NAME = os.environ.get("SAGEMAKER_ENDPOINT_NAME", "fds-team2-endpoint")
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "arn:aws:states:ap-northeast-2:054422645032:stateMachine:team2-fds-state-machine")

sagemaker_runtime = boto3.client("sagemaker-runtime")
sfn = boto3.client("stepfunctions")

CATEGORIES = ["Clothing", "Electronics", "Food", "Grocery", "Travel"]  # A가 지정한 고정 순서


def get_fraud_probability(t: dict) -> float:
    """A가 전달한 규격 그대로: CSV(text/csv), 헤더 없음, 12개 컬럼 고정 순서. (변경 없음)"""
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


def start_step_functions(transaction: dict, probability: float) -> str:
    """Kinesis 원본 거래 + SageMaker 점수를 하나의 JSON으로 묶어 Step Functions 실행 시작.

    B/A와 합의한 입력 계약:
      {
        "transaction_id": str,
        "transaction": {...원본 거래 필드 전부...},
        "fraud_probability": float,
        "scored_at": ISO8601 UTC 문자열
      }
    """
    transaction_id = transaction["transaction_id"]

    sfn_input = {
        "transaction_id": transaction_id,
        "transaction": transaction,
        "fraud_probability": probability,
        "scored_at": datetime.now(timezone.utc).isoformat(),
    }

    # Step Functions 실행 이름은 상태머신 내 고유해야 하므로 transaction_id + 짧은 랜덤 suffix 사용
    # (재전송/재시도 시 이름 충돌 방지, 80자 제한 내)
    execution_name = f"{transaction_id}-{uuid.uuid4().hex[:8]}"[:80]

    response = sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=execution_name,
        input=json.dumps(sfn_input, ensure_ascii=False),
    )
    return response["executionArn"]


def handle_transaction(transaction: dict):
    transaction_id = transaction["transaction_id"]

    # ── SageMaker 호출: 1회 재시도 후 실패 시 로그 남기고 예외 재발생 (기존과 동일) ──
    try:
        probability = get_fraud_probability(transaction)
    except Exception as e:
        logger.warning(f"[{transaction_id}] SageMaker 호출 1차 실패, 재시도: {e}")
        try:
            probability = get_fraud_probability(transaction)
        except Exception as e2:
            logger.error(f"[{transaction_id}] SageMaker 호출 재시도도 실패: {e2}")
            raise  # Kinesis 트리거 기본 재시도 정책에 맡김

    execution_arn = start_step_functions(transaction, probability)

    logger.info(
        f"[{transaction_id}] Step Functions 실행 시작 (prob={probability:.6f}) "
        f"executionArn={execution_arn}"
    )


def lambda_handler(event, context):
    for record in event["Records"]:
        raw = base64.b64decode(record["kinesis"]["data"])
        transaction = json.loads(raw)

        try:
            handle_transaction(transaction)
        except Exception as e:
            # 개별 레코드 실패가 배치 전체를 막지 않도록 로그만 남기고 계속 진행. (기존과 동일)
            logger.error(f"레코드 처리 실패: {e} / record={record}")

    return {"statusCode": 200}