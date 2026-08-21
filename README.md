# AWS-Intensive-FDS-Project

##역할분담
<img width="1304" height="1496" alt="image" src="https://github.com/user-attachments/assets/06151798-a3fc-4e1c-89b6-d50766962a96" />

## 프로젝트 개요

AWS 환경에서 구축한 실시간 하이브리드 이상거래 탐지 시스템(FDS) 파이프라인 PoC입니다. 통계 기반 모델로 즉시 판단하는 **Fast Track**과, LLM으로 판단 근거를 설명하는 **Slow Track**을 분리한 티어링 아키텍처로 설계했습니다.

- **데이터셋**: `credit_card_fraud_10k.csv` (1만 건, 결측치·중복 없음, 사기 비율 1.51%)
- **리전**: `ap-northeast-2` (서울)

## 아키텍처

Kinesis로 유입된 거래 데이터는 1차 Lambda에서 SageMaker 모델로 사기 확률만 계산되고, 이후의 모든 분기·LLM 호출·알림·저장 로직은 Step Functions 상태머신이 전담합니다. 사기 확률에 따라 낮음/중간/높음 3단계로 분기됩니다.

```
[시뮬레이터] → Kinesis Data Streams
                    │
                    ▼
         1차 Lambda (확률 계산 전용)
         SageMaker 엔드포인트 호출 → Step Functions 실행
                    │
                    ▼
       Step Functions — Choice (3단계 분기)
   ┌────────────────┼────────────────────┐
   ▼                ▼                     ▼
  낮음              중간                   높음
score ≤ 0.3    0.3 < score ≤ 0.7      score > 0.7
   │                │                     │
   │           2차 Lambda 호출          2차 Lambda 호출
   │          (Bedrock 사유 생성)       (Bedrock 사유 생성)
   │                │                     │
   │            SNS 알림                  │
   │           (담당자 통보)               │
   ▼                ▼                     ▼
DynamoDB         DynamoDB               DynamoDB
Approved         Review + reason        Blocked + reason
```

| 분기 | 조건 | 동작 | DynamoDB status |
|---|---|---|---|
| 낮음 | `fraud_probability ≤ 0.3` | DynamoDB 기록만 (LLM/알림 없음) | `Approved` |
| 중간 | `0.3 < fraud_probability ≤ 0.7` | Bedrock 사유 생성 → SNS 알림 → DynamoDB 기록 | `Review` |
| 높음 | `fraud_probability > 0.7` | Bedrock 사유 생성 → DynamoDB 기록 (SNS 없음, 즉시 차단) | `Blocked` |

## 사용 기술 스택

- **Amazon Kinesis Data Streams**: 실시간 거래 데이터 수집
- **AWS Lambda**: Fast Track(확률 계산·라우팅), Slow Track(LLM 사유 생성)
- **AWS Step Functions**: 3단계 분기 상태머신
- **Amazon SageMaker**: XGBoost 기반 사기 확률 예측 모델
- **Amazon Bedrock**: `anthropic.claude-3-haiku-20240307-v1:0` — 판단 사유 자연어 생성
- **Amazon DynamoDB**: 거래별 판정 결과 저장
- **Amazon SNS**: 중간(Review) 분기 담당자 알림
- **Amazon SQS**: 초기 이진 분기 아키텍처에서 사용(현재는 Step Functions 직접 호출로 대체)

## SageMaker 모델 입출력 규격

- **입력**: `text/csv`, 헤더 없음, 12개 컬럼 고정 순서 — `amount, transaction_hour, foreign_transaction, location_mismatch, device_trust_score, velocity_last_24h, cardholder_age` + `merchant_category` 원핫 인코딩 5칸(`Clothing, Electronics, Food, Grocery, Travel`)
- **출력**: 사기 확률(0~1) 단일 float 값

## 분기 임계값

3단계 분기 임계값은 **0.3 / 0.7**로 확정했습니다. 실제 SageMaker 엔드포인트로 LOW/MEDIUM/HIGH 후보 60건을 스코어링해 임계값 경계 검증을 진행했습니다 (`simulator/score_tier_candidates.py`, 결과: `simulator/tier_test_scored_v2.csv`).

## 폴더 구조

```
├── lambda/          # 1차·2차 Lambda 함수 코드
├── simulator/        # Kinesis 전송 시뮬레이터 및 테스트 데이터셋
├── data/              # 원본/파생 데이터셋
├── scripts/           # 데이터 전처리·추출용 보조 스크립트
```

## 시뮬레이터 실행 방법

```bash
conda activate fds_env
python simulator/simulate_kinesis_stream.py --interval 1
```
`--interval` 값으로 전송 간격(초)을 조정할 수 있습니다.

## 트러블슈팅 기록

- 1차/2차 Lambda 코드가 서로 뒤바뀌어 배포된 적이 있어, 코드 수정 후에는 파일 상단 docstring으로 배포된 코드가 맞는지 항상 확인합니다.
- Lambda 실행 역할에 `sns:Publish`, `sqs:SendMessage`, `bedrock:InvokeModel` 권한이 없어 발생한 `AuthorizationError`를 IAM 정책 추가로 해결했습니다.
- 로컬에서 Kinesis로 직접 전송하는 IAM 사용자에는 `kinesis:PutRecord` 권한이 별도로 필요합니다(Lambda 실행 역할과는 별개).

## 향후 개선 사항 (선택)

- 검토 화면(사람이 건별로 승인/거부하는 웹페이지)
- QuickSight 대시보드 연동
