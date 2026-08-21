"""
simulate_kinesis_stream.py

목적:
  fds_simulator_dataset.json (또는 .csv)을 1초에 1건씩 읽어
  Amazon Kinesis Data Stream(team2-fds-data-stream)으로 put_record 전송하는
  Phase 2 시뮬레이터 스크립트.

실행 환경: 로컬 PC (Windows, conda fds_env, `aws configure` 완료된 상태)
  - 이 스크립트는 실제 AWS 자격증명이 필요하므로 로컬에서 실행할 것.
  - 사전 설치: `pip install boto3` (이미 fds_env에 설치되어 있음)

사용 예:
  conda activate fds_env
  python simulate_kinesis_stream.py
  python simulate_kinesis_stream.py --interval 0.5 --limit 10   # 앞 10건만 0.5초 간격으로
  python simulate_kinesis_stream.py --dry-run                    # 실제 전송 없이 페이로드만 확인
  python simulate_kinesis_stream.py --loop                       # 66건 다 보내면 처음부터 반복

주의:
  - PartitionKey는 transaction_id를 그대로 사용 (체크리스트 "파티션 키 설계" 항목 반영)
  - Kinesis put_record의 Data는 raw bytes로 전달하면 됨. base64 인코딩은
    Kinesis가 Lambda(1차)로 이벤트를 전달할 때 자동으로 처리하므로 여기서는
    별도 인코딩 불필요 (1차 Lambda가 수신 시 base64 디코딩하는 것과 짝을 이룸).
  - 전송하는 JSON에는 원본 스키마 그대로(transaction_id, amount, transaction_hour,
    merchant_category, foreign_transaction, location_mismatch, device_trust_score,
    velocity_last_24h, cardholder_age, is_fraud)에 리플레이 시각(event_timestamp)만
    추가함. is_fraud를 포함하는 이유: 기존에 성공했던 test-fraud-001 / test-txn-001
    테스트와 동일한 페이로드 구조를 유지하기 위함 (1차 Lambda는 SageMaker 호출 시
    is_fraud/transaction_id를 알아서 제외하고 사용하므로 포함돼 있어도 무방).
    만약 실제 팀 데모에서 "미지의 실시간 거래"처럼 보이길 원한다면 --strip-label
    옵션으로 is_fraud를 페이로드에서 제거할 수 있음.
"""

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("simulator")

DEFAULT_STREAM_NAME = "team2-fds-data-stream"
DEFAULT_REGION = "ap-northeast-2"

NUMERIC_INT_FIELDS = {
    "transaction_hour",
    "foreign_transaction",
    "location_mismatch",
    "device_trust_score",
    "velocity_last_24h",
    "cardholder_age",
    "is_fraud",
}


def load_records(input_path: Path) -> list[dict]:
    if input_path.suffix.lower() == ".json":
        with open(input_path, "r", encoding="utf-8") as f:
            records = json.load(f)
    elif input_path.suffix.lower() == ".csv":
        records = []
        with open(input_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for key in NUMERIC_INT_FIELDS:
                    if key in row and row[key] != "":
                        row[key] = int(row[key])
                if "amount" in row and row["amount"] != "":
                    # 정수/실수 모두 대응
                    amt = row["amount"]
                    row["amount"] = int(amt) if float(amt).is_integer() else float(amt)
                records.append(row)
    else:
        raise ValueError(f"지원하지 않는 입력 파일 형식: {input_path.suffix}")

    if not records:
        raise ValueError(f"{input_path}에 레코드가 없습니다.")
    return records


def build_payload(record: dict, strip_label: bool) -> dict:
    payload = dict(record)
    if strip_label:
        payload.pop("is_fraud", None)
    # 리플레이 시점 timestamp 필드 삽입 (체크리스트 항목)
    payload["event_timestamp"] = datetime.now(timezone.utc).isoformat()
    return payload


def send_loop(records: list[dict], args) -> None:
    kinesis = None
    if not args.dry_run:
        import boto3  # 지연 임포트: --dry-run일 땐 boto3/자격증명 불필요

        kinesis = boto3.client("kinesis", region_name=args.region)

    total_sent, total_failed = 0, 0
    fraud_sent, normal_sent = 0, 0
    pass_num = 0

    try:
        while True:
            pass_num += 1
            log.info(f"=== 전송 {'회차 ' + str(pass_num) if args.loop else '시작'} (총 {len(records)}건) ===")
            for i, record in enumerate(records, start=1):
                if args.limit and total_sent + total_failed >= args.limit:
                    break

                payload = build_payload(record, args.strip_label)
                partition_key = str(record["transaction_id"])
                data_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

                if args.dry_run:
                    log.info(
                        f"[DRY-RUN] ({i}/{len(records)}) PartitionKey={partition_key} "
                        f"is_fraud={record.get('is_fraud')} payload={payload}"
                    )
                    total_sent += 1
                else:
                    try:
                        resp = kinesis.put_record(
                            StreamName=args.stream_name,
                            Data=data_bytes,
                            PartitionKey=partition_key,
                        )
                        log.info(
                            f"[OK] ({i}/{len(records)}) {partition_key} -> "
                            f"ShardId={resp['ShardId']} SeqNo={resp['SequenceNumber']}"
                        )
                        total_sent += 1
                    except Exception as e:
                        log.error(f"[FAIL] ({i}/{len(records)}) {partition_key} -> {e}")
                        total_failed += 1

                if record.get("is_fraud") in (1, "1", True):
                    fraud_sent += 1
                else:
                    normal_sent += 1

                if args.limit and total_sent + total_failed >= args.limit:
                    break

                time.sleep(args.interval)

            if not args.loop:
                break
            if args.limit and total_sent + total_failed >= args.limit:
                break

    except KeyboardInterrupt:
        log.warning("사용자 중단 (Ctrl+C) — 지금까지 결과를 요약합니다.")

    log.info(
        f"=== 완료: 성공 {total_sent}건 / 실패 {total_failed}건 "
        f"(사기 라벨 {fraud_sent}건, 정상 라벨 {normal_sent}건) ==="
    )


def main():
    parser = argparse.ArgumentParser(description="FDS 파이프라인 Kinesis 시뮬레이터")
    parser.add_argument("--input", default="fds_simulator_dataset.json", help="입력 데이터 파일(.json 또는 .csv)")
    parser.add_argument("--stream-name", default=DEFAULT_STREAM_NAME, help="Kinesis 스트림명")
    parser.add_argument("--region", default=DEFAULT_REGION, help="AWS 리전")
    parser.add_argument("--interval", type=float, default=1.0, help="레코드 간 전송 간격(초)")
    parser.add_argument("--limit", type=int, default=0, help="전송할 최대 건수 (0=제한 없음, 데이터셋 전체)")
    parser.add_argument("--loop", action="store_true", help="데이터셋을 끝까지 보내면 처음부터 반복")
    parser.add_argument("--strip-label", action="store_true", help="페이로드에서 is_fraud 필드를 제거 (실제 미지의 거래처럼 시뮬레이션)")
    parser.add_argument("--dry-run", action="store_true", help="실제 Kinesis 전송 없이 페이로드만 로그로 확인")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        log.error(f"입력 파일을 찾을 수 없습니다: {input_path}")
        sys.exit(1)

    records = load_records(input_path)
    log.info(f"데이터 {len(records)}건 로드 완료: {input_path}")
    log.info(
        f"설정: stream={args.stream_name}, region={args.region}, "
        f"interval={args.interval}s, loop={args.loop}, dry_run={args.dry_run}, "
        f"strip_label={args.strip_label}, limit={args.limit or '전체'}"
    )

    send_loop(records, args)


if __name__ == "__main__":
    main()