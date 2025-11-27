"""
Simple SQS poller to trigger the user_search scraper for new Flow-JSON uploads.

Config via environment variables:
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION   (usual AWS creds)
  QUEUE_URL   - required, full SQS queue URL (e.g. https://sqs.eu-central-1.amazonaws.com/123456789012/zulagen-upload)
  REGION      - optional, defaults to AWS_DEFAULT_REGION or eu-central-1
  PYTHON_CMD  - optional, defaults to "python3"
  SLOWMO_MS   - optional, defaults to 0
  DELAY_SEC   - optional, defaults to 0.05

Usage:
  QUEUE_URL=https://... AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  AWS_DEFAULT_REGION=eu-central-1 python3 poller.py
"""

import json
import os
import subprocess
import sys
import tempfile
import time

import boto3


QUEUE_URL = os.environ.get("QUEUE_URL") or "https://sqs.eu-central-1.amazonaws.com/648862944706/zulagen-upload"
REGION = os.environ.get("REGION") or os.environ.get("AWS_DEFAULT_REGION") or "eu-central-1"
# Standard: nimm das laufende Python (damit das venv auch für den Child-Prozess gilt)
PYTHON_CMD = os.environ.get("PYTHON_CMD", sys.executable)
SLOWMO_MS = os.environ.get("SLOWMO_MS", "0")
DELAY_SEC = os.environ.get("DELAY_SEC", "0.05")


if not QUEUE_URL:
    raise SystemExit("QUEUE_URL env var is required (full SQS queue URL).")

sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def _process_message(msg: dict):
    msg_id = msg.get("MessageId", "?")
    body_raw = msg.get("Body", "")
    try:
        body = json.loads(body_raw)
    except Exception:
        raise ValueError(f"[{msg_id}] Unbekanntes Message-Format: {body_raw!r}")

    records = body.get("Records") if isinstance(body, dict) else None
    if not records:
        raise ValueError(f"[{msg_id}] Keine S3-Records im Message-Body: {body_raw!r}")

    record = records[0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    print(f"[INFO] [{msg_id}] Lade {bucket}/{key} …")
    s3.download_file(bucket, key, tmp.name)

    print(f"[INFO] [{msg_id}] Starte scraper für {bucket}/{key} …")
    result = subprocess.run(
        [
            PYTHON_CMD,
            "-m",
            "src.user_search",
            "--flow-file",
            tmp.name,
            "--headless",
            "true",
            "--slowmo",
            str(SLOWMO_MS),
            "--delay",
            str(DELAY_SEC),
        ],
        check=False,
    )
    print(f"[INFO] [{msg_id}] Scraper beendet mit Code {result.returncode}")


def main():
    print(f"[INFO] Polling SQS: {QUEUE_URL} (Region: {REGION})")
    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=60,
            )
            messages = resp.get("Messages", [])
            if not messages:
                continue

            for m in messages:
                try:
                    _process_message(m)
                except Exception as exc:
                    print(f"[WARNUNG] Verarbeitung fehlgeschlagen: {exc}")
                finally:
                    try:
                        sqs.delete_message(
                            QueueUrl=QUEUE_URL, ReceiptHandle=m["ReceiptHandle"]
                        )
                    except Exception as exc:
                        print(f"[WARNUNG] Konnte Message nicht löschen: {exc}")
        except KeyboardInterrupt:
            print("Beende Poller …")
            break
        except Exception as exc:
            print(f"[WARNUNG] Fehler beim Polling: {exc}")
            time.sleep(2)


if __name__ == "__main__":
    main()
