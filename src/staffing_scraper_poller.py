"""
Staffing-Scraper Poller
-----------------------

Dieses Skript läuft dauerhaft (z. B. via systemd) und übernimmt:
  1. Bei der Hub-API nach neuen Läufen fragen (/staffing/scraper/claim)
  2. Den vorhandenen Playwright-Scraper starten (src.main schicht-bestaetigen ...)
  3. Die erzeugte CSV ins konfigurierte S3 hochladen
  4. Der API den Abschluss melden (/staffing/scraper/complete)

Abhängigkeiten:
  pip install requests boto3 python-dotenv (falls .env verwendet wird)
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import boto3
import requests

logging.basicConfig(
    level=os.environ.get("STAFFING_LOG_LEVEL", "INFO"),
    format="[%(asctime)s] %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("staffing_scraper")

BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = Path(os.environ.get("SCRAPER_EXPORT_DIR") or BASE_DIR / "exports")

API_BASE = os.environ.get("STAFFING_API_BASE", "https://api.greatstaff.com").rstrip("/")
CLAIM_ENDPOINT = f"{API_BASE}/staffing/scraper/claim"
COMPLETE_ENDPOINT = f"{API_BASE}/staffing/scraper/complete"

SCRAPER_SECRET = os.environ.get("STAFFING_RUN_SECRET") or os.environ.get("STAFFING_SCRAPER_SECRET")
SCRAPER_COMMAND = os.environ.get(
    "SCRAPER_COMMAND",
    "-m src.main schicht-bestaetigen --headless true",
)
PYTHON_CMD = os.environ.get("SCRAPER_PYTHON_CMD", "python")
POLL_INTERVAL = float(os.environ.get("SCRAPER_POLL_INTERVAL", "60"))

S3_BUCKET = os.environ.get("STAFFING_BUCKET") or os.environ.get("STAFFING_S3_BUCKET")
S3_PREFIX = (os.environ.get("STAFFING_PLAN_FOLDER") or "staffing/dienstplan").strip("/")
S3_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "eu-central-1"

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})


def claim_run() -> Optional[dict]:
    """Fragt bei der API nach dem nächsten pending-Lauf."""
    if not SCRAPER_SECRET:
        raise RuntimeError("STAFFING_RUN_SECRET fehlt")

    try:
        response = session.post(
            CLAIM_ENDPOINT,
            headers={"x-scraper-secret": SCRAPER_SECRET},
            json={},
            timeout=30,
        )
    except requests.RequestException as exc:
        LOGGER.error("Claim-Request fehlgeschlagen: %s", exc)
        return None

    if response.status_code == 204:
        return None
    if response.status_code == 403:
        LOGGER.error("Ungültiges Secret für Claim-Endpoint (403)")
        time.sleep(120)
        return None
    if response.status_code >= 500:
        LOGGER.warning("Claim-Endpoint %s lieferte %s", CLAIM_ENDPOINT, response.status_code)
        return None

    response.raise_for_status()
    payload = response.json()
    LOGGER.info("Lauf %s zugewiesen", payload.get("runId"))
    return payload


def run_scraper_process() -> Path:
    """Startet Playwright und ermittelt die neu erzeugte CSV-Datei."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    before = snapshot_exports()

    cmd = [PYTHON_CMD, *shlex.split(SCRAPER_COMMAND)]
    LOGGER.info("Starte Scraper: %s", " ".join(cmd))
    subprocess.run(cmd, cwd=str(BASE_DIR), check=True)

    new_file = detect_new_export(before)
    if not new_file:
        raise RuntimeError("Keine neue CSV im exports-Ordner gefunden")
    LOGGER.info("Neue Datei erkannt: %s", new_file)
    return new_file


def snapshot_exports() -> Dict[str, float]:
    """Merkt sich den mtime-Stand vor dem Lauf."""
    return {str(path): path.stat().st_mtime for path in EXPORT_DIR.glob("*.csv")}


def detect_new_export(before: Dict[str, float]) -> Optional[Path]:
    """Ermittelt die neueste CSV nach dem Lauf."""
    candidates = sorted(
        EXPORT_DIR.glob("*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for candidate in candidates:
        if str(candidate) not in before or candidate.stat().st_mtime > before.get(str(candidate), 0):
            return candidate
    return candidates[0] if candidates else None


def upload_to_s3(file_path: Path) -> Optional[str]:
    """Lädt die CSV ins konfigurierte S3-Bucket."""
    if not S3_BUCKET:
        LOGGER.warning("STAFFING_BUCKET nicht gesetzt – überspringe Upload")
        return None

    folder_date = os.environ.get("STAFFING_FOLDER_DATE") or datetime.now().strftime("%Y-%m-%d")
    key = f"{S3_PREFIX}/{folder_date}/{file_path.name}"
    LOGGER.info("Lade %s nach s3://%s/%s …", file_path, S3_BUCKET, key)

    s3 = boto3.client("s3", region_name=S3_REGION)
    s3.upload_file(
        str(file_path),
        S3_BUCKET,
        key,
        ExtraArgs={"ContentType": "text/csv"},
    )
    return key


def count_rows(file_path: Path) -> int:
    """Zählt die Zeilen (inkl. Header) in der CSV."""
    with file_path.open("r", encoding="utf-8", errors="ignore") as handler:
        return sum(1 for _ in handler)


def mark_complete(run_id: str, status: str, payload: dict) -> None:
    """Sendet das Ergebnis an die API."""
    data = {"runId": run_id, "status": status}
    data.update(payload)
    try:
        response = session.post(
            COMPLETE_ENDPOINT,
            headers={"x-scraper-secret": SCRAPER_SECRET},
            json=data,
            timeout=30,
        )
        response.raise_for_status()
        LOGGER.info("Run %s → %s gemeldet", run_id, status)
    except requests.RequestException as exc:
        LOGGER.error("Complete-Request fehlgeschlagen: %s", exc)


def process_run(job: dict) -> None:
    """Führt einen einzelnen Lauf aus."""
    run_id = job.get("runId")
    if not run_id:
        LOGGER.warning("Antwort ohne runId erhalten: %s", job)
        return

    try:
        export_path = run_scraper_process()
        file_key = upload_to_s3(export_path)
        row_count = count_rows(export_path)
        folder_date = os.environ.get("STAFFING_FOLDER_DATE") or datetime.now().strftime("%Y-%m-%d")
        summary = {
            "totalRows": max(row_count - 1, 0),
            "generatedAt": datetime.utcnow().isoformat(),
        }
        completion_payload = {
            "fileKey": file_key,
            "fileName": export_path.name,
            "folderDate": folder_date,
            "summary": summary,
            "message": f"{row_count - 1} Kontakte verarbeitet",
        }
        mark_complete(run_id, "success", completion_payload)
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.exception("Lauf %s fehlgeschlagen", run_id)
        mark_complete(
            run_id,
            "error",
            {"error": str(exc)},
        )


def main() -> None:
    LOGGER.info("Starte Staffing-Scraper-Poller (Claim: %s)", CLAIM_ENDPOINT)
    while True:
        job = claim_run()
        if job:
            process_run(job)
            time.sleep(5)
        else:
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        LOGGER.info("Beende auf Benutzerwunsch …")
