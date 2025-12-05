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
PARENT_DIR = BASE_DIR.parent if BASE_DIR.parent != BASE_DIR else BASE_DIR
DEFAULT_WORKING_DIR = BASE_DIR
if not (BASE_DIR / "src").is_dir():
    parent_candidate = PARENT_DIR
    if parent_candidate and parent_candidate.exists() and (parent_candidate / "src").is_dir():
        DEFAULT_WORKING_DIR = parent_candidate
SCRAPER_WORKING_DIR = Path(os.environ.get("SCRAPER_WORKING_DIR") or DEFAULT_WORKING_DIR)
EXPORT_DIR = Path(os.environ.get("SCRAPER_EXPORT_DIR") or BASE_DIR / "exports")

API_BASE = os.environ.get("STAFFING_API_BASE", "https://api.greatstaff.com").rstrip("/")
CLAIM_ENDPOINT = f"{API_BASE}/staffing/scraper/claim"
COMPLETE_ENDPOINT = f"{API_BASE}/staffing/scraper/complete"

SCRAPER_SECRET = os.environ.get("STAFFING_RUN_SECRET") or os.environ.get("STAFFING_SCRAPER_SECRET")
SCRAPER_COMMAND = os.environ.get(
    "SCRAPER_COMMAND",
    "-m src.main schicht-bestaetigen --headless true",
)
SCRAPER_LOGIN_COMMAND = os.environ.get(
    "SCRAPER_LOGIN_COMMAND",
    "-m src.main login --headless true",
).strip()
LOGIN_STEP_NAME = (os.environ.get("SCRAPER_LOGIN_STEP") or "login").strip().lower()
PYTHON_CMD = os.environ.get("SCRAPER_PYTHON_CMD", "python3")
FORCE_HEADLESS = os.environ.get("SCRAPER_FORCE_HEADLESS", "true").strip().lower() not in {"false", "0", "off"}
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


def run_playwright_command(command: str, *, expect_export: bool) -> Optional[Path]:
    """Führt einen Playwright-Befehl aus und liefert optional die erzeugte CSV."""
    normalized_command = command.strip()
    if not normalized_command:
        raise RuntimeError("Kein Playwright-Befehl konfiguriert")

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    before = snapshot_exports() if expect_export else {}

    if not SCRAPER_WORKING_DIR.exists():
        raise RuntimeError(f"Arbeitsverzeichnis {SCRAPER_WORKING_DIR} existiert nicht")

    cmd = enforce_headless_args([PYTHON_CMD, *shlex.split(normalized_command)])
    LOGGER.info(
        "Starte Playwright: %s (cwd=%s)",
        " ".join(cmd),
        SCRAPER_WORKING_DIR,
    )
    subprocess.run(cmd, cwd=str(SCRAPER_WORKING_DIR), check=True)

    if not expect_export:
        return None

    new_file = detect_new_export(before)
    if not new_file:
        raise RuntimeError("Keine neue CSV im exports-Ordner gefunden")
    LOGGER.info("Neue Datei erkannt: %s", new_file)
    return new_file


def run_scraper_process() -> Path:
    """Startet den eigentlichen Schicht-Scraper und liefert den Export."""
    export_path = run_playwright_command(SCRAPER_COMMAND, expect_export=True)
    if not export_path:
        raise RuntimeError("Scraper-Lauf hat keine CSV erzeugt")
    return export_path


def run_login_process() -> None:
    """Führt den Login-Durchlauf aus, um SessionStorage/Cookies zu erneuern."""
    if not SCRAPER_LOGIN_COMMAND:
        raise RuntimeError("SCRAPER_LOGIN_COMMAND ist nicht gesetzt")
    LOGGER.info("Führe Login-Skript aus, um Sitzungsdaten zu erneuern …")
    run_playwright_command(SCRAPER_LOGIN_COMMAND, expect_export=False)


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


def parse_job_metadata(job: dict) -> dict:
    """Parst das metadata-Feld aus der API-Antwort."""
    metadata = job.get("metadata")
    if not metadata:
        return {}
    if isinstance(metadata, dict):
        return metadata
    if isinstance(metadata, str):
        try:
            parsed = json.loads(metadata)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            LOGGER.warning("Konnte metadata nicht lesen: %s", metadata)
    return {}


def determine_job_step(job: dict, metadata: dict) -> str:
    """Ermittelt den Step aus metadata.step (Fallback job['step'])."""
    candidate = None
    if metadata:
        candidate = metadata.get("step") or metadata.get("phase")
    if not candidate:
        candidate = job.get("step")
    return str(candidate or "").strip().lower()


def is_truthy(value) -> bool:
    """Hilfsfunktion, um boolesche Flags aus metadata zu lesen."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def should_only_run_login(metadata: dict) -> bool:
    """Gibt an, ob ein Lauf nur den Login-Schritt ausführen soll."""
    if not metadata:
        return False
    if is_truthy(metadata.get("loginOnly")) or is_truthy(metadata.get("onlyLogin")):
        return True
    mode = metadata.get("mode") or metadata.get("runMode")
    if isinstance(mode, str) and mode.strip().lower() in {"login-only", "login"}:
        return True
    return False


def enforce_headless_args(cmd: list[str]) -> list[str]:
    """Stellt sicher, dass Playwright immer mit --headless true ausgeführt wird."""
    if not FORCE_HEADLESS or not cmd:
        return cmd

    updated = list(cmd)
    headless_index = None
    for idx, token in enumerate(updated):
        if token.startswith("--headless"):
            headless_index = idx
            break

    if headless_index is None:
        updated.extend(["--headless", "true"])
        return updated

    token = updated[headless_index]
    if token == "--headless":
        if headless_index + 1 < len(updated):
            updated[headless_index + 1] = "true"
        else:
            updated.append("true")
        return updated
    if token.startswith("--headless="):
        updated[headless_index] = "--headless=true"
        return updated

    updated.insert(headless_index + 1, "true")
    return updated


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
    metadata = parse_job_metadata(job)
    step = determine_job_step(job, metadata)
    login_requested = bool(LOGIN_STEP_NAME) and step == LOGIN_STEP_NAME
    login_only = login_requested and should_only_run_login(metadata)

    try:
        if login_requested:
            LOGGER.info("Run %s als Login-Phase erkannt – starte Login-Befehl zuerst", run_id)
            run_login_process()
            LOGGER.info("Login-Phase abgeschlossen")
            if login_only:
                summary = {
                    "step": step or LOGIN_STEP_NAME,
                    "loginOnly": True,
                    "generatedAt": datetime.utcnow().isoformat(),
                }
                completion_payload = {
                    "summary": summary,
                    "message": metadata.get("loginMessage") or "Session aktualisiert (Login-Run)",
                }
                mark_complete(run_id, "success", completion_payload)
                return

        export_path = run_scraper_process()
        file_key = upload_to_s3(export_path)
        row_count = count_rows(export_path)
        folder_date = os.environ.get("STAFFING_FOLDER_DATE") or datetime.now().strftime("%Y-%m-%d")
        summary = {
            "totalRows": max(row_count - 1, 0),
            "generatedAt": datetime.utcnow().isoformat(),
        }
        if login_requested:
            summary["loginStep"] = {"executed": True, "step": step or LOGIN_STEP_NAME}

        message = f"{row_count - 1} Kontakte verarbeitet"
        if login_requested:
            message = f"{message} (inkl. Login)"
        completion_payload = {
            "fileKey": file_key,
            "fileName": export_path.name,
            "folderDate": folder_date,
            "summary": summary,
            "message": message,
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
