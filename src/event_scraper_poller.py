"""
Event-Scraper Poller
--------------------
Pollt die Hub-API für neue Event-Anlagen, startet va_anlage.py mit Payload
und meldet den Abschluss zurück.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional

import requests

logging.basicConfig(
    level=os.environ.get("EVENT_SCRAPER_LOG_LEVEL", "INFO"),
    format="[%(asctime)s] %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("event_scraper")

BASE_DIR = Path(__file__).resolve().parent
PARENT_DIR = BASE_DIR.parent if BASE_DIR.parent != BASE_DIR else BASE_DIR
DEFAULT_WORKING_DIR = BASE_DIR
if not (BASE_DIR / "src").is_dir():
    parent_candidate = PARENT_DIR
    if parent_candidate and parent_candidate.exists() and (parent_candidate / "src").is_dir():
        DEFAULT_WORKING_DIR = parent_candidate

SCRAPER_WORKING_DIR = Path(os.environ.get("EVENT_SCRAPER_WORKING_DIR") or DEFAULT_WORKING_DIR)
PYTHON_CMD = os.environ.get("EVENT_SCRAPER_PYTHON_CMD", "python3")
POLL_INTERVAL = float(os.environ.get("EVENT_SCRAPER_POLL_INTERVAL", "20"))

API_BASE = (os.environ.get("EVENT_SCRAPER_API_BASE") or "https://api.greatstaff.com").rstrip("/")
CLAIM_ENDPOINT = f"{API_BASE}/event-form/scraper/claim"
COMPLETE_ENDPOINT = f"{API_BASE}/event-form/scraper/complete"
SCRAPER_SECRET = os.environ.get("EVENT_SCRAPER_SECRET") or os.environ.get("SCRAPER_RUN_SECRET")

SCRAPER_COMMAND = os.environ.get("EVENT_SCRAPER_COMMAND", "va_anlage.py --headless true").strip()
RESULTS_PATH = Path(os.environ.get("EVENT_SCRAPER_RESULTS") or "exports/va_anlage_result.jsonl")

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})


def claim_run() -> Optional[dict]:
    if not SCRAPER_SECRET:
        raise RuntimeError("EVENT_SCRAPER_SECRET fehlt")
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


def load_latest_result() -> dict:
    if not RESULTS_PATH.exists():
        return {}
    try:
        with RESULTS_PATH.open("r", encoding="utf-8") as handle:
            lines = [line.strip() for line in handle.readlines() if line.strip()]
        if not lines:
            return {}
        return json.loads(lines[-1])
    except Exception as exc:
        LOGGER.warning("Ergebnisdatei konnte nicht gelesen werden: %s", exc)
        return {}


def run_scraper(payload: dict) -> subprocess.CompletedProcess:
    if not SCRAPER_WORKING_DIR.exists():
        raise RuntimeError(f"Arbeitsverzeichnis {SCRAPER_WORKING_DIR} existiert nicht")
    cmd = [PYTHON_CMD, *shlex.split(SCRAPER_COMMAND)]
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
        json.dump(payload, tmp)
        tmp_path = tmp.name
    try:
        cmd.extend(["--payload-file", tmp_path])
        LOGGER.info("Starte Scraper: %s (cwd=%s)", " ".join(cmd), SCRAPER_WORKING_DIR)
        return subprocess.run(cmd, cwd=str(SCRAPER_WORKING_DIR), capture_output=True, text=True)
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass


def mark_complete(run_id: str, status: str, payload: dict) -> None:
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


def main() -> None:
    LOGGER.info("Event-Scraper Poller gestartet.")
    while True:
        job = None
        try:
            job = claim_run()
        except Exception as exc:
            LOGGER.error("Claim fehlgeschlagen: %s", exc)

        if not job:
            time.sleep(POLL_INTERVAL)
            continue

        run_id = job.get("runId")
        request_id = job.get("requestId")
        payload = {
            "requestId": request_id,
            "customerName": job.get("customerName"),
            "formData": job.get("formData") or {},
            "personalbedarf": job.get("personalbedarf") or [],
        }

        result = run_scraper(payload)
        log_text = "\n".join([(result.stdout or "").strip(), (result.stderr or "").strip()]).strip()
        if log_text:
            tail = log_text[-4000:]
            LOGGER.info("Scraper-Output (tail):\n%s", tail)

        if result.returncode != 0:
            LOGGER.error("Scraper fehlgeschlagen (rc=%s)", result.returncode)
            mark_complete(
                run_id,
                "error",
                {
                    "requestId": request_id,
                    "message": "Scraper-Fehler",
                    "error": f"Returncode {result.returncode}",
                    "logText": log_text,
                },
            )
            continue

        latest = load_latest_result()
        success = bool(latest.get("success"))
        status = "success" if success else "error"
        mark_complete(
            run_id,
            status,
            {
                "requestId": request_id,
                "persplanId": latest.get("persplanId") or "",
                "persplanUrl": latest.get("persplanUrl") or "",
                "message": latest.get("message") or "",
                "logText": log_text,
            },
        )


if __name__ == "__main__":
    main()
