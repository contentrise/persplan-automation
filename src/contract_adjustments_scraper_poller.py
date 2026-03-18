"""
Contract Adjustments Scraper Poller
-----------------------------------

Pollt die Hub-API für neue Vertragsanpassungs-Transfers, lädt das
signierte PDF herunter, startet den Playwright-Scraper und meldet
Status inkl. Logs zurück.
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
    level=os.environ.get("CONTRACT_ADJUSTMENTS_SCRAPER_LOG_LEVEL", "INFO"),
    format="[%(asctime)s] %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("contract_adjustments_scraper")

BASE_DIR = Path(__file__).resolve().parent
PARENT_DIR = BASE_DIR.parent if BASE_DIR.parent != BASE_DIR else BASE_DIR
DEFAULT_WORKING_DIR = BASE_DIR
if not (BASE_DIR / "src").is_dir():
    parent_candidate = PARENT_DIR
    if parent_candidate and parent_candidate.exists() and (parent_candidate / "src").is_dir():
        DEFAULT_WORKING_DIR = parent_candidate

SCRAPER_WORKING_DIR = Path(os.environ.get("SCRAPER_WORKING_DIR") or DEFAULT_WORKING_DIR)
PYTHON_CMD = os.environ.get("SCRAPER_PYTHON_CMD", "python3")
POLL_INTERVAL = float(os.environ.get("CONTRACT_ADJUSTMENTS_SCRAPER_POLL_INTERVAL", "20"))

API_BASE = (
    os.environ.get("CONTRACT_ADJUSTMENTS_SCRAPER_API_BASE")
    or os.environ.get("STAFFING_API_BASE")
    or "https://api.greatstaff.com"
).rstrip("/")
CLAIM_ENDPOINT = f"{API_BASE}/contract-adjustments/scraper/claim"
COMPLETE_ENDPOINT = f"{API_BASE}/contract-adjustments/scraper/complete"

SCRAPER_SECRET = (
    os.environ.get("CONTRACT_ADJUSTMENTS_SCRAPER_SECRET")
    or os.environ.get("SCRAPER_RUN_SECRET")
)

SCRAPER_COMMAND = (
    os.environ.get("CONTRACT_ADJUSTMENTS_SCRAPER_COMMAND")
    or "-m src.main vertragsanpassung-transfer --headless true"
).strip()
LOGIN_COMMAND = (
    os.environ.get("CONTRACT_ADJUSTMENTS_SCRAPER_COMMAND_LOGIN")
    or "-m src.main login --headless true"
).strip()

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})


def claim_run() -> Optional[dict]:
    if not SCRAPER_SECRET:
        raise RuntimeError("CONTRACT_ADJUSTMENTS_SCRAPER_SECRET fehlt")
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


def run_playwright(command: str, env: dict) -> subprocess.CompletedProcess:
    if not SCRAPER_WORKING_DIR.exists():
        raise RuntimeError(f"Arbeitsverzeichnis {SCRAPER_WORKING_DIR} existiert nicht")
    cmd = [PYTHON_CMD, *shlex.split(command)]
    LOGGER.info("Starte Playwright: %s (cwd=%s)", " ".join(cmd), SCRAPER_WORKING_DIR)
    return subprocess.run(cmd, cwd=str(SCRAPER_WORKING_DIR), env=env, capture_output=True, text=True)


def run_login() -> None:
    if not LOGIN_COMMAND:
        raise RuntimeError("LOGIN_COMMAND fehlt")
    env = os.environ.copy()
    result = run_playwright(LOGIN_COMMAND, env=env)
    if result.returncode != 0:
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        raise RuntimeError(f"Login fehlgeschlagen (rc={result.returncode})\n{stdout}\n{stderr}")
    LOGGER.info("Login erfolgreich.")


def download_signed_pdf(url: str, target_path: Path) -> None:
    response = session.get(url, timeout=60)
    response.raise_for_status()
    target_path.write_bytes(response.content)


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


def process_run(job: dict) -> None:
    run_id = job.get("runId") or job.get("run_id")
    entry_id = job.get("entryId") or job.get("entry_id")
    phone = str(job.get("phone") or "").strip()
    description = str(job.get("description") or "").strip()
    signed_url = str(job.get("signedPdfUrl") or "").strip()
    if not run_id or not entry_id or not phone or not description or not signed_url:
        mark_complete(run_id, "error", {"error": "Unvollständiger Job"})
        return

    started_at = time.time()
    log_chunks = []
    try:
        run_login()
        with tempfile.TemporaryDirectory(prefix=f"vertragsanpassung-{entry_id}-") as tmp_dir:
            input_dir = Path(tmp_dir) / "perso-input"
            input_dir.mkdir(parents=True, exist_ok=True)

            json_payload = {"phone": phone, "description": description}
            json_path = input_dir / f"vertragsanpassung-{entry_id}.json"
            json_path.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")

            pdf_path = input_dir / "vertragsanpassung.pdf"
            download_signed_pdf(signed_url, pdf_path)
            LOGGER.info("Datei heruntergeladen: %s", pdf_path.name)

            env = os.environ.copy()
            env["PERSO_INPUT_DIR"] = str(input_dir)
            result = run_playwright(SCRAPER_COMMAND, env=env)
            log_chunks.append("=== STDOUT ===\n" + (result.stdout or "").strip())
            log_chunks.append("=== STDERR ===\n" + (result.stderr or "").strip())

            if result.returncode != 0:
                raise RuntimeError(f"Scraper Exit-Code {result.returncode}")

        duration = round(time.time() - started_at, 2)
        summary = {"durationSeconds": duration, "entryId": entry_id, "step": "vertragsanpassung"}
        mark_complete(
            run_id,
            "success",
            {
                "message": "Scraper erfolgreich abgeschlossen.",
                "summary": summary,
                "logText": "\n\n".join(log_chunks),
            },
        )
    except Exception as exc:  # pylint: disable=broad-except
        duration = round(time.time() - started_at, 2)
        summary = {"durationSeconds": duration, "entryId": entry_id, "step": "vertragsanpassung"}
        log_chunks.append(f"=== ERROR ===\n{exc}")
        mark_complete(
            run_id,
            "error",
            {
                "error": str(exc),
                "message": "Scraper fehlgeschlagen.",
                "summary": summary,
                "logText": "\n\n".join(log_chunks),
            },
        )


def main() -> None:
    LOGGER.info("Starte Vertragsanpassungen-Scraper-Poller (Claim: %s)", CLAIM_ENDPOINT)
    while True:
        job = claim_run()
        if job:
            process_run(job)
            time.sleep(3)
        else:
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        LOGGER.info("Beende auf Benutzerwunsch …")
