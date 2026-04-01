"""
Shift Apply Poller
------------------
Pollt /shift-apply/scraper/claim, führt shift_apply.py aus,
und meldet das Ergebnis an /shift-apply/scraper/complete.
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

import requests

logging.basicConfig(
    level=os.environ.get("SHIFT_APPLY_LOG_LEVEL", "INFO"),
    format="[%(asctime)s] %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("shift_apply_poller")

API_BASE = os.environ.get("SHIFT_APPLY_API_BASE", "https://api.greatstaff.com").rstrip("/")
CLAIM_ENDPOINT = f"{API_BASE}/shift-apply/scraper/claim"
COMPLETE_ENDPOINT = f"{API_BASE}/shift-apply/scraper/complete"
SECRET = os.environ.get("SHIFT_APPLY_RUN_SECRET") or os.environ.get("SHIFT_APPLY_SECRET")

PYTHON_CMD = os.environ.get("SHIFT_APPLY_PYTHON_CMD", "python3")
SCRAPER_COMMAND = os.environ.get("SHIFT_APPLY_COMMAND", "-m src.shift_apply")
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_WORKING_DIR = BASE_DIR.parent if (BASE_DIR.parent / "src").is_dir() else BASE_DIR
WORKING_DIR = Path(os.environ.get("SHIFT_APPLY_WORKING_DIR") or DEFAULT_WORKING_DIR)
POLL_INTERVAL = float(os.environ.get("SHIFT_APPLY_POLL_INTERVAL", "60"))

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})


def claim_run():
    if not SECRET:
        raise RuntimeError("SHIFT_APPLY_RUN_SECRET fehlt")
    try:
        response = session.post(
            CLAIM_ENDPOINT,
            headers={"x-scraper-secret": SECRET},
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
    LOGGER.info("Run %s zugewiesen", payload.get("requestId") or payload.get("requestId"))
    return payload


def run_scraper(payload: dict) -> subprocess.CompletedProcess:
    if not WORKING_DIR.exists():
        raise RuntimeError(f"Arbeitsverzeichnis {WORKING_DIR} existiert nicht")
    cmd = [PYTHON_CMD, *shlex.split(SCRAPER_COMMAND)]
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
        json.dump(payload, tmp)
        tmp_path = tmp.name
    try:
        cmd.extend(["--payload-file", tmp_path])
        LOGGER.info("Starte Scraper: %s (cwd=%s)", " ".join(cmd), WORKING_DIR)
        env = os.environ.copy()
        env["HEADLESS"] = "true"
        return subprocess.run(cmd, cwd=str(WORKING_DIR), capture_output=True, text=True, env=env)
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass


def mark_complete(request_id: str, status: str, payload: dict) -> None:
    data = {"requestId": request_id, "status": status}
    data.update(payload)
    try:
        response = session.post(
            COMPLETE_ENDPOINT,
            headers={"x-scraper-secret": SECRET},
            json=data,
            timeout=30,
        )
        response.raise_for_status()
        LOGGER.info("Run %s → %s gemeldet", request_id, status)
    except requests.RequestException as exc:
        LOGGER.error("Complete-Request fehlgeschlagen: %s", exc)


def parse_result(stdout: str) -> dict:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    for line in reversed(lines):
        try:
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            continue
    return {}


def main():
    LOGGER.info("Shift-Apply-Poller gestartet (%s)", CLAIM_ENDPOINT)
    while True:
        job = None
        try:
            job = claim_run()
        except Exception as exc:
            LOGGER.error("Claim fehlgeschlagen: %s", exc)

        if not job:
            time.sleep(POLL_INTERVAL)
            continue

        request_id = job.get("requestId") or job.get("request_id")
        if not request_id:
            LOGGER.warning("Job ohne requestId erhalten")
            time.sleep(5)
            continue

        try:
            result = run_scraper(job)
        except Exception as exc:
            mark_complete(request_id, "error", {"error": str(exc)})
            continue

        log_text = "\n".join([(result.stdout or "").strip(), (result.stderr or "").strip()]).strip()
        parsed = parse_result(result.stdout or "")
        success = bool(parsed.get("success"))
        resolved_user_id = parsed.get("resolved_user_id") or ""
        error_type = parsed.get("error_type") or ""

        if result.returncode != 0:
            payload = {
                "error": parsed.get("error") or f"Returncode {result.returncode}",
                "logText": log_text,
            }
            if error_type:
                payload["failure_type"] = error_type
            mark_complete(request_id, "error", payload)
            continue

        if success:
            mark_complete(
                request_id,
                "success",
                {"message": parsed.get("message") or "OK", "logText": log_text, "resolved_user_id": resolved_user_id},
            )
        else:
            payload = {"error": parsed.get("error") or "Unbekannter Fehler", "logText": log_text}
            if error_type:
                payload["failure_type"] = error_type
            mark_complete(request_id, "error", payload)

        time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        LOGGER.info("Beende auf Benutzerwunsch …")
