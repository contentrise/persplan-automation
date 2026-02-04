"""
Personalfragebogen Scraper Poller
---------------------------------

Pollt die Hub-API für neue Scraper-Runs (Mitarbeiteranlage / Vervollständigen / Mitarbeiterinformationen),
lädt die Daten und den Vertrag herunter, startet die Playwright-Skripte und
meldet den Status inkl. Logs zurück.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests

logging.basicConfig(
    level=os.environ.get("PERSONAL_SCRAPER_LOG_LEVEL", "INFO"),
    format="[%(asctime)s] %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("personal_scraper")

BASE_DIR = Path(__file__).resolve().parent
PARENT_DIR = BASE_DIR.parent if BASE_DIR.parent != BASE_DIR else BASE_DIR
DEFAULT_WORKING_DIR = BASE_DIR
if not (BASE_DIR / "src").is_dir():
    parent_candidate = PARENT_DIR
    if parent_candidate and parent_candidate.exists() and (parent_candidate / "src").is_dir():
        DEFAULT_WORKING_DIR = parent_candidate

SCRAPER_WORKING_DIR = Path(os.environ.get("SCRAPER_WORKING_DIR") or DEFAULT_WORKING_DIR)
PYTHON_CMD = os.environ.get("SCRAPER_PYTHON_CMD", os.environ.get("STAFFING_PYTHON_CMD", "python3"))
POLL_INTERVAL = float(os.environ.get("PERSONAL_SCRAPER_POLL_INTERVAL", "20"))

API_BASE = (
    os.environ.get("PERSONAL_SCRAPER_API_BASE")
    or os.environ.get("STAFFING_API_BASE")
    or "https://api.greatstaff.com"
).rstrip("/")
DETAIL_ENDPOINT = f"{API_BASE}/personalerfassung"
CLAIM_ENDPOINT = f"{API_BASE}/personalerfassung/scraper/claim"
COMPLETE_ENDPOINT = f"{API_BASE}/personalerfassung/scraper/complete"
SCRAPER_SECRET = (
    os.environ.get("PERSONAL_SCRAPER_SECRET")
    or os.environ.get("STAFFING_RUN_SECRET")
    or os.environ.get("SCRAPER_RUN_SECRET")
)

STEP_COMMANDS = {
    "anlage": (
        os.environ.get("PERSONAL_SCRAPER_COMMAND_ANLAGE")
        or os.environ.get("STAFFING_SCRAPER_COMMAND_ANLAGE")
        or "-m src.main mitarbeiteranlage --headless true"
    ).strip(),
    "vervollstaendigen": (
        os.environ.get("PERSONAL_SCRAPER_COMMAND_VOLL")
        or os.environ.get("STAFFING_SCRAPER_COMMAND_VOLL")
        or "-m src.main mitarbeiter-vervollstaendigen --headless true"
    ).strip(),
    "mitarbeiterinformationen": (
        os.environ.get("PERSONAL_SCRAPER_COMMAND_INFO")
        or os.environ.get("STAFFING_SCRAPER_COMMAND_INFO")
        or "-m src.main mitarbeiterinformationen --headless true"
    ).strip(),
}
LOGIN_COMMAND = (
    os.environ.get("PERSONAL_SCRAPER_COMMAND_LOGIN")
    or os.environ.get("STAFFING_SCRAPER_COMMAND_LOGIN")
    or "-m src.main login --headless true"
).strip()

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})


def claim_run() -> Optional[dict]:
    if not SCRAPER_SECRET:
        raise RuntimeError("PERSONAL_SCRAPER_SECRET fehlt")
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


def fetch_entry(entry_id: str) -> dict:
    response = session.get(f"{DETAIL_ENDPOINT}/{entry_id}", timeout=30)
    response.raise_for_status()
    return response.json()


def download_contract(contract_url: str, target_path: Path) -> None:
    response = session.get(contract_url, timeout=60)
    response.raise_for_status()
    content_type = (response.headers.get("Content-Type") or "").lower()
    if "pdf" not in content_type and not contract_url.lower().endswith(".pdf"):
        raise RuntimeError(f"Vertrag ist kein PDF (Content-Type: {content_type})")
    target_path.write_bytes(response.content)


def _guess_extension(url: str, content_type: str, fallback: str = ".bin") -> str:
    ct = (content_type or "").lower()
    if "pdf" in ct:
        return ".pdf"
    if "png" in ct:
        return ".png"
    if "jpeg" in ct or "jpg" in ct:
        return ".jpg"

    parsed = urlparse(url or "")
    suffix = Path(parsed.path).suffix
    if suffix:
        return suffix
    return fallback


def download_optional_file(file_url: str, target_stem: str, target_dir: Path) -> Optional[Path]:
    if not file_url:
        return None
    response = session.get(file_url, timeout=60)
    response.raise_for_status()
    ext = _guess_extension(file_url, response.headers.get("Content-Type", ""))
    target_path = target_dir / f"{target_stem}{ext}"
    target_path.write_bytes(response.content)
    return target_path


def build_input_payload(entry_data: dict, contract_data: dict | None) -> dict:
    if contract_data is None:
        return entry_data
    return {
        "fragebogen": entry_data,
        "vertrag": contract_data,
    }


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
    step = (job.get("step") or "").strip().lower()
    if not run_id or not entry_id or not step:
        LOGGER.warning("Unvollständiger Job: %s", job)
        return
    command = STEP_COMMANDS.get(step)
    if not command:
        mark_complete(run_id, "error", {"error": f"Unbekannter step: {step}"})
        return

    started_at = time.time()
    log_chunks = []
    try:
        LOGGER.info("Starte Login vor Run %s ...", run_id)
        run_login()
        entry = fetch_entry(entry_id)
        payload = entry.get("data") or {}
        if isinstance(entry.get("pdfUrls"), dict):
            payload["pdfUrls"] = entry.get("pdfUrls")
        contract_data = payload.get("contract_transfer") or {}
        contract_file = contract_data.get("file") if isinstance(contract_data, dict) else None

        with tempfile.TemporaryDirectory(prefix=f"perso-{entry_id}-") as tmp_dir:
            input_dir = Path(tmp_dir) / "perso-input"
            input_dir.mkdir(parents=True, exist_ok=True)

            json_payload = build_input_payload(payload, contract_data if step == "vervollstaendigen" else None)
            json_path = input_dir / f"personalbogen-{entry_id}.json"
            json_path.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")

            if step == "vervollstaendigen":
                if not contract_file or not contract_file.get("url"):
                    raise RuntimeError("Vertrag fehlt oder URL nicht vorhanden")
                contract_path = input_dir / "vertrag.pdf"
                download_contract(contract_file["url"], contract_path)
                LOGGER.info("Datei heruntergeladen: %s", contract_path.name)

                pdf_urls = payload.get("pdfUrls") if isinstance(payload, dict) else {}
                optional_sources = []
                if isinstance(pdf_urls, dict):
                    optional_sources.extend(
                        [
                            ("personalbogen", str(pdf_urls.get("personal") or "")),
                            ("zusatzvereinbarung", str(pdf_urls.get("zusatzvereinbarung") or "")),
                            ("sicherheitsbelehrung", str(pdf_urls.get("sicherheitsbelehrung") or "")),
                        ]
                    )

                uploads = payload.get("uploads") if isinstance(payload, dict) else {}
                if isinstance(uploads, dict):
                    for key, stem in [("immatrikulation", "immatrikulation"), ("infektionsschutz", "infektionsschutz")]:
                        meta = uploads.get(key)
                        if isinstance(meta, dict):
                            optional_sources.append((stem, str(meta.get("url") or "")))

                if not optional_sources:
                    LOGGER.info("Keine optionalen Dokument-Quellen im Payload gefunden.")
                for stem, source_url in optional_sources:
                    if not source_url:
                        LOGGER.info("Optionale Quelle '%s' fehlt (keine URL).", stem)
                        continue
                    try:
                        downloaded = download_optional_file(source_url, stem, input_dir)
                        if downloaded:
                            LOGGER.info("Datei heruntergeladen: %s", downloaded.name)
                    except Exception as exc:
                        LOGGER.warning("Optionale Datei '%s' konnte nicht geladen werden: %s", stem, exc)

            env = os.environ.copy()
            env["PERSO_INPUT_DIR"] = str(input_dir)

            result = run_playwright(command, env=env)
            log_chunks.append("=== STDOUT ===\n" + (result.stdout or "").strip())
            log_chunks.append("=== STDERR ===\n" + (result.stderr or "").strip())

            if result.returncode != 0:
                raise RuntimeError(f"Scraper Exit-Code {result.returncode}")

        duration = round(time.time() - started_at, 2)
        summary = {"durationSeconds": duration, "step": step, "entryId": entry_id}
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
        summary = {"durationSeconds": duration, "step": step, "entryId": entry_id}
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
    LOGGER.info("Starte Personalfragebogen-Scraper-Poller (Claim: %s)", CLAIM_ENDPOINT)
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
