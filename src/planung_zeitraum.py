"""
Öffnet planung.php, setzt den Zeitraum auf „heute bis in X Tagen“ und exportiert offene Schichten.
"""

from __future__ import annotations

import csv
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
import time
from typing import Any, Dict, List

import boto3
from urllib.parse import urljoin

from playwright.sync_api import Frame, Page, sync_playwright

from src import config

S3_BUCKET = os.getenv("PLANUNG_BUCKET", "greatstaff-data-storage").strip()
S3_PREFIX = os.getenv("PLANUNG_PREFIX", "planung/offene").strip().strip("/")
S3_DELTA_PREFIX = os.getenv("PLANUNG_DELTA_PREFIX", "planung/anfragen-delta").strip().strip("/")


def _require_login_state() -> Path:
    state_path = Path(config.STATE_PATH)
    if not state_path.exists():
        raise RuntimeError(
            f"[FEHLER] Kein gespeicherter Login-State unter {state_path}. "
            "Bitte zuerst 'login' ausführen."
        )
    return state_path


def _wait_for_inhalt_frame(page: Page, timeout_seconds: int = 20) -> Frame:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        frame = page.frame(name="inhalt")
        if frame:
            return frame
        time.sleep(0.5)
    raise RuntimeError("[FEHLER] Frame 'inhalt' wurde nicht gefunden.")


def _open_planung(page: Page) -> Frame:
    frame = _wait_for_inhalt_frame(page, timeout_seconds=25)
    target = urljoin(config.BASE_URL, "planung.php")
    print(f"[INFO] Lade planung.php ({target}) …")
    frame.goto(target, wait_until="domcontentloaded", timeout=30000)
    frame.wait_for_selector("form#planungAnzeige", timeout=20000)
    print("[OK] Formular 'planungAnzeige' geladen.")
    return frame


def _format_date(value: datetime) -> str:
    return value.strftime("%d.%m.%Y")


def _fill_date(frame: Frame, field_name: str, value: str) -> None:
    locator = frame.locator(f"form#planungAnzeige input[name='{field_name}']").first
    locator.wait_for(state="visible", timeout=8000)
    locator.click()
    locator.fill(value)
    time.sleep(0.2)


def _submit_zeitraum(frame: Frame) -> None:
    submit = frame.locator(
        "form#planungAnzeige input[name='datum_suche'][value='Zeitraum anzeigen']"
    ).first
    submit.wait_for(state="attached", timeout=8000)
    print("[AKTION] Klicke auf 'Zeitraum anzeigen' …")
    submit.click()
    frame.wait_for_load_state("networkidle", timeout=20000)
    print("[OK] Zeitraum angewendet.")


def _load_event_table(frame: Frame, max_scrolls: int = 60) -> int:
    """
    Scrollt die Tabelle komplett, damit nachgeladene Veranstaltungen erscheinen.
    """
    locator = frame.locator("tr[name^='tr_']")
    last_count = -1
    stable_rounds = 0
    for _ in range(max_scrolls):
        try:
            frame.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            pass
        time.sleep(0.6)
        current = locator.count()
        if current > last_count:
            last_count = current
            stable_rounds = 0
        else:
            stable_rounds += 1
            if stable_rounds >= 3:
                break
    try:
        frame.evaluate("window.scrollTo(0, 0)")
    except Exception:
        pass
    print(f"[INFO] Veranstaltungen geladen: {max(last_count, 0)} Zeilen erkannt.")
    return max(last_count, 0)


def _extract_event_rows(frame: Frame) -> List[Dict[str, Any]]:
    """
    Liest relevante Felder aus jeder Veranstaltungs-Zeile aus.
    """
    return frame.evaluate(
        """
        () => {
            const rows = Array.from(document.querySelectorAll("tr[name^='tr_']"));
            const parseSpanValue = (span) => {
                if (!span) return 0;
                const text = span.textContent || "";
                const match = text.match(/-?\\d+/);
                return match ? parseInt(match[0], 10) : 0;
            };
            const cleanup = (value) =>
                value ? value.replace(/\\s+/g, " ").trim() : "";

            return rows.map((row) => {
                const cells = row.querySelectorAll("td");
                const eventIdCell = cells[1];
                const timeframeCell = cells[4];
                const countsCell = cells[5];
                const customerCell = cells[6];
                const infoCell = cells[7];
                const addressCell = cells[8];
                const titleLink = infoCell ? infoCell.querySelector("a[href]") : null;

                const filledSpan = countsCell
                    ? countsCell.querySelector("span[title*='Besetzte Schichten']")
                    : null;
                const totalSpan = countsCell
                    ? countsCell.querySelector("span[title*='Anzahl der Schichten']")
                    : null;
                const requestSpan = countsCell
                    ? countsCell.querySelector("span[title*='Anfragen']")
                    : null;

                return {
                    eventId: cleanup(
                        eventIdCell
                            ? eventIdCell.textContent
                            : row.getAttribute("data-id") || ""
                    ),
                    title: cleanup(
                        titleLink
                            ? titleLink.textContent
                            : infoCell
                            ? infoCell.innerText
                            : ""
                    ),
                    timeframe: cleanup(timeframeCell ? timeframeCell.innerText : ""),
                    customer: cleanup(customerCell ? customerCell.innerText : ""),
                    address: cleanup(addressCell ? addressCell.innerText : ""),
                    filled: parseSpanValue(filledSpan),
                    total: parseSpanValue(totalSpan),
                    requests: parseSpanValue(requestSpan),
                };
            });
        }
        """
    )


def _prepare_event_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for row in rows:
        try:
            filled = int(row.get("filled") or 0)
            total = int(row.get("total") or 0)
            requests = int(row.get("requests") or 0)
        except (TypeError, ValueError):
            filled = total = requests = 0
        offen = max(total - filled, 0)
        cleaned = {
            "event_id": (row.get("eventId") or "").strip(),
            "title": (row.get("title") or "").strip(),
            "timeframe": (row.get("timeframe") or "").strip(),
            "customer": (row.get("customer") or "").strip(),
            "address": (row.get("address") or "").strip(),
            "besetzt": max(filled, 0),
            "gesamt": max(total, 0),
            "anfragen": max(requests, 0),
            "offen": offen,
        }
        events.append(cleaned)
    return events


def _write_open_events_csv(events: List[Dict[str, Any]]) -> Path:
    export_dir = Path(config.EXPORT_DIR)
    export_dir.mkdir(parents=True, exist_ok=True)
    csv_path = export_dir / f"planung_offene_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    fieldnames = [
        "event_id",
        "title",
        "timeframe",
        "customer",
        "address",
        "besetzt",
        "gesamt",
        "anfragen",
        "offen",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for event in events:
            writer.writerow(event)
    return csv_path


def _parse_snapshot_timestamp(path: Path) -> datetime | None:
    match = re.search(r"planung_offene_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})\.csv$", path.name)
    if not match:
        return None
    try:
        return datetime.strptime(f"{match.group(1)} {match.group(2)}", "%Y-%m-%d %H-%M-%S")
    except ValueError:
        return None


def _find_previous_snapshot(current_path: Path) -> Path | None:
    export_dir = Path(config.EXPORT_DIR)
    current_ts = _parse_snapshot_timestamp(current_path)
    if current_ts is None:
        return None

    candidates: list[tuple[datetime, Path]] = []
    for path in export_dir.glob("planung_offene_*.csv"):
        ts = _parse_snapshot_timestamp(path)
        if ts and ts < current_ts:
            candidates.append((ts, path))

    if not candidates:
        return None

    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def _event_key(row: Dict[str, Any]) -> str:
    event_id = (row.get("event_id") or "").strip()
    if event_id:
        return f"id:{event_id}"
    parts = [
        (row.get("title") or "").strip(),
        (row.get("timeframe") or "").strip(),
        (row.get("customer") or "").strip(),
        (row.get("address") or "").strip(),
    ]
    return "|".join(parts)


def _read_snapshot(path: Path) -> Dict[str, Dict[str, Any]]:
    rows: Dict[str, Dict[str, Any]] = {}
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cleaned = {
                "event_id": (row.get("event_id") or "").strip(),
                "title": (row.get("title") or "").strip(),
                "timeframe": (row.get("timeframe") or "").strip(),
                "customer": (row.get("customer") or "").strip(),
                "address": (row.get("address") or "").strip(),
                "besetzt": int(row.get("besetzt") or 0),
                "gesamt": int(row.get("gesamt") or 0),
                "anfragen": int(row.get("anfragen") or 0),
                "offen": int(row.get("offen") or 0),
            }
            rows[_event_key(cleaned)] = cleaned
    return rows


def _write_anfragen_delta_csv(rows: List[Dict[str, Any]]) -> Path:
    export_dir = Path(config.EXPORT_DIR)
    export_dir.mkdir(parents=True, exist_ok=True)
    csv_path = export_dir / f"planung_anfragen_delta_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    fieldnames = [
        "event_id",
        "title",
        "timeframe",
        "customer",
        "address",
        "anfragen_alt",
        "anfragen_neu",
        "delta",
        "besetzt",
        "gesamt",
        "offen",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return csv_path


def _upload_csv_to_s3(csv_path: Path, prefix: str) -> str | None:
    if not S3_BUCKET:
        print("[INFO] PLANUNG_BUCKET nicht gesetzt – S3-Upload übersprungen.")
        return None

    date_folder = datetime.now().strftime("%Y-%m-%d")
    key_parts = [part for part in (prefix, date_folder, csv_path.name) if part]
    key = "/".join(key_parts)

    s3 = boto3.client("s3")
    try:
        s3.upload_file(str(csv_path), S3_BUCKET, key)
    except Exception as exc:
        print(f"[WARNUNG] Upload nach S3 fehlgeschlagen: {exc}")
        return None

    print(f"[OK] CSV in S3 gespeichert: s3://{S3_BUCKET}/{key}")
    return key


def run_planung_zeitraum(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    days_forward: int = 21,
    hold_seconds: int = 5,
    upload_s3: bool = False,
    compute_delta: bool = False,
) -> Path | None:
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms
    state_path = _require_login_state()
    days_forward = max(days_forward, 0)

    start_date = datetime.now()
    end_date = start_date + timedelta(days=days_forward)
    start_str = _format_date(start_date)
    end_str = _format_date(end_date)

    print(f"[INFO] Stelle Zeitraum von {start_str} bis {end_str} ein …")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        context = browser.new_context(storage_state=str(state_path))
        page = context.new_page()
        page.goto(config.BASE_URL, wait_until="load")

        csv_path: Path | None = None
        try:
            frame = _open_planung(page)
            _fill_date(frame, "von", start_str)
            _fill_date(frame, "bis", end_str)
            _submit_zeitraum(frame)
            _load_event_table(frame)
            rows = _extract_event_rows(frame)
            events = _prepare_event_rows(rows)
            if events:
                csv_path = _write_open_events_csv(events)
                print(
                    f"[OK] {len(events)} Veranstaltungen exportiert: {csv_path}"
                )
            else:
                csv_path = _write_open_events_csv([])
                print("[INFO] Keine Veranstaltungen gefunden – CSV enthält nur Kopfzeile.")

            if csv_path and compute_delta:
                previous = _find_previous_snapshot(csv_path)
                if previous:
                    current_rows = _read_snapshot(csv_path)
                    prev_rows = _read_snapshot(previous)
                    delta_rows: List[Dict[str, Any]] = []
                    for key, current in current_rows.items():
                        prev = prev_rows.get(key)
                        prev_anfragen = prev.get("anfragen", 0) if prev else 0
                        delta = int(current.get("anfragen", 0)) - int(prev_anfragen)
                        if delta > 0:
                            delta_rows.append(
                                {
                                    "event_id": current.get("event_id", ""),
                                    "title": current.get("title", ""),
                                    "timeframe": current.get("timeframe", ""),
                                    "customer": current.get("customer", ""),
                                    "address": current.get("address", ""),
                                    "anfragen_alt": prev_anfragen,
                                    "anfragen_neu": current.get("anfragen", 0),
                                    "delta": delta,
                                    "besetzt": current.get("besetzt", 0),
                                    "gesamt": current.get("gesamt", 0),
                                    "offen": current.get("offen", 0),
                                }
                            )
                    delta_path = _write_anfragen_delta_csv(delta_rows)
                    print(
                        f"[OK] Delta-CSV erstellt ({len(delta_rows)} neue Anfragen): {delta_path}"
                    )
                    if upload_s3:
                        _upload_csv_to_s3(delta_path, S3_DELTA_PREFIX)
                else:
                    print("[INFO] Kein vorheriger Snapshot gefunden – Delta-CSV übersprungen.")

            if csv_path and upload_s3:
                _upload_csv_to_s3(csv_path, S3_PREFIX)
            if hold_seconds > 0:
                print(f"[INFO] Halte Browser für {hold_seconds} Sekunden offen …")
                time.sleep(hold_seconds)
            return csv_path
        finally:
            print("[INFO] Browser wird geschlossen …")
            browser.close()
    return None


def main():
    run_planung_zeitraum()


if __name__ == "__main__":
    main()
