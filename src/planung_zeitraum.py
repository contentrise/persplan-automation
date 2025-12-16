"""
Öffnet planung.php, setzt den Zeitraum auf „heute bis in X Tagen“ und exportiert offene Schichten.
"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path
import time
from typing import Any, Dict, List
from urllib.parse import urljoin

from playwright.sync_api import Frame, Page, sync_playwright

from src import config


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


def _prepare_open_events(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    open_events: List[Dict[str, Any]] = []
    for row in rows:
        try:
            filled = int(row.get("filled") or 0)
            total = int(row.get("total") or 0)
            requests = int(row.get("requests") or 0)
        except (TypeError, ValueError):
            filled = total = requests = 0
        open_count = max(total - filled, 0)
        if open_count <= 0:
            continue
        offen = open_count
        anfragen_clean = max(requests, 0)

        if offen <= 0:
            match_status = "voll besetzt"
            fehlend = 0
        elif anfragen_clean <= 0:
            match_status = "keine anfragen"
            fehlend = offen
        elif anfragen_clean >= offen:
            match_status = "kann komplett gedeckt werden"
            fehlend = 0
        else:
            match_status = "teilweise deckbar"
            fehlend = offen - anfragen_clean

        cleaned = {
            "event_id": (row.get("eventId") or "").strip(),
            "title": (row.get("title") or "").strip(),
            "timeframe": (row.get("timeframe") or "").strip(),
            "customer": (row.get("customer") or "").strip(),
            "address": (row.get("address") or "").strip(),
            "besetzt": max(filled, 0),
            "gesamt": max(total, 0),
            "anfragen": anfragen_clean,
            "offen": offen,
            "status": match_status,
            "fehlend_nach_anfragen": fehlend,
        }
        open_events.append(cleaned)
    return open_events


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
        "status",
        "fehlend_nach_anfragen",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for event in events:
            writer.writerow(event)
    return csv_path


def run_planung_zeitraum(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    days_forward: int = 21,
    hold_seconds: int = 5,
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
            open_events = _prepare_open_events(rows)
            if open_events:
                csv_path = _write_open_events_csv(open_events)
                print(
                    f"[OK] {len(open_events)} Veranstaltungen mit offenen Schichten exportiert: {csv_path}"
                )
            else:
                csv_path = _write_open_events_csv([])
                print("[INFO] Keine offenen Schichten gefunden – CSV enthält nur Kopfzeile.")
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
