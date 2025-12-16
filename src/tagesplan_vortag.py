"""
Tagesplan-Reporter:
- Öffnet den „Tagesplan (alt)“
- Setzt beide Datumsfelder auf den gewünschten Tag (Default: gestern)
- Liest alle Veranstaltungen/Mitarbeiter aus
- Schreibt Schichtzeiten, Stempelzeiten + Notiz in eine CSV
"""

from __future__ import annotations

import csv
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import List, Dict, Any, Tuple

import boto3
from playwright.sync_api import Frame, sync_playwright

from src import config
from src.schicht_bestaetigen import open_tagesplan_alt

S3_BUCKET = os.getenv("CHECKIN_BUCKET", "greatstaff-data-storage").strip()
S3_PREFIX = os.getenv("CHECKIN_PREFIX", "einchecken").strip().strip("/")


def _require_login_state() -> Path:
    state_path = Path(config.STATE_PATH)
    if not state_path.exists():
        raise RuntimeError(
            f"[FEHLER] Kein gespeicherter Login-State unter {state_path}. Bitte zuerst 'login' ausführen."
        )
    return state_path


def _resolve_target_date(days_back: int, explicit_date: str | None) -> str:
    if explicit_date:
        try:
            datetime.strptime(explicit_date, "%d.%m.%Y")
        except ValueError as exc:
            raise RuntimeError("--date muss im Format TT.MM.JJJJ angegeben werden.") from exc
        return explicit_date
    days_back = max(days_back, 0)
    target = datetime.now() - timedelta(days=days_back or 0)
    return target.strftime("%d.%m.%Y")


def _set_date(frame: Frame, field_name: str, value: str) -> None:
    selector = f"input[name='{field_name}']"
    input_el = frame.locator(selector).first
    input_el.wait_for(state="visible", timeout=8000)
    input_el.click()
    input_el.fill(value)
    time.sleep(0.2)


def _click_anzeigen(frame: Frame) -> None:
    button = frame.locator("input[name='timestamp_auswahl_anzeigen']").first
    button.wait_for(state="visible", timeout=5000)
    button.click()


def _wait_for_refresh(frame: Frame) -> None:
    frame.wait_for_selector("tr[id^='tr_schicht_']", timeout=20000)


def _shift_date_string(date_str: str, days: int) -> str | None:
    try:
        base = datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        return None
    target = base + timedelta(days=days)
    return target.strftime("%d.%m.%Y")


def _load_day(frame: Frame, day: str, label: str) -> int:
    print(f"[INFO] Lade Tagesplan für {day} ({label}) …")
    _set_date(frame, "timestamp_von", day)
    _set_date(frame, "timestamp_bis", day)
    _click_anzeigen(frame)
    _wait_for_refresh(frame)
    total_rows = _load_all_rows(frame)
    print(f"[INFO] {label}: {total_rows} Zeilen erkannt.")
    return total_rows


def _load_all_rows(frame: Frame, max_scrolls: int = 60) -> int:
    locator = frame.locator("tr[id^='tr_schicht_']")
    last_count = locator.count()
    stable_rounds = 0
    for _ in range(max_scrolls):
        try:
            frame.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            pass
        time.sleep(0.5)
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
    return last_count


def _extract_shift_records(frame: Frame) -> List[Dict[str, Any]]:
    return frame.evaluate(
        """
        () => {
            const rows = Array.from(document.querySelectorAll("tr[id^='tr_schicht_']"));
            return rows.map((row) => {
                const roleCell = row.querySelector("td");
                const role = roleCell ? roleCell.textContent.trim() : "";

                let header = row.previousElementSibling;
                let eventText = "";
                while (header) {
                    const th = header.querySelector("th.zelle_objekt_header");
                    if (th) {
                        eventText = th.innerText.replace(/\\s+/g, " ").trim();
                        break;
                    }
                    header = header.previousElementSibling;
                }

                const nameCell =
                    row.querySelector("td[id^='row_']") ||
                    row.querySelectorAll("td")[2] ||
                    null;

                let employee = "";
                let phone = "";
                if (nameCell) {
                    const clone = nameCell.cloneNode(true);
                    clone.querySelectorAll("img").forEach((img) => img.remove());
                    const tel = clone.querySelector("a[href^='tel:']");
                    if (tel) {
                        phone = tel.textContent.replace(/\\s+/g, "");
                        tel.remove();
                    }
                    employee = clone.textContent.replace(/\\s+/g, " ").trim();
                }

                const shiftCell = row.querySelector("td[id^='td_schicht_']");
                const shiftTime = shiftCell
                    ? shiftCell.textContent.replace(/\\s+/g, " ").trim()
                    : "";

                const clockImg = row.querySelector("div[id^='dak_tms_'] img");
                const clockText = clockImg
                    ? (clockImg.getAttribute("title") || clockImg.getAttribute("alt") || "").trim()
                    : "";

                return {
                    rowId: row.id,
                    eventText,
                    role,
                    employee,
                    phone,
                    shiftTime,
                    clockText,
                    dateRange: "",
                };
            });
        }
        """
    )


def _normalize_clock_text(value: str) -> str:
    if not value:
        return ""
    text = value.replace("\r", " ").replace("\n", " ").strip()
    return " ".join(text.split())


def _extract_times(value: str) -> list[int]:
    if not value:
        return []
    matches = re.findall(r"(\d{1,2}):(\d{2})", value)
    result: list[int] = []
    for hour_raw, minute_raw in matches:
        hour = int(hour_raw)
        minute = int(minute_raw)
        if 0 <= hour < 24 and 0 <= minute < 60:
            result.append(hour * 60 + minute)
    return result


def _extract_first_time(value: str) -> int | None:
    times = _extract_times(value)
    return times[0] if times else None


def _extract_last_time(value: str) -> int | None:
    times = _extract_times(value)
    return times[-1] if times else None


def _extract_shift_bounds(value: str) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    times = _extract_times(value)
    if not times:
        return None, None
    start = times[0]
    end = times[1] if len(times) >= 2 else None
    if end is not None and end <= start:
        end += 24 * 60
    return start, end


def _classify_check_in(shift_start: int | None, check_in: int | None) -> str:
    if check_in is None:
        return "Garnicht eingecheckt"
    if shift_start is None:
        return "Schichtzeit unbekannt"
    delta = check_in - shift_start
    if delta <= 0:
        return "Pünktlich eingecheckt"
    if delta <= 30:
        return "Eingecheckt mit 30 Minuten Toleranz"
    return "Zu spät eingecheckt"


def _normalize_checkout_minutes(check_out: int | None, shift_end: int | None) -> int | None:
    if check_out is None or shift_end is None:
        return check_out
    if shift_end >= 24 * 60 and check_out <= (shift_end - 24 * 60):
        return check_out + 24 * 60
    return check_out


def _classify_check_out(shift_end: int | None, check_out: int | None) -> str:
    if check_out is None:
        return "Garnicht ausgestempelt"
    if shift_end is None:
        return "Schichtzeit unbekannt"

    delta = check_out - shift_end
    if delta == 0:
        return "Pünktlich ausgestempelt"
    if delta > 0:
        if delta <= 30:
            return "Ausgestempelt mit 30 Minuten Toleranz"
        return "Zu spät ausgestempelt"
    # delta < 0
    if abs(delta) <= 30:
        return "Zu früh ausgestempelt (<=30 Min)"
    return "Zu früh ausgestempelt"


def _format_date_range(base_day: datetime, shift_start: int | None, shift_end: int | None) -> str:
    start_day = base_day
    if shift_end is None:
        end_day = start_day
    else:
        additional_days = shift_end // (24 * 60)
        end_day = start_day + timedelta(days=additional_days)
    if end_day.date() == start_day.date():
        return start_day.strftime("%d.%m.%Y")
    if start_day.year == end_day.year and start_day.month == end_day.month:
        return f"{start_day.strftime('%d')}-{end_day.strftime('%d.%m.%Y')}"
    return f"{start_day.strftime('%d.%m.%Y')} - {end_day.strftime('%d.%m.%Y')}"


def _normalize_key(value: str) -> str:
    return " ".join(value.lower().split()) if value else ""


def _build_shift_lookup(records: List[Dict[str, Any]]) -> Tuple[Dict[Tuple[str, str], str], Dict[str, str]]:
    by_event: Dict[Tuple[str, str], str] = {}
    by_name: Dict[str, str] = {}
    for record in records:
        shift = (record.get("shiftTime") or "").strip()
        if not shift:
            continue
        name_key = _normalize_key(record.get("employee", ""))
        event_key = _normalize_key(record.get("eventText", ""))
        if name_key and event_key and (name_key, event_key) not in by_event:
            by_event[(name_key, event_key)] = shift
        if name_key and name_key not in by_name:
            by_name[name_key] = shift
    return by_event, by_name


def _fill_missing_shift_times(
    records: List[Dict[str, Any]],
    fallback_records: List[Dict[str, Any]],
    fallback_label: str,
) -> int:
    if not fallback_records:
        return 0
    by_event, by_name = _build_shift_lookup(fallback_records)
    filled = 0
    for record in records:
        if record.get("shiftTime"):
            continue
        name_key = _normalize_key(record.get("employee", ""))
        event_key = _normalize_key(record.get("eventText", ""))
        shift = None
        if name_key:
            shift = by_event.get((name_key, event_key)) or by_name.get(name_key)
        if shift:
            record["shiftTime"] = shift
            filled += 1
    if filled:
        print(f"[INFO] {filled} Schichtzeiten via {fallback_label} ergänzt.")
    else:
        print(f"[WARNUNG] Keine passenden Schichtzeiten im {fallback_label} gefunden.")
    return filled


def _postprocess_records(records: List[Dict[str, Any]], base_day: datetime) -> List[Dict[str, Any]]:
    processed = []
    for record in records:
        shift_time = record.get("shiftTime", "")
        clock_text = _normalize_clock_text(record.get("clockText", ""))
        notes = []
        if not shift_time:
            notes.append("keine schichtzeiten")
        if not clock_text:
            notes.append("keine einstempelzeit")
        shift_start_minutes, shift_end_minutes = _extract_shift_bounds(shift_time)
        clock_start_minutes = _extract_first_time(clock_text)
        clock_end_minutes = _extract_last_time(clock_text)
        calibrated_checkout = _normalize_checkout_minutes(clock_end_minutes, shift_end_minutes)
        comment_in = _classify_check_in(shift_start_minutes, clock_start_minutes)
        comment_out = _classify_check_out(shift_end_minutes, calibrated_checkout)
        date_label = _format_date_range(base_day, shift_start_minutes, shift_end_minutes)
        processed.append(
            {
                "datum": date_label,
                "veranstaltung": record.get("eventText", ""),
                "rolle": record.get("role", ""),
                "mitarbeiter": record.get("employee", ""),
                "telefon": record.get("phone", ""),
                "schichtzeit": shift_time,
                "eingestempelt": clock_text,
                "notiz": ", ".join(notes),
                "kommentar_einstempeln": comment_in,
                "kommentar_ausstempeln": comment_out,
            }
        )
    return processed


def _target_date_slug(date_str: str) -> str:
    try:
        parsed = datetime.strptime(date_str, "%d.%m.%Y")
        return parsed.strftime("%Y-%m-%d")
    except ValueError:
        return date_str.replace(".", "-")


def _write_csv(rows: List[Dict[str, Any]], target_date: str) -> Path:
    export_dir = Path(config.EXPORT_DIR or "exports")
    export_dir.mkdir(parents=True, exist_ok=True)
    slug = _target_date_slug(target_date)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = export_dir / f"tagesplan_{slug}_{timestamp}.csv"
    fieldnames = [
        "datum",
        "veranstaltung",
        "rolle",
        "mitarbeiter",
        "telefon",
        "schichtzeit",
        "eingestempelt",
        "notiz",
        "kommentar_einstempeln",
        "kommentar_ausstempeln",
    ]
    with open(path, "w", encoding="utf-8", newline="") as handler:
        writer = csv.DictWriter(handler, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def _upload_csv_to_s3(csv_path: Path, target_date: str) -> str | None:
    if not S3_BUCKET:
        print("[INFO] CHECKIN_BUCKET nicht gesetzt – S3-Upload übersprungen.")
        return None

    slug = _target_date_slug(target_date)
    key_parts = [part for part in (S3_PREFIX, slug, csv_path.name) if part]
    key = "/".join(key_parts)

    s3 = boto3.client("s3")
    try:
        s3.upload_file(str(csv_path), S3_BUCKET, key)
    except Exception as exc:
        print(f"[WARNUNG] Upload nach S3 fehlgeschlagen: {exc}")
        return None

    print(f"[OK] CSV in S3 gespeichert: s3://{S3_BUCKET}/{key}")
    return key


def run_tagesplan_vortag(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    hold_seconds: int = 5,
    days_back: int = 1,
    explicit_date: str | None = None,
) -> Path:
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms
    state_path = _require_login_state()
    target_date = _resolve_target_date(days_back, explicit_date)
    target_dt = datetime.strptime(target_date, "%d.%m.%Y")
    print(f"[INFO] Stelle Tagesplan für {target_date} ein …")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        context = browser.new_context(storage_state=str(state_path))
        page = context.new_page()
        page.goto(config.BASE_URL, wait_until="load")

        try:
            frame = open_tagesplan_alt(page)
            _load_day(frame, target_date, "Zieldatum")
            records = _extract_shift_records(frame)
            print(f"[INFO] Anzahl Schicht-Zeilen im Export: {len(records)}")

            missing_shift_records = [rec for rec in records if not rec.get("shiftTime")]
            fallback_loaded = False
            if missing_shift_records:
                fallback_day = _shift_date_string(target_date, -1)
                if fallback_day:
                    print(
                        f"[INFO] {len(missing_shift_records)} Zeilen ohne Schichtzeit – "
                        f"suche Infos am Vortag {fallback_day}."
                    )
                    _load_day(frame, fallback_day, "Fallback (Vortag)")
                    fallback_records = _extract_shift_records(frame)
                    _fill_missing_shift_times(records, fallback_records, "Vortag")
                    fallback_loaded = True
                else:
                    print("[WARNUNG] Konnte Vortag nicht berechnen – überspringe Fallback.")

            processed = _postprocess_records(records, target_dt)
            missing_shift = sum(1 for row in processed if not row["schichtzeit"])
            missing_clock = sum(1 for row in processed if not row["eingestempelt"])
            if missing_shift or missing_clock:
                print(
                    f"[INFO] {missing_shift} Einträge ohne Schichtzeit, "
                    f"{missing_clock} ohne Einstempelzeit."
                )

            if fallback_loaded:
                _load_day(frame, target_date, "Zieldatum (zur Anzeige)")

            csv_path = _write_csv(processed, target_date)
            _upload_csv_to_s3(csv_path, target_date)
            print(f"[OK] CSV erstellt: {csv_path}")
            if hold_seconds > 0:
                print(f"[INFO] Warte noch {hold_seconds} Sekunden zur manuellen Kontrolle …")
                time.sleep(hold_seconds)
            return csv_path
        finally:
            print("[INFO] Browser wird geschlossen …")
            browser.close()


def main():
    run_tagesplan_vortag()


if __name__ == "__main__":
    main()
