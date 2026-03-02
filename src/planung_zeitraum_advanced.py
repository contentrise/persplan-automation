"""
Öffnet planung.php, setzt den Zeitraum auf „heute bis in X Tagen“ und exportiert offene Schichten.
"""

from __future__ import annotations

import csv
import json
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

DDB_TABLE = os.getenv("PLANUNG_ADV_TABLE", "").strip()
DDB_PK_FIELD = (os.getenv("PLANUNG_ADV_PK_FIELD") or "pk").strip() or "pk"
DDB_SK_FIELD = (os.getenv("PLANUNG_ADV_SK_FIELD") or "sk").strip() or "sk"
DDB_RUN_PREFIX = (os.getenv("PLANUNG_ADV_RUN_PREFIX") or "RUN#").strip() or "RUN#"
DDB_EVENT_PREFIX = (os.getenv("PLANUNG_ADV_EVENT_PREFIX") or "EVENT#").strip() or "EVENT#"
DDB_META_PK = (os.getenv("PLANUNG_ADV_META_PK") or "META").strip() or "META"
DDB_META_SK_LATEST = (os.getenv("PLANUNG_ADV_META_SK_LATEST") or "LATEST").strip() or "LATEST"
DDB_RUN_META_SK = (os.getenv("PLANUNG_ADV_RUN_META_SK") or "META").strip() or "META"
WINDOW_START = (os.getenv("PLANUNG_ADV_WINDOW_START") or "06:00").strip()
WINDOW_END = (os.getenv("PLANUNG_ADV_WINDOW_END") or "18:00").strip()
WINDOW_ENABLED = os.getenv("PLANUNG_ADV_WINDOW_ENABLED")
EVENT_TIMEOUT_SECONDS = float(os.getenv("PLANUNG_ADV_EVENT_TIMEOUT_SEC", "15") or 15)


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
            "anfragen_json": "",
            "schichten_json": "",
        }
        events.append(cleaned)
    return events


def _get_list_signature(target) -> dict[str, Any] | None:
    return target.evaluate(
        """
        () => {
            const list = document.querySelector("#mitarbeiterListeNamen");
            if (!list) return null;
            const items = Array.from(list.children || []);
            const first = items[0] ? items[0].textContent || "" : "";
            const last = items[items.length - 1] ? items[items.length - 1].textContent || "" : "";
            return {
                count: items.length,
                first: first.trim(),
                last: last.trim(),
            };
        }
        """
    )


def _wait_for_list_change(target, previous: dict[str, Any] | None, timeout_ms: int = 12000) -> None:
    if previous is None:
        return
    try:
        target.wait_for_function(
            """
            (prev) => {
                const list = document.querySelector("#mitarbeiterListeNamen");
                if (!list) return false;
                const items = Array.from(list.children || []);
                const first = items[0] ? items[0].textContent || "" : "";
                const last = items[items.length - 1] ? items[items.length - 1].textContent || "" : "";
                const count = items.length;
                return count !== prev.count || first.trim() !== prev.first || last.trim() !== prev.last;
            }
            """,
            arg=previous,
            timeout=timeout_ms,
        )
    except Exception:
        pass


def _ensure_anfragen_filter(target) -> None:
    group_button = target.locator("#mitarbeiterListeFilter button:has(img.group_add)").first
    settings_button = target.locator("#mitarbeiterListeFilter button:has(img.settings)").first
    group_button.wait_for(state="attached", timeout=10000)
    settings_button.wait_for(state="attached", timeout=10000)

    def _cls(locator) -> str:
        return locator.get_attribute("class") or ""

    before_settings = _cls(settings_button)
    before_group = _cls(group_button)
    pre_sig = _get_list_signature(target)
    print(f"[DEBUG] Filter-Status vorher: settings='{before_settings}', anfragen='{before_group}', liste={pre_sig}")

    if "filterOn" in before_settings:
        print("[DEBUG] Schalte settings-Filter aus …")
        settings_button.click(force=True)

    after_settings = _cls(settings_button)
    if "filterOn" in after_settings:
        print(f"[WARNUNG] settings-Filter bleibt AN: '{after_settings}'")
    else:
        print(f"[DEBUG] settings-Filter AUS: '{after_settings}'")

    if "filterOn" not in _cls(group_button):
        print("[DEBUG] Schalte Anfragen-Filter an …")
        group_button.click(force=True)

    after_group = _cls(group_button)
    if "filterOn" not in after_group:
        print(f"[WARNUNG] Anfragen-Filter bleibt AUS: '{after_group}'")
    else:
        print(f"[DEBUG] Anfragen-Filter AN: '{after_group}'")

    _wait_for_list_change(target, pre_sig, timeout_ms=12000)
    post_sig = _get_list_signature(target)
    print(f"[DEBUG] Liste nach Filter: {post_sig}")


def _extract_anfragen_list(target) -> List[Dict[str, Any]]:
    return target.evaluate(
        """
        () => {
            const list = document.querySelector("#mitarbeiterListeNamen");
            if (!list) return [];
            const cleanup = (value) =>
                value ? value.replace(/\\s+/g, " ").trim() : "";
            const entries = Array.from(list.children);
            return entries.map((row) => {
                const nameEl = row.querySelector("span[style*='font-weight: 700']");
                const cityEl = row.querySelector("span[title^='Wohnort'] span");
                const requestDateEl = row.querySelector("span[title*='Anfage gestellt am']");
                const availabilityEl = row.querySelector("span[title='Leistungsbereitschaft']");
                const dialogEl = row.querySelector("span[id^='dialog_']");
                return {
                    name: cleanup(nameEl ? nameEl.textContent : ""),
                    city: cleanup(cityEl ? cityEl.textContent : ""),
                    request_date: cleanup(requestDateEl ? requestDateEl.textContent : "").replace(/\\[|\\]/g, ""),
                    availability: cleanup(availabilityEl ? availabilityEl.textContent : ""),
                    info: cleanup(dialogEl ? dialogEl.textContent : ""),
                };
            });
        }
        """
    )


def _wait_for_anfragen_list(target, timeout_ms: int = 15000) -> None:
    try:
        target.wait_for_function(
            """
            () => {
                const list = document.querySelector("#mitarbeiterListeNamen");
                if (!list) return false;
                return list.children !== null;
            }
            """,
            timeout=timeout_ms,
        )
    except Exception:
        pass


def _dump_debug_state(page: Page, event_id: str, reason: str) -> None:
    export_dir = Path(config.EXPORT_DIR) / "debug"
    export_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    safe_event = re.sub(r"[^A-Za-z0-9_-]", "_", event_id or "event")
    base = export_dir / f"planung_adv_{safe_event}_{timestamp}"
    try:
        page.screenshot(path=str(base.with_suffix(".png")), full_page=True)
    except Exception as exc:
        print(f"[WARNUNG] Screenshot fehlgeschlagen ({event_id}): {exc}")
    try:
        html = page.content()
        base.with_suffix(".html").write_text(html, encoding="utf-8")
    except Exception as exc:
        print(f"[WARNUNG] HTML-Dump fehlgeschlagen ({event_id}): {exc}")
    base.with_suffix(".txt").write_text(reason, encoding="utf-8")


def _extract_schichten_table(target) -> List[Dict[str, Any]]:
    return target.evaluate(
        """
        () => {
            const table = document.querySelector("#tblSchichtDaten");
            if (!table) return [];
            const bodyRows = Array.from(table.querySelectorAll("tbody tr"))
                .filter((tr) => tr.querySelector("input[id^='cb_']"));
            const cleanup = (value) =>
                value ? value.replace(/\\s+/g, " ").trim() : "";

            return bodyRows.map((row) => {
                const cells = row.querySelectorAll("td");
                const idInput = row.querySelector("input[id^='cb_']");
                const schichtId = idInput ? idInput.id.replace("cb_", "") : "";
                const funktion = cleanup(cells[1]?.textContent || "");
                const position = cleanup(cells[2]?.textContent || "");
                const pauschal = cleanup(cells[3]?.textContent || "");
                const zeiten = cleanup(cells[4]?.textContent || "");
                const assignedCell = row.querySelector("td.schichtZeitZelle[style*='green'] div");
                const assigned = cleanup(assignedCell?.textContent || "");

                return {
                    schicht_id: schichtId,
                    funktion,
                    position,
                    pauschal,
                    zeiten,
                    disponiert: assigned,
                };
            });
        }
        """
    )


def _collect_anfragen_for_event(page: Page, frame: Frame, event_id: str) -> List[Dict[str, Any]]:
    start_ts = time.monotonic()
    href = frame.evaluate(
        """
        (id) => {
            const row = document.querySelector(`tr[data-id='${id}']`);
            if (!row) return null;
            const link = row.querySelector("a[href*='planung_intraday.php']");
            return link ? link.getAttribute("href") : null;
        }
        """,
        event_id,
    )
    if not href:
        raise RuntimeError(f"Event-Link nicht gefunden: {event_id}")

    url = urljoin(config.BASE_URL, href)
    print(f"[DEBUG] Detail-URL für Event {event_id}: {url}")
    detail = page.context.new_page()
    try:
        detail.goto(url, wait_until="domcontentloaded", timeout=int(EVENT_TIMEOUT_SECONDS * 1000))
        try:
            detail.wait_for_load_state("networkidle", timeout=int(EVENT_TIMEOUT_SECONDS * 1000))
        except Exception:
            pass
        remaining = max(EVENT_TIMEOUT_SECONDS - (time.monotonic() - start_ts), 1)
        detail.wait_for_selector("#mitarbeiterListeFilter", timeout=int(remaining * 1000))
        _ensure_anfragen_filter(detail)
        sig = _get_list_signature(detail)
        if sig and sig.get("count", 0) == 0:
            print("[DEBUG] Keine Anfragen gefunden – überspringe Wartezeit.")
        else:
            remaining = max(EVENT_TIMEOUT_SECONDS - (time.monotonic() - start_ts), 1)
            _wait_for_anfragen_list(detail, timeout_ms=int(remaining * 1000))
            time.sleep(0.8)
        anfragen = _extract_anfragen_list(detail)
        schichten = _extract_schichten_table(detail)
        print(f"[DEBUG] Anfragen-Liste Größe: {len(anfragen)}")
        print(f"[DEBUG] Schichten gelesen: {len(schichten)}")
        return anfragen, schichten
    except Exception as exc:
        duration = time.monotonic() - start_ts
        reason = f"{type(exc).__name__}: {exc}"
        print(f"[WARNUNG] Event {event_id} abgebrochen nach {duration:.1f}s: {reason}")
        _dump_debug_state(detail, event_id, reason)
        raise
    finally:
        detail.close()


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
        "anfragen_json",
        "schichten_json",
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


def _truthy_env(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_hour_minute(value: str) -> tuple[int, int] | None:
    match = re.match(r"^([01]?\\d|2[0-3]):([0-5]\\d)$", value.strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _within_window(now: datetime) -> bool:
    start = _parse_hour_minute(WINDOW_START or "06:00")
    end = _parse_hour_minute(WINDOW_END or "18:00")
    if not start or not end:
        return True
    start_minutes = start[0] * 60 + start[1]
    end_minutes = end[0] * 60 + end[1]
    now_minutes = now.hour * 60 + now.minute
    if start_minutes <= end_minutes:
        return start_minutes <= now_minutes <= end_minutes
    return now_minutes >= start_minutes or now_minutes <= end_minutes


def _write_events_to_dynamodb(events: List[Dict[str, Any]]) -> str | None:
    if not DDB_TABLE:
        print("[INFO] PLANUNG_ADV_TABLE nicht gesetzt – DynamoDB-Upload übersprungen.")
        return None

    snapshot_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    run_id = snapshot_at.replace(":", "-")
    run_pk = f"{DDB_RUN_PREFIX}{run_id}"

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DDB_TABLE)

    def _clean_int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    with table.batch_writer() as batch:
        for idx, event in enumerate(events, start=1):
            event_id = (event.get("event_id") or "").strip() or f"event-{idx}"
            item = {
                DDB_PK_FIELD: run_pk,
                DDB_SK_FIELD: f"{DDB_EVENT_PREFIX}{event_id}",
                "run_id": run_id,
                "snapshot_at": snapshot_at,
                "snapshot_date": snapshot_at[:10],
                "event_id": event_id,
                "title": (event.get("title") or "").strip(),
                "timeframe": (event.get("timeframe") or "").strip(),
                "customer": (event.get("customer") or "").strip(),
                "address": (event.get("address") or "").strip(),
                "besetzt": _clean_int(event.get("besetzt")),
                "gesamt": _clean_int(event.get("gesamt")),
                "anfragen": _clean_int(event.get("anfragen")),
                "offen": _clean_int(event.get("offen")),
                "anfragen_json": event.get("anfragen_json") or "[]",
                "schichten_json": event.get("schichten_json") or "[]",
            }
            batch.put_item(Item=item)

        batch.put_item(
            Item={
                DDB_PK_FIELD: run_pk,
                DDB_SK_FIELD: DDB_RUN_META_SK,
                "run_id": run_id,
                "snapshot_at": snapshot_at,
                "snapshot_date": snapshot_at[:10],
                "events": len(events),
            }
        )

    table.put_item(
        Item={
            DDB_PK_FIELD: DDB_META_PK,
            DDB_SK_FIELD: DDB_META_SK_LATEST,
            "run_id": run_id,
            "snapshot_at": snapshot_at,
            "snapshot_date": snapshot_at[:10],
            "events": len(events),
        }
    )

    print(f"[OK] DynamoDB Snapshot gespeichert: {DDB_TABLE} ({len(events)} Events)")
    return run_id


def run_planung_zeitraum(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    days_forward: int = 21,
    hold_seconds: int = 5,
    upload_s3: bool = False,
    compute_delta: bool = False,
    write_ddb: bool = False,
    ignore_window: bool = False,
) -> Path | None:
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms
    state_path = _require_login_state()
    days_forward = max(days_forward, 0)
    write_ddb = write_ddb or _truthy_env(os.getenv("PLANUNG_ADV_WRITE_DDB"))
    enforce_window = _truthy_env(WINDOW_ENABLED) if WINDOW_ENABLED is not None else True

    if enforce_window and not ignore_window:
        now = datetime.now()
        if not _within_window(now):
            print(
                f"[INFO] Aktuelle Zeit {now.strftime('%H:%M')} liegt außerhalb des Fensters "
                f"{WINDOW_START} - {WINDOW_END}. Lauf übersprungen."
            )
            return None

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
                print("[INFO] Lade Anfragen je Veranstaltung …")
                for idx, event in enumerate(events, start=1):
                    event_id = (event.get("event_id") or "").strip()
                    if not event_id:
                        continue
                    event_start = time.monotonic()
                    print(f"[INFO] Starte Event {idx}/{len(events)}: {event_id}")
                    try:
                        anfragen, schichten = _collect_anfragen_for_event(page, frame, event_id)
                        event["anfragen_json"] = json.dumps(anfragen)
                        event["schichten_json"] = json.dumps(schichten)
                        duration = time.monotonic() - event_start
                        print(
                            f"[OK] {idx}/{len(events)} Event {event_id}: {len(anfragen)} Anfragen ({duration:.1f}s)"
                        )
                    except Exception as exc:
                        duration = time.monotonic() - event_start
                        print(f"[WARNUNG] Anfragen für Event {event_id} nicht lesbar: {exc}")
                        print(f"[WARNUNG] Event {event_id} übersprungen ({duration:.1f}s)")
                        event["anfragen_json"] = "[]"
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
            if write_ddb:
                _write_events_to_dynamodb(events or [])
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
