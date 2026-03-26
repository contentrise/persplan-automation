import argparse
import time
import json
import calendar
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import Frame, Page, TimeoutError, sync_playwright

from src import config
from src.login import do_login

FIELD_TIMEOUT_MS = 5000
DEFAULT_EVENT_TYPE_LABEL = "Bürojob"
FALLBACK_EVENT_TYPE_LABELS = ["Bürojob", "Office"]
DEFAULT_CUSTOMER_LABEL = "GREATSTAFF"
DEFAULT_START_DATE = "07.06.2026"
DEFAULT_END_DATE = "07.06.2026"
DEFAULT_START_TIME = "10:00"
DEFAULT_END_TIME = "17:00"
DEFAULT_LOCATION = "Werinherstraße 43, 81541 München"
DEFAULT_NOTE_TEXT = "test"
RESULTS_PATH = Path("exports") / "va_anlage_result.jsonl"


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

def _find_frame_with_selector(page: Page, selector: str, timeout_seconds: int = 20) -> Frame:
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        for frame in page.frames:
            try:
                if frame.query_selector(selector):
                    return frame
            except Exception as exc:
                last_error = exc
        time.sleep(0.25)
    raise RuntimeError(f"[FEHLER] Selector nicht gefunden: {selector}. Last error: {last_error}")

def _find_frame_with_selector_or_none(page: Page, selector: str, timeout_seconds: int = 5) -> Frame | None:
    try:
        return _find_frame_with_selector(page, selector, timeout_seconds=timeout_seconds)
    except Exception:
        return None

def _dump_frame_debug(page: Page):
    try:
        print(f"[DEBUG] Page URL: {page.url}")
        print("[DEBUG] Verfügbare Frames:")
        for frame in page.frames:
            try:
                name = frame.name or "(no-name)"
                url = frame.url or "(no-url)"
                print(f"[DEBUG] - {name}: {url}")
            except Exception:
                pass
    except Exception:
        pass

def _dump_page_debug_artifacts(page: Page, label: str):
    try:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        out_dir = Path("exports")
        out_dir.mkdir(parents=True, exist_ok=True)
        html_path = out_dir / f"va_anlage_debug_{label}_{timestamp}.html"
        html_path.write_text(page.content(), encoding="utf-8")
        print(f"[DEBUG] HTML-Dump geschrieben: {html_path}")
        try:
            png_path = out_dir / f"va_anlage_debug_{label}_{timestamp}.png"
            page.screenshot(path=str(png_path), full_page=True)
            print(f"[DEBUG] Screenshot geschrieben: {png_path}")
        except Exception as exc:
            print(f"[WARNUNG] Screenshot fehlgeschlagen: {exc}")
    except Exception as exc:
        print(f"[WARNUNG] Debug-Artefakte konnten nicht geschrieben werden: {exc}")

def _find_frame_with_any_selector(page: Page, selectors: list[str], timeout_seconds: int = 20) -> tuple[Frame, str]:
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        for frame in page.frames:
            for selector in selectors:
                try:
                    if frame.query_selector(selector):
                        return frame, selector
                except Exception as exc:
                    last_error = exc
        time.sleep(0.25)
    raise RuntimeError(
        f"[FEHLER] Keiner der Selector gefunden: {selectors}. Last error: {last_error}"
    )

def _find_frame_with_any_selector_or_none(page: Page, selectors: list[str], timeout_seconds: int = 5) -> tuple[Frame, str] | None:
    try:
        return _find_frame_with_any_selector(page, selectors, timeout_seconds=timeout_seconds)
    except Exception:
        return None
def _debug_skip(label: str, reason: str):
    print(f"[DEBUG] Überspringe '{label}' ({reason}).")


def _safe_wait(locator, label: str) -> bool:
    try:
        locator.wait_for(state="visible", timeout=FIELD_TIMEOUT_MS)
        return True
    except TimeoutError:
        _debug_skip(label, "Timeout nach 5s")
    except Exception as exc:
        _debug_skip(label, f"Fehler: {exc}")
    return False


def _safe_fill(frame: Frame, selector: str, value: str, label: str):
    locator = frame.locator(selector).first
    if not _safe_wait(locator, label):
        return
    try:
        locator.click()
        locator.fill(value)
        print(f"[OK] {label} gesetzt: {value}")
    except Exception as exc:
        _debug_skip(label, f"Fehler beim Füllen: {exc}")


def _safe_select_label(frame: Frame, selector: str, label: str, field_label: str) -> bool:
    locator = frame.locator(selector).first
    if not _safe_wait(locator, field_label):
        return False
    try:
        locator.select_option(label=label)
        print(f"[OK] {field_label} gewählt: {label}")
        return True
    except Exception as exc:
        _debug_skip(field_label, f"Konnte '{label}' nicht wählen: {exc}")
    return False


def _safe_select_value(frame: Frame, selector: str, value: str, field_label: str) -> bool:
    locator = frame.locator(selector).first
    if not _safe_wait(locator, field_label):
        return False
    try:
        locator.select_option(value=value)
        print(f"[OK] {field_label} gewählt (value={value})")
        return True
    except Exception as exc:
        _debug_skip(field_label, f"Konnte value '{value}' nicht wählen: {exc}")
    return False


def _safe_check(frame: Frame, selector: str, label: str, checked: bool = True):
    locator = frame.locator(selector).first
    if not _safe_wait(locator, label):
        return
    try:
        locator.set_checked(checked)
        print(f"[OK] {label} gesetzt: {checked}")
    except Exception as exc:
        _debug_skip(label, f"Fehler beim Setzen: {exc}")


def _log_event_types(frame: Frame):
    try:
        options = frame.locator("#event option").all()
        values = []
        for option in options:
            label = (option.inner_text() or "").strip()
            value = option.get_attribute("value") or ""
            if label:
                values.append(f"{label} (value={value})")
        print(f"[INFO] Veranstaltungstypen gefunden: {len(values)}")
    except Exception as exc:
        print(f"[WARNUNG] Konnte Veranstaltungstypen nicht lesen: {exc}")


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%d.%m.%Y")

def _normalize_date(value: str | None, fallback: str) -> str:
    if not value:
        return fallback
    cleaned = str(value).strip()
    if not cleaned:
        return fallback
    if "." in cleaned:
        return cleaned
    try:
        parsed = datetime.strptime(cleaned, "%Y-%m-%d")
        return parsed.strftime("%d.%m.%Y")
    except Exception:
        return fallback

def _load_payload(payload_raw: str | None, payload_file: str | None) -> dict:
    if payload_raw:
        return json.loads(payload_raw)
    if payload_file:
        return json.loads(Path(payload_file).read_text(encoding="utf-8"))
    return {}

def _build_address(form_data: dict) -> str:
    parts = [
        form_data.get("strasse"),
        form_data.get("hausnummer"),
        form_data.get("plz"),
        form_data.get("ort"),
    ]
    return " ".join([str(p).strip() for p in parts if p]).strip()

def _build_note_text(form_data: dict) -> str:
    lines = []
    for label, key in [
        ("Dresscode", "dresscode"),
        ("Tätigkeit", "taetigkeit"),
        ("Treffpunkt", "treffpunkt"),
        ("Bemerkung", "bemerkung"),
    ]:
        value = form_data.get(key)
        if value:
            lines.append(f"{label}: {value}")
    return "\n".join(lines).strip()

def _find_best_option_match(frame: Frame, selector: str, query: str) -> tuple[str, str] | None:
    if not query:
        return None
    try:
        options = frame.locator(f"{selector} option").all()
        query_lc = query.strip().lower()
        for option in options:
            label = (option.inner_text() or "").strip()
            value = option.get_attribute("value") or ""
            label_lc = label.lower()
            if query_lc and label_lc and (query_lc in label_lc or label_lc in query_lc):
                return label, value
    except Exception:
        return None
    return None

def _select_funktion_by_label(frame: Frame, desired_label: str) -> bool:
    if not desired_label:
        return False
    match = _find_best_option_match(frame, "#funktion_id", desired_label)
    if match:
        return _safe_select_value(frame, "#funktion_id", match[1], "Funktion")
    # Fallback: exact label select
    return _safe_select_label(frame, "#funktion_id", desired_label, "Funktion")

def _get_selected_option_text(frame: Frame, selector: str) -> str:
    try:
        option = frame.locator(f"{selector} option:checked").first
        if option.count() == 0:
            return ""
        return (option.inner_text() or "").strip()
    except Exception:
        return ""


def run_va_anlage(headless: bool | None, slowmo_ms: int | None, payload: dict | None):
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    payload = payload or {}
    form_data = payload.get("formData") or payload.get("form_data") or {}
    customer_label = payload.get("customerName") or payload.get("customer_name") or DEFAULT_CUSTOMER_LABEL
    event_type_label = form_data.get("veranstaltungstyp") or DEFAULT_EVENT_TYPE_LABEL
    va_name_final = form_data.get("vaName") or form_data.get("va_name") or "Event"
    start_date = _normalize_date(form_data.get("startDate"), DEFAULT_START_DATE)
    end_date = _normalize_date(form_data.get("endDate") or form_data.get("startDate"), DEFAULT_END_DATE)
    start_time = form_data.get("startTime") or DEFAULT_START_TIME
    end_time = form_data.get("endTime") or DEFAULT_END_TIME
    location = _build_address(form_data) or DEFAULT_LOCATION
    note_text = _build_note_text(form_data) or DEFAULT_NOTE_TEXT
    taetigkeit = form_data.get("taetigkeit") or form_data.get("taetigkeitCustom") or note_text
    treffpunkt = form_data.get("treffpunkt") or note_text
    personalbedarf = payload.get("personalbedarf") or []

    state_path = _require_login_state()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        context = browser.new_context(storage_state=str(state_path))
        page = context.new_page()
        success_dialog = {"hit": False, "text": "", "action": ""}
        temp_marker = f"TMP-{int(time.time())}"
        found = None
        intraday_url = ""

        def _handle_dialog(dialog):
            try:
                msg = (dialog.message or "").strip()
                success_dialog["text"] = msg
                if "Termin eingetragen" in msg:
                    success_dialog["hit"] = True
                print(f"[INFO] Browser-Dialog: {msg}")
                try:
                    print(f"[INFO] Dialog-Typ: {dialog.type}")
                except Exception:
                    pass
                dialog.dismiss()
                success_dialog["dismissed"] = True
                success_dialog["action"] = "dismiss"
            except Exception as exc:
                print(f"[WARNUNG] Dialog-Handling fehlgeschlagen: {exc}")

        page.on("dialog", _handle_dialog)

        print("[INFO] Lade Startseite mit gespeicherter Session …")
        page.goto(config.BASE_URL, wait_until="load", timeout=20000)

        frame = _wait_for_inhalt_frame(page, timeout_seconds=20)
        target_url = urljoin(config.BASE_URL, "planung_neuertermin.php")
        print(f"[INFO] Öffne Neuer Termin: {target_url}")
        frame.goto(target_url, wait_until="domcontentloaded", timeout=30000)
        try:
            frame.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass

        # PersPlan lädt gelegentlich in ein anderes Frame/neu gerendert – daher per Selector-Scan
        frame_and_sel = _find_frame_with_any_selector_or_none(
            page,
            ["#kunde", "select[name='kunden_id']", "form#neuerTermin", "form[name='neuerTermin']"],
            timeout_seconds=30,
        )
        frame = frame_and_sel[0] if frame_and_sel else None
        customer_selector = frame_and_sel[1] if frame_and_sel else "#kunde"
        if not frame:
            # Häufigster Grund: Session abgelaufen → Login-Formular sichtbar
            if _find_frame_with_selector_or_none(page, "#loginName", timeout_seconds=4):
                print("[WARNUNG] Login erforderlich – führe Login aus und versuche erneut …")
                do_login(page)
                frame = _wait_for_inhalt_frame(page, timeout_seconds=20)
                frame.goto(target_url, wait_until="domcontentloaded", timeout=30000)
                try:
                    frame.wait_for_load_state("networkidle", timeout=20000)
                except Exception:
                    pass
                frame_and_sel = _find_frame_with_any_selector(
                    page,
                    ["#kunde", "select[name='kunden_id']", "form#neuerTermin", "form[name='neuerTermin']"],
                    timeout_seconds=30,
                )
                frame, customer_selector = frame_and_sel
            else:
                # Fallback: Top-Level-Navigation probieren (manchmal wird das Frameset umgangen)
                print("[WARNUNG] #kunde nicht gefunden – versuche Top-Level-Navigation …")
                page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
                try:
                    page.wait_for_load_state("networkidle", timeout=20000)
                except Exception:
                    pass
                frame_and_sel = _find_frame_with_any_selector_or_none(
                    page,
                    ["#kunde", "select[name='kunden_id']", "form#neuerTermin", "form[name='neuerTermin']"],
                    timeout_seconds=20,
                )
                if not frame_and_sel:
                    _dump_frame_debug(page)
                    _dump_page_debug_artifacts(page, "neuer_termin")
                    raise RuntimeError("[FEHLER] Formular '#kunde' nicht gefunden (unerwartete Seite).")
                frame, customer_selector = frame_and_sel

        # Falls wir nur das Form gefunden haben, jetzt explizit das Kunden-Select suchen
        if customer_selector in ("form#neuerTermin", "form[name='neuerTermin']"):
            if _find_frame_with_selector_or_none(page, "#kunde", timeout_seconds=10):
                customer_selector = "#kunde"
            elif _find_frame_with_selector_or_none(page, "select[name='kunden_id']", timeout_seconds=10):
                customer_selector = "select[name='kunden_id']"
            else:
                _dump_frame_debug(page)
                _dump_page_debug_artifacts(page, "neuer_termin")
                raise RuntimeError("[FEHLER] Kunden-Select nicht gefunden (form vorhanden).")

        frame.wait_for_selector(customer_selector, state="attached", timeout=20000)
        print("[OK] Formular für neuen Termin geladen.")

        _log_event_types(frame)

        # Kunde
        if not _safe_select_label(frame, customer_selector, customer_label, "Kunde"):
            if not _safe_select_label(frame, customer_selector, DEFAULT_CUSTOMER_LABEL, "Kunde"):
                _safe_select_value(frame, customer_selector, "39", "Kunde")

        # Veranstaltungstyp (best-effort)
        selected = False
        for label in [event_type_label, *FALLBACK_EVENT_TYPE_LABELS]:
            if label and _safe_select_label(frame, "#event", label, "Veranstaltungstyp"):
                selected = True
                break
        if not selected:
            best = _find_best_option_match(frame, "#event", event_type_label)
            if best and _safe_select_value(frame, "#event", best[1], "Veranstaltungstyp"):
                selected = True
        if not selected:
            _debug_skip("Veranstaltungstyp", f"keinen passenden Typ gefunden ({event_type_label})")

        # Event Daten
        _safe_fill(frame, "#ort", location, "Ort")
        selected_land = _get_selected_option_text(frame, "#land")
        if selected_land != "Deutschland":
            _safe_select_label(frame, "#land", "Deutschland", "Land")

        # Bundesland nur setzen, wenn nicht bereits Bayern gewählt ist
        selected_state = _get_selected_option_text(frame, "#bundesland")
        if selected_state != "Bayern":
            if not _safe_select_label(frame, "#bundesland", "Bayern", "Bundesland"):
                if not _safe_select_value(frame, "#bundesland", "Bayern", "Bundesland"):
                    try:
                        frame.evaluate(
                            """(selector, value) => {
                                const el = document.querySelector(selector);
                                if (!el) return false;
                                el.value = value;
                                el.dispatchEvent(new Event('change', { bubbles: true }));
                                return true;
                            }""",
                            "#bundesland",
                            "Bayern",
                        )
                        print("[OK] Bundesland gesetzt via evaluate: Bayern")
                    except Exception as exc:
                        _debug_skip("Bundesland", f"Fallback evaluate fehlgeschlagen: {exc}")

        _safe_fill(frame, "#von_uhrzeit", start_time, "Start Uhrzeit")
        _safe_fill(frame, "#von_datum1", start_date, "Start Datum")
        _safe_fill(frame, "#bis_uhrzeit", end_time, "Ende Uhrzeit")
        _safe_fill(frame, "#bis_datum1", end_date, "Ende Datum")

        # Notizen
        _safe_fill(frame, "#bemerkung_area", note_text, "Event Bemerkung")
        _safe_fill(frame, "#va_bezeichnung", temp_marker, "VA-Name (Marker)")
        _safe_fill(frame, "#taetigkeit", taetigkeit, "Durchzuführende Tätigkeit")
        _safe_fill(frame, "textarea[name='zusaetzliche_bemerkung']", note_text, "Zusätzliche Bemerkung")
        _safe_fill(frame, "#treffpunkt", treffpunkt, "Treffpunkt")

        # Stealth Modus aktivieren
        _safe_check(frame, "input[name='versteckt']", "Stealth Modus", checked=True)

        # Hinzufügen klicken
        add_button = frame.locator("#addButton").first
        if _safe_wait(add_button, "Hinzufügen Button"):
            try:
                with page.expect_event("dialog", timeout=15000):
                    add_button.click()
                print("[AKTION] Hinzufügen geklickt (Dialog erwartet).")
            except Exception as exc:
                _debug_skip("Hinzufügen Button", f"Klick fehlgeschlagen: {exc}")

        # Falls Dialog sehr spät kommt, kurz nachwarten
        for _ in range(10):
            if success_dialog["text"]:
                break
            time.sleep(0.3)

        if success_dialog["hit"]:
            print("[OK] Erfolgsmeldung erkannt: Termin eingetragen.")
        elif success_dialog["text"]:
            print("[WARNUNG] Dialog erkannt, aber keine Erfolgsmeldung: " + success_dialog["text"])
        else:
            print("[WARNUNG] Kein Browser-Dialog erkannt.")
        if success_dialog["action"]:
            print(f"[INFO] Dialog-Aktion: {success_dialog['action']}")

        # Nach dem Abbrechen: planung.php laden, Monat/Jahr setzen, Monat Anzeigen, passende Zeile finden
        try:
            if success_dialog.get("dismissed"):
                try:
                    page.wait_for_load_state("networkidle", timeout=20000)
                except Exception:
                    pass
                time.sleep(1.0)

            overview_page = page.opener() if hasattr(page, "opener") and page.opener() else page
            overview_frame = _wait_for_inhalt_frame(overview_page, timeout_seconds=15)
            overview_url = urljoin(config.BASE_URL, "planung.php")
            print(f"[INFO] Lade Übersicht: {overview_url}")
            overview_frame.goto(overview_url, wait_until="domcontentloaded", timeout=30000)
            overview_frame.wait_for_selector("form#planungAnzeige", timeout=20000)

            target_date = _parse_date(start_date)
            month_value = str(target_date.month)
            year_value = str(target_date.year)

            _safe_select_value(overview_frame, "#monat", month_value, "Monat")
            _safe_select_value(overview_frame, "#jahr", year_value, "Jahr")
            # Zeitraum innerhalb des Zielmonats
            last_day = calendar.monthrange(target_date.year, target_date.month)[1]
            range_from = f"01.{target_date.month:02d}.{target_date.year}"
            range_to = f"{last_day:02d}.{target_date.month:02d}.{target_date.year}"
            _safe_fill(overview_frame, "#von", range_from, "von (Zeitraum)")
            _safe_fill(overview_frame, "#bis", range_to, "bis (Zeitraum)")

            submit_month = overview_frame.locator("input[name='datum_suche'][value='Monat Anzeigen']").first
            if _safe_wait(submit_month, "Monat Anzeigen"):
                submit_month.click()
                print("[DEBUG] Monat Anzeigen geklickt")

            # Zusätzlich: Marker direkt über Suche filtern
            _safe_fill(overview_frame, "#suchfeld", temp_marker, "Suchfeld")
            checkbox = overview_frame.locator("#checkbox_suche_zeitraum").first
            if _safe_wait(checkbox, "Zeitraum berücksichtigen"):
                try:
                    checkbox.set_checked(True)
                except Exception:
                    pass
            search_btn = overview_frame.locator("input[type='submit'][value='Suchen']").first
            if _safe_wait(search_btn, "Suchen"):
                search_btn.click()
                print("[DEBUG] Suche geklickt")

            overview_frame.wait_for_selector("tr[name^='tr_']", timeout=20000)

            # Schnell-Path: Suche in der Seite per JS nach Marker im VA-Namen
            found = overview_frame.evaluate(
                """(marker) => {
                    const rows = Array.from(document.querySelectorAll("tr[name^='tr_']"));
                    for (let i = 0; i < rows.length; i++) {
                        const tds = rows[i].querySelectorAll("td");
                        if (!tds || tds.length < 9) continue;
                        const vaCellText = tds[7].innerText || "";
                        if (!vaCellText.includes(marker)) continue;
                        const id = (tds[1]?.innerText || "").trim();
                        const link = rows[i].querySelector("a[href*='planung_intraday.php']");
                        const href = link ? link.getAttribute("href") : "";
                        return { index: i, id, href };
                    }
                    return null;
                }""",
                temp_marker,
            )

            if not found:
                print("[WARNUNG] Keine passende Veranstaltung gefunden (Marker nicht gefunden).")
            else:
                print(f"[OK] Veranstaltung gefunden. ID={found['id']}")
                intraday_url = found["href"] or ""
                if intraday_url:
                    intraday_url = urljoin(config.BASE_URL, intraday_url)
                    print(f"[INFO] Intraday-Link gefunden: {intraday_url}")
                else:
                    print("[WARNUNG] Kein Intraday-Link im Treffer gefunden.")

                # Nur zur Veranstaltungsdetail-Ansicht wechseln
                if intraday_url:
                    try:
                        print(f"[INFO] Öffne Veranstaltungsdetail: {intraday_url}")
                        overview_frame.goto(intraday_url, wait_until="domcontentloaded", timeout=30000)
                        overview_frame.wait_for_selector("#header_uebersicht", timeout=20000)
                        print("[OK] Veranstaltungsdetail geladen.")

                        # VA-Name bereinigen/setzen (Temp raus, neuer Name rein)
                        try:
                            header_edit = overview_frame.locator("#header_uebersicht img[title='Bearbeiten']").first
                            popup_page = None
                            if header_edit.count() > 0:
                                try:
                                    with overview_page.expect_popup(timeout=15000) as pop:
                                        header_edit.click()
                                    popup_page = pop.value
                                except Exception:
                                    onclick = header_edit.get_attribute("onclick") or ""
                                    marker = "planung_neuertermin.php?secureid="
                                    if marker in onclick:
                                        start = onclick.find(marker) + len(marker)
                                        tail = onclick[start:]
                                        for sep in ("'", "\"", ")", " "):
                                            if sep in tail:
                                                tail = tail.split(sep, 1)[0]
                                        target_url = urljoin(
                                            config.BASE_URL,
                                            f"planung_neuertermin.php?secureid={tail}",
                                        )
                                        popup_page = overview_page.context.new_page()
                                        popup_page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
                            if popup_page:
                                popup_page.wait_for_load_state("domcontentloaded", timeout=30000)
                                popup_frame = popup_page.frame(name="inhalt") or popup_page.main_frame
                                popup_frame.wait_for_selector("#va_bezeichnung", timeout=20000)
                                _safe_fill(popup_frame, "#va_bezeichnung", va_name_final, "VA-Name (Final)")
                                save_button = popup_frame.locator("#addButton").first
                                if save_button.count() == 0:
                                    save_button = popup_frame.locator("button[name='saveButton']").first
                                if save_button.count() == 0:
                                    save_button = popup_frame.locator("input[name='saveButton']").first
                                if save_button.count() > 0:
                                    save_button.click()
                                    print("[OK] VA-Name gespeichert.")
                        except Exception as exc:
                            print(f"[WARNUNG] VA-Name konnte nicht aktualisiert werden: {exc}")

                        def _open_neue_schicht_popup():
                            neue_schicht_btn = overview_frame.locator("button:has-text('Neue Schicht')").first
                            popup = None
                            if _safe_wait(neue_schicht_btn, "Neue Schicht"):
                                try:
                                    with overview_page.expect_popup(timeout=15000) as pop:
                                        neue_schicht_btn.click()
                                    popup = pop.value
                                except Exception:
                                    onclick = neue_schicht_btn.get_attribute("onclick") or ""
                                    if "schicht_hinzufuegen_neu.php" in onclick:
                                        marker = "schicht_hinzufuegen_neu.php?secureid="
                                        if marker in onclick:
                                            start = onclick.find(marker) + len(marker)
                                            tail = onclick[start:]
                                            for sep in ("'", "\"", ")", " "):
                                                if sep in tail:
                                                    tail = tail.split(sep, 1)[0]
                                            target_url = urljoin(
                                                config.BASE_URL,
                                                f"schicht_hinzufuegen_neu.php?secureid={tail}",
                                            )
                                            popup = overview_page.context.new_page()
                                            popup.goto(target_url, wait_until="domcontentloaded", timeout=30000)
                            return popup

                        def _create_shift(slot: dict, position_label: str | None):
                            popup_page = _open_neue_schicht_popup()
                            if not popup_page:
                                print("[WARNUNG] Neue Schicht Fenster nicht geöffnet.")
                                return
                            popup_page.wait_for_load_state("domcontentloaded", timeout=30000)
                            popup_frame = popup_page.frame(name="inhalt") or popup_page.main_frame
                            popup_frame.wait_for_selector("#anzahl", timeout=20000)
                            anzahl = str(slot.get("anzahl") or 1)
                            _safe_fill(popup_frame, "#anzahl", anzahl, "Anzahl")
                            slot_start = slot.get("startTime") or start_time
                            slot_end = slot.get("endTime") or end_time
                            _safe_fill(popup_frame, "#von_uhrzeit", slot_start, "Von Uhrzeit")
                            try:
                                popup_frame.evaluate(
                                    "(value) => { const el = document.querySelector('#bis_uhrzeit'); if (el) { el.value = value; el.dispatchEvent(new Event('change', { bubbles: true })); } }",
                                    slot_end,
                                )
                                print(f"[OK] Bis Uhrzeit gesetzt: {slot_end}")
                            except Exception as exc:
                                print(f"[WARNUNG] Bis Uhrzeit konnte nicht gesetzt werden: {exc}")
                            if position_label:
                                if not _select_funktion_by_label(popup_frame, position_label):
                                    print(f"[WARNUNG] Funktion nicht gefunden: {position_label}")
                            # Speichern klicken
                            save_btn = popup_frame.locator("button:has-text('Speichern')").first
                            if _safe_wait(save_btn, "Speichern"):
                                try:
                                    save_btn.click()
                                    try:
                                        dialog = popup_frame.locator(".ui-dialog:has-text('Fehler')").first
                                        if dialog.count() > 0:
                                            msg = dialog.locator(".ui-dialog-content").inner_text().strip()
                                            print(f"[WARNUNG] Dialog: {msg}")
                                            ok_btn = dialog.locator("button:has-text('Ok')").first
                                            if ok_btn.count() > 0:
                                                ok_btn.click()
                                    except Exception:
                                        pass
                                    popup_frame.wait_for_selector("p.msg_out_message_container", timeout=20000)
                                    toast = popup_frame.locator("p.msg_out_message_container").first
                                    toast_text = (toast.inner_text() or "").strip()
                                    if "Schicht hinzugefügt" in toast_text:
                                        print("[OK] Schicht hinzugefügt.")
                                    else:
                                        print(f"[WARNUNG] Toast erkannt: {toast_text}")
                                    close_btn = popup_frame.locator("button:has-text('Schließen')").first
                                    if _safe_wait(close_btn, "Schließen"):
                                        close_btn.click()
                                        print("[OK] Schließen geklickt.")
                                except Exception as exc:
                                    print(f"[WARNUNG] Speichern fehlgeschlagen: {exc}")

                        if not personalbedarf:
                            print("[WARNUNG] Kein Personalbedarf angegeben – keine Schichten angelegt.")
                        else:
                            for entry in personalbedarf:
                                position_label = entry.get("position") or entry.get("positionLabel") or entry.get("funktion")
                                slots = entry.get("slots") or []
                                if not slots:
                                    _create_shift({"startTime": start_time, "endTime": end_time, "anzahl": entry.get("anzahl") or 1}, position_label)
                                else:
                                    for slot in slots:
                                        _create_shift(slot, position_label)
                    except Exception as exc:
                        print(f"[WARNUNG] Intraday-Ansicht konnte nicht geöffnet werden: {exc}")
                else:
                    print("[WARNUNG] Kein Intraday-Link gefunden, Detail-Ansicht nicht geöffnet.")
        except Exception as exc:
            print(f"[WARNUNG] Konnte Übersicht nicht auslesen: {exc}")

        try:
            RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
            result_payload = {
                "timestamp": datetime.utcnow().isoformat(),
                "requestId": payload.get("requestId") or payload.get("request_id"),
                "success": success_dialog.get("hit", False),
                "message": success_dialog.get("text", ""),
                "persplanId": found["id"] if found else "",
                "persplanUrl": intraday_url or "",
                "vaName": va_name_final,
                "customer": customer_label,
            }
            with RESULTS_PATH.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(result_payload) + "\n")
        except Exception as exc:
            print(f"[WARNUNG] Ergebnis konnte nicht geschrieben werden: {exc}")

        print("[INFO] Halte 5 Sekunden …")
        time.sleep(5)

        print("[INFO] Fertig. Browser wird geschlossen …")
        browser.close()


def main():
    parser = argparse.ArgumentParser(description="VA Anlage (Neuer Termin)")
    parser.add_argument("--headless", choices=["true", "false"], default=None)
    parser.add_argument("--slowmo", type=int, default=None)
    parser.add_argument("--payload", type=str, default=None, help="JSON Payload")
    parser.add_argument("--payload-file", type=str, default=None, help="Pfad zu JSON Payload")
    args = parser.parse_args()

    headless = None if args.headless is None else args.headless == "true"
    payload = _load_payload(args.payload, args.payload_file)
    run_va_anlage(headless=headless, slowmo_ms=args.slowmo, payload=payload)


if __name__ == "__main__":
    main()
