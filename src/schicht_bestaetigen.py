import csv
import time
from datetime import datetime, timedelta
from pathlib import Path
import re
from urllib.parse import urljoin
from playwright.sync_api import Page, Frame, Locator, sync_playwright

from src import config


def _wait_for_frame(page: Page, name: str, timeout_seconds: int = 20) -> Frame:
    """Polls until a named frame becomes available."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        frame = page.frame(name=name)
        if frame:
            return frame
        time.sleep(0.5)
    raise RuntimeError(f"[FEHLER] Frame '{name}' wurde nicht gefunden.")


def _load_inhalt_url(page: Page, target_href: str, wait_selector: str | None = None) -> Frame:
    """Navigiert den 'inhalt'-Frame zu einer URL und wartet optional auf einen Selektor."""
    if not target_href:
        raise RuntimeError("[FEHLER] Ungültige Ziel-URL für Frame-Navigation.")
    full_url = urljoin(config.BASE_URL, target_href)
    frame = _wait_for_frame(page, "inhalt", timeout_seconds=25)
    print(f"[INFO] Lade im Frame: {full_url}")
    frame.goto(full_url, wait_until="domcontentloaded", timeout=30000)
    if wait_selector:
        frame.wait_for_selector(wait_selector, timeout=20000)
    return frame


def open_tagesplan_alt(page: Page) -> Frame:
    """
    Navigiert innerhalb des bestehenden Sessions-Fensters zur Startseite
    und klickt auf den Button „Tagesplan (alt)“.
    """
    print("[INFO] Suche nach Startseiten-Inhalt …")
    frame_inhalt = _wait_for_frame(page, "inhalt", timeout_seconds=25)
    print("[OK] Frame 'inhalt' aktiv.")

    button_selector = "a.jq_menueButtonMitIcon[onclick*='willkommen_tagesplan.php']"
    print("[INFO] Suche Link 'Tagesplan (alt)' …")
    deadline = time.time() + 20
    while time.time() < deadline:
        if frame_inhalt.locator(button_selector).count() > 0:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("[FEHLER] 'Tagesplan (alt)'-Link nicht gefunden.")

    button = frame_inhalt.locator(button_selector).first
    button.scroll_into_view_if_needed()
    print("[AKTION] Klicke auf 'Tagesplan (alt)' …")
    button.click()

    print("[INFO] Warte auf Tagesplan (alt) …")
    target_selector = "input[name='timestamp_bis']"
    for _ in range(80):
        frame_inhalt = page.frame(name="inhalt")
        if frame_inhalt and frame_inhalt.locator(target_selector).count() > 0:
            print("[OK] Filterformular mit 'timestamp_bis' gefunden.")
            return frame_inhalt
        time.sleep(0.5)

    print("[WARNUNG] Formular nicht erkannt – versuche direkten Aufruf von 'willkommen_tagesplan.php'.")
    frame_inhalt = _load_inhalt_url(page, "willkommen_tagesplan.php", wait_selector=target_selector)
    print("[OK] Tagesplan (alt) geladen (Fallback).")
    return frame_inhalt


def _calc_fallback_date(frame: Frame, fallback_days: int) -> str:
    """Berechnet timestamp_bis anhand timestamp_von + fallback_days."""
    von_input = frame.locator("input[name='timestamp_von']")
    von_value = ""
    try:
        von_value = von_input.input_value().strip()
    except Exception:
        pass

    if not von_value:
        von_value = datetime.now().strftime("%d.%m.%Y")

    try:
        base = datetime.strptime(von_value, "%d.%m.%Y")
    except ValueError:
        base = datetime.now()

    target = base + timedelta(days=fallback_days)
    return target.strftime("%d.%m.%Y")


def _set_in_x_tagen(page: Page, frame: Frame, days: int) -> None:
    bis_input = frame.locator("input[name='timestamp_bis']")
    bis_input.wait_for(state="visible", timeout=10000)
    bis_input.click()
    time.sleep(0.5)

    quick_locator = frame.locator(
        f"text=/in\\s+{days}\\s+tage?n/i"
    )

    if quick_locator.count() > 0:
        print(f"[INFO] Klicke Quick-Link 'in {days} Tagen' …")
        quick_locator.first.click()
        time.sleep(0.5)
    else:
        print(f"[WARNUNG] Kein Quick-Link gefunden – setze Datum (heute + {days} Tage) manuell.")
        new_value = _calc_fallback_date(frame, days)
        bis_input.fill(new_value)
        time.sleep(0.2)


def apply_filter(page: Page, frame: Frame) -> Frame:
    """Stellt den Zeitraum ein (bis = in X Tagen) und klickt auf 'anzeigen'."""
    days = config.TAGESPLAN_IN_TAGEN
    print(f"[INFO] Setze 'bis' auf 'in {days} Tagen' …")
    _set_in_x_tagen(page, frame, days)

    anzeigen_button = frame.locator("input[name='timestamp_auswahl_anzeigen']")
    anzeigen_button.wait_for(state="visible", timeout=5000)
    anzeigen_button.click()
    print("[INFO] Filter angewendet – warte auf Aktualisierung …")
    time.sleep(1.0)

    frame = _wait_for_frame(page, "inhalt", timeout_seconds=25)
    frame.wait_for_selector("a[href*='planung_intraday.php']", timeout=20000)
    try:
        frame.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.5)
    except Exception:
        pass
    print("[OK] Gefilterte Veranstaltungen geladen.")
    return frame


def _load_complete_event_list(frame: Frame, selector: str, max_scrolls: int = 40) -> int:
    """
    Scrollt bis zum Ende der Seite und wartet auf Nachlade-Events.
    Gibt die finale Anzahl der gefundenen Veranstaltungen zurück.
    """
    last_count = -1
    stable_rounds = 0
    for attempt in range(max_scrolls):
        try:
            frame.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            pass
        time.sleep(0.6)
        current_count = frame.locator(selector).count()
        if current_count > last_count:
            print(f"[INFO] Veranstaltungen geladen: {current_count}")
            last_count = current_count
            stable_rounds = 0
        else:
            stable_rounds += 1
            if stable_rounds >= 3:
                break

    try:
        frame.evaluate("window.scrollTo(0, 0)")
    except Exception:
        pass

    return max(last_count, 0)


def collect_event_links(frame: Frame) -> list[dict[str, str]]:
    """Liest alle Veranstaltungs-Links (Header) aus."""
    selector = "a[href*='planung_intraday.php']"
    total_loaded = _load_complete_event_list(frame, selector)
    locator = frame.locator(selector)
    count = total_loaded or locator.count()
    events: list[dict[str, str]] = []
    seen = set()
    for i in range(count):
        link = locator.nth(i)
        href = link.get_attribute("href") or ""
        if not href or href in seen:
            continue
        seen.add(href)
        text = " ".join(link.inner_text().split())
        events.append({"href": href, "text": text})
    print(f"[INFO] Anzahl Veranstaltungen im Filter: {len(events)}")
    return events


def _normalize_name(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split()).strip().lower()


def _ensure_employee_filter_disabled(frame: Frame) -> None:
    """
    Stellt sicher, dass der Funktions-Filter (Zahnradsymbol) deaktiviert ist,
    da ansonsten bestimmte Mitarbeiter nicht angezeigt werden.
    """
    button = frame.locator(
        "#vue-intraday-ma-liste-app button[class*='filter'][class*='ml-10']"
    ).first
    icon = (
        frame.locator(
            "#vue-intraday-ma-liste-app img.sprite_16x16.settings.pointer[title*='Funktion']"
        ).first
        if frame.locator(
            "#vue-intraday-ma-liste-app img.sprite_16x16.settings.pointer[title*='Funktion']"
        ).count()
        > 0
        else None
    )
    target_click = button if button.count() > 0 else icon
    if target_click is None or target_click.count() == 0:
        return

    def _class_value() -> str:
        try:
            if button.count() > 0:
                return (button.get_attribute("class") or "").lower()
            if icon and icon.count() > 0:
                container = icon.locator("xpath=ancestor::*[contains(@class,'ml-10')][1]")
                if container.count() > 0:
                    return (container.first.get_attribute("class") or "").lower()
                return (icon.get_attribute("class") or "").lower()
            return ""
        except Exception:
            return ""

    class_attr = _class_value()
    if "filteron" not in class_attr:
        return

    print("[INFO] Funktion-Filter ist aktiv – deaktiviere …")
    try:
        target_click.click()
    except Exception as exc:
        print(f"[WARNUNG] Filter-Icon konnte nicht geklickt werden: {exc}")
        return

    for _ in range(10):
        time.sleep(0.2)
        class_attr = _class_value()
        if "filteroff" in class_attr:
            print("[OK] Funktion-Filter deaktiviert.")
            return

    print("[WARNUNG] Filterstatus blieb aktiv – bitte manuell prüfen.")


def build_phonebook_from_overview(frame: Frame) -> dict[str, str]:
    """
    Extrahiert Telefonnummern direkt von der Übersichtstabelle (Filterseite),
    damit Notfall-Fallbacks ohne Popup möglich sind.
    """
    _ensure_employee_filter_disabled(frame)
    _load_complete_event_list(frame, "td[id^='row_']")
    phonebook: dict[str, str] = {}
    links = frame.locator("td[id^='row_'] a[href^='tel:']")
    for i in range(links.count()):
        link = links.nth(i)
        phone = (link.inner_text() or "").strip()
        if not phone:
            continue
        cell = link.locator("xpath=ancestor::td[1]")
        if cell.count() == 0:
            continue
        raw_text = cell.inner_text().strip()
        if not raw_text:
            continue
        name = raw_text.split("(", 1)[0].strip().rstrip(":")
        normalized = _normalize_name(name)
        if normalized and normalized not in phonebook:
            phonebook[normalized] = phone
    print(f"[INFO] Telefonliste aus Übersicht geladen: {len(phonebook)} Einträge")
    return phonebook


def _scrape_phone_from_event(frame: Frame, name: str) -> str | None:
    """
    Sucht auf der aktuellen Veranstaltungsseite nach einer Telefonnummer
    innerhalb der Mitarbeiter-Tabelle.
    """
    normalized_target = _normalize_name(name)
    tel_links = frame.locator("a[href^='tel:']")
    candidates = tel_links.count()
    print(f"[DEBUG] Suche Telefonnummer in Veranstaltung für '{name}' – Tel-Links: {candidates}")
    for i in range(candidates):
        link = tel_links.nth(i)
        try:
            phone = (link.inner_text() or "").strip()
        except Exception:
            phone = ""
        ancestor = link.locator(
            "xpath=ancestor::*[self::td or self::div or self::span or self::p][1]"
        )
        try:
            context_text = ancestor.inner_text().strip() if ancestor.count() > 0 else ""
        except Exception:
            context_text = ""
        if not context_text:
            continue
        normalized_context = _normalize_name(context_text)
        if normalized_target in normalized_context:
            if phone:
                print(f"[INFO] Telefonnummer direkt aus Veranstaltung gelesen: {phone}")
                return phone
    return None


def _debug_phone_context(frame: Frame, name: str) -> None:
    """Gibt Debug-Informationen aus, warum keine Telefonnummer gefunden wurde."""
    print(f"[DEBUG] Telefonnummer für '{name}' weiterhin nicht gefunden. Dump Kontext …")
    tel_links = frame.locator("a[href^='tel:']")
    total = tel_links.count()
    print(f"[DEBUG] Gesamtzahl Tel-Links: {total}")
    sample = min(total, 10)
    for i in range(sample):
        link = tel_links.nth(i)
        try:
            phone = (link.inner_text() or "").strip()
        except Exception:
            phone = ""
        ancestor = link.locator(
            "xpath=ancestor::*[self::td or self::div or self::span or self::p][1]"
        )
        try:
            context_text = ancestor.inner_text().strip() if ancestor.count() > 0 else ""
        except Exception:
            context_text = ""
        print(f"[DEBUG] Link #{i+1}: phone='{phone}' context='{context_text[:200]}'")
    return None


def find_orange_assignments(frame: Frame) -> list[str]:
    """Sammelt alle Mitarbeiter-Namen, deren Schicht orange hinterlegt ist."""
    cells = frame.locator("td.schichtZeitZelle")
    names: list[str] = []
    for i in range(cells.count()):
        cell = cells.nth(i)
        style = (cell.get_attribute("style") or "").lower()
        if "orange" not in style:
            continue
        raw_text = cell.inner_text().strip()
        if not raw_text:
            continue
        name = raw_text.split("\n", 1)[0].strip()
        if name:
            names.append(name)
    return names


def split_name(raw: str) -> tuple[str, str]:
    cleaned = raw.strip()
    if "," in cleaned:
        last, first = [part.strip() for part in cleaned.split(",", 1)]
        return first, last
    parts = cleaned.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return cleaned, ""


def extract_event_date(title: str) -> str:
    match = re.search(r"(\d{2}\.\d{2}\.\d{4})", title)
    if match:
        return match.group(1)
    return ""


def extract_header_info(frame: Frame) -> dict[str, str]:
    table = frame.locator("table#header_uebersicht")
    if table.count() == 0:
        return {}
    rows = table.locator("tr")
    if rows.count() < 2:
        return {}
    cells = rows.nth(1).locator("td")
    info: dict[str, str] = {}
    if cells.count() >= 4:
        info["date"] = cells.nth(3).inner_text().strip()
    if cells.count() >= 5:
        info["time"] = cells.nth(4).inner_text().strip()
    return info


def _get_search_input(frame: Frame) -> Locator | None:
    app = frame.locator("#vue-intraday-ma-liste-app")
    if app.count() == 0:
        return None
    search = app.locator("input[placeholder='Suchen']").first
    return search if search.count() > 0 else None


def _locate_employee_row(frame: Frame, name: str) -> Locator | None:
    _ensure_employee_filter_disabled(frame)

    container = frame.locator("#mitarbeiterListeNamen")
    if container.count() == 0:
        return None

    search_input = _get_search_input(frame)
    if search_input:
        search_input.fill("")
        search_input.fill(name)
        time.sleep(0.5)

    target = container.locator("div", has_text=name)
    if target.count() == 0:
        rows = container.locator("div")
        for i in range(rows.count()):
            txt = rows.nth(i).inner_text().strip()
            if _normalize_name(name) in _normalize_name(txt):
                return rows.nth(i)
        return None
    return target.first


def fetch_phone_via_popup(frame: Frame, name: str) -> str | None:
    """Öffnet das Info-Popup eines Mitarbeiters und gibt Mobilnummer zurück."""
    row = _locate_employee_row(frame, name)
    if row is None:
        return None

    info_icon = row.locator("img.sprite_16x16.information_wan").first
    if info_icon.count() == 0:
        return None

    info_icon.click()
    popup = frame.locator("#div_td_title")
    try:
        popup.wait_for(state="visible", timeout=5000)
    except Exception:
        return None

    phone = None
    tel_links = popup.locator("a[href^='tel:']")
    if tel_links.count() > 0:
        phone = tel_links.first.inner_text().strip()

    close_btn = popup.locator("img[src*='cancel']")
    if close_btn.count() > 0:
        close_btn.first.click()
    else:
        popup.evaluate("el => el.style.display='none'")

    search_input = _get_search_input(frame)
    if search_input:
        search_input.fill("")
        time.sleep(0.2)

    return phone


def fetch_phone_via_overview_refresh(
    page: Page, name: str, overview_phonebook: dict[str, str]
) -> str | None:
    """
    Lädt die Übersicht erneut, baut das Telefonbuch neu auf und versucht den Namen zu finden.
    """
    print(f"[INFO] Lade Übersicht erneut, um Telefonnummer für '{name}' zu finden …")
    frame = _load_inhalt_url(page, "willkommen_tagesplan.php", wait_selector="input[name='timestamp_bis']")
    frame = apply_filter(page, frame)
    fresh_book = build_phonebook_from_overview(frame)
    overview_phonebook.update(fresh_book)
    normalized = _normalize_name(name)
    phone = overview_phonebook.get(normalized)
    if phone:
        print(f"[INFO] Telefonnummer nach Refresh gefunden: {phone}")
    else:
        print(f"[WARNUNG] Auch nach Refresh keine Telefonnummer für '{name}'.")
    return phone


def fetch_phone_via_admin_directory(
    page: Page, name: str, overview_phonebook: dict[str, str]
) -> str | None:
    """Navigiert zur Mitarbeiterverwaltung und sucht dort nach der Telefonnummer."""
    print(f"[INFO] Suche Telefonnummer für '{name}' über Administration → Mitarbeiter …")
    try:
        frame = _load_inhalt_url(page, "user.php", wait_selector="table#user_tbl")
    except Exception as exc:
        print(f"[WARNUNG] Admin-Mitarbeiterliste konnte nicht geladen werden: {exc}")
        return None

    search_input = frame.locator("div.dataTables_filter input[type='search']").first
    if search_input.count() == 0:
        print("[WARNUNG] Suchfeld in Mitarbeiterliste nicht gefunden.")
    queries = []
    first, last = split_name(name)
    normalized = _normalize_name(name)
    for value in (name, last, first):
        if value:
            queries.append(value.strip())
    queries.append("")

    rows_locator = frame.locator("table#user_tbl tbody tr")

    for query in queries:
        if search_input.count() > 0:
            search_input.fill(query)
            time.sleep(0.5)
        row_count = rows_locator.count()
        for i in range(row_count):
            row = rows_locator.nth(i)
            text = row.inner_text().strip()
            if not text:
                continue
            if normalized not in _normalize_name(text):
                continue
            tel_link = row.locator("a[href^='tel:']").first
            if tel_link.count() == 0:
                continue
            phone = tel_link.inner_text().strip()
            if phone:
                print(f"[INFO] Telefonnummer in Mitarbeiterliste gefunden: {phone}")
                overview_phonebook[normalized] = phone
                return phone
    print(f"[WARNUNG] Mitarbeiter '{name}' nicht in der Admin-Liste gefunden.")
    return None


def process_veranstaltungen(
    page: Page, events: list[dict[str, str]], overview_phonebook: dict[str, str]
) -> list[dict[str, str]]:
    """Iteriert durch alle Veranstaltungen, meldet orange belegte Schichten."""
    if not events:
        print("[WARNUNG] Keine Veranstaltungen vorhanden – nichts zu prüfen.")
        return []

    total = len(events)
    report_rows = []
    for idx, event in enumerate(events, start=1):
        href = event.get("href", "")
        title = event.get("text", "").strip()
        print(f"[INFO] ({idx}/{total}) Öffne Veranstaltung: {title}")

        frame = _load_inhalt_url(page, href, wait_selector="td.schichtZeitZelle")
        header_info = extract_header_info(frame)
        event_date = header_info.get("date") if header_info else ""
        if not event_date:
            event_date = extract_event_date(title)

        orange_names = find_orange_assignments(frame)

        if orange_names:
            for name in orange_names:
                print(f"[ORANGE] {title} → {name}")
                normalized = _normalize_name(name)
                phone = overview_phonebook.get(normalized)
                if not phone:
                    phone = _scrape_phone_from_event(frame, name)
                    if phone:
                        overview_phonebook[normalized] = phone
                if not phone:
                    phone = fetch_phone_via_popup(frame, name)
                    if phone:
                        print(f"[INFO] Telefonnummer über Popup gefunden: {phone}")
                        overview_phonebook[normalized] = phone
                if not phone:
                    phone = fetch_phone_via_overview_refresh(page, name, overview_phonebook)
                if not phone:
                    phone = fetch_phone_via_admin_directory(page, name, overview_phonebook)
                if not phone:
                    _debug_phone_context(frame, name)
                    print(
                        f"[WARNUNG] Keine Telefonnummer für '{name}' gefunden – Eintrag wird übersprungen."
                    )
                    continue
                print(f"[PHONE] {name} → {phone}")
                first_name, last_name = split_name(name)
                report_rows.append(
                    {
                        "event": title,
                        "first_name": first_name,
                        "last_name": last_name,
                        "phone": phone,
                        "date": event_date,
                    }
                )
        else:
            print(f"[INFO] {title}: keine orange markierten Mitarbeiter.")

    print("[OK] Alle Veranstaltungen geprüft.")
    return report_rows


def write_orange_report(rows: list[dict[str, str]]) -> None:
    if not rows:
        print("[INFO] Keine gelben Mitarbeiter – es wird keine CSV erstellt.")
        return

    export_dir = Path(config.EXPORT_DIR)
    export_dir.mkdir(parents=True, exist_ok=True)
    filename = f"orange_schichten_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    path = export_dir / filename

    fieldnames = ["veranstaltung", "datum", "vorname", "nachname", "telefon"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "veranstaltung": row["event"],
                    "datum": row["date"],
                    "vorname": row["first_name"],
                    "nachname": row["last_name"],
                    "telefon": row["phone"],
                }
            )

    print(f"[OK] CSV mit gelben Mitarbeitern gespeichert: {path}")


def run_schicht_bestaetigen(headless: bool | None = None, slowmo_ms: int | None = None) -> None:
    """
    Hilfsfunktion für CLI: nutzt gespeicherten Login-State und führt nur den Klick aus.
    """
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    state_path = Path(config.STATE_PATH)
    if not state_path.exists():
        raise RuntimeError(
            f"[FEHLER] Kein gespeicherter Login-State unter {state_path}. Bitte zuerst 'login' ausführen."
        )

    print("[INFO] Starte Browser für Schicht-Bestätigung …")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        context = browser.new_context(storage_state=str(state_path))
        page = context.new_page()

        print("[INFO] Lade Startseite mit bestehender Session …")
        page.goto(config.BASE_URL, wait_until="load")

        try:
            frame = open_tagesplan_alt(page)
            frame = apply_filter(page, frame)
            events = collect_event_links(frame)
            phonebook = build_phonebook_from_overview(frame)
            rows = process_veranstaltungen(page, events, phonebook)
            write_orange_report(rows)
        finally:
            print("[INFO] Browser wird geschlossen …")
            browser.close()
