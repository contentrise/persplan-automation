from __future__ import annotations

import csv
import json
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import Frame, Page, TimeoutError

from src import config


def _wait_for_inhalt_frame(page: Page, timeout_seconds: int = 20) -> Frame:
    """PersPlan nutzt Frames – hier pollt man bis der Inhaltsframe sichtbar ist."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        frame = page.frame(name="inhalt")
        if frame:
            return frame
        time.sleep(0.5)
    raise RuntimeError("[FEHLER] Frame 'inhalt' wurde nicht gefunden.")


def _open_kunden_liste(page: Page) -> Frame:
    """Lädt kunden.php im Inhaltsframe und wartet auf die Tabelle."""
    frame = _wait_for_inhalt_frame(page)
    target_url = urljoin(config.BASE_URL, "kunden.php")
    print(f"[INFO] Öffne Kundenliste: {target_url}")

    last_error: Exception | None = None
    for attempt in range(2):
        try:
            frame.goto(target_url, wait_until="domcontentloaded", timeout=30000)
            frame.wait_for_selector("#kunden_tbl_wrapper tr[id^='kunden_tbl_row_']", timeout=25000)
            length_select = frame.locator("#kunden_tbl_length select")
            if length_select.count() > 0:
                try:
                    length_select.select_option(value="-1")
                    frame.wait_for_load_state("networkidle")
                except Exception:
                    pass
            print("[OK] Kundenliste mit Tabellenzeilen erkannt.")
            return frame
        except TimeoutError as exc:
            last_error = exc
            print(f"[WARNUNG] Kundenliste konnte nicht geladen werden (Versuch {attempt + 1}/2).")
            time.sleep(2)

    raise RuntimeError(f"[FEHLER] Kundenliste nicht erreichbar: {last_error}")


def _click_company_name(frame: Frame, row_index: int) -> dict:
    """Klickt den Firmennamen (3. Spalte) in der gewünschten Tabellenzeile."""
    rows = frame.locator("tr[id^='kunden_tbl_row_']")
    total = rows.count()
    if total == 0:
        raise RuntimeError("[FEHLER] Keine Kundenzeilen gefunden (tr#kunden_tbl_row_*).")
    if row_index < 0 or row_index >= total:
        raise RuntimeError(
            f"[FEHLER] Angeforderter Zeilenindex {row_index} ist nicht verfügbar – total={total}."
        )

    row = rows.nth(row_index)
    row_id = row.get_attribute("id") or f"index-{row_index}"
    cells = row.locator("td")
    if cells.count() < 3:
        raise RuntimeError(f"[FEHLER] Zeile {row_id} hat weniger als 3 Spalten.")

    customer_number = _normalize_text(cells.nth(1).inner_text())

    link = cells.nth(2).locator("a").first
    if link.count() == 0:
        raise RuntimeError(f"[FEHLER] Kein Firmenlink in Zeile {row_id} gefunden.")

    name = link.inner_text().strip()
    href = link.get_attribute("href") or ""
    print(f"[AKTION] Öffne Kunde #{row_index + 1}: {name or 'Unbekannt'} ({row_id})")

    try:
        with frame.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            link.click()
    except TimeoutError:
        if href:
            target = urljoin(config.BASE_URL, href)
            print(f"[WARNUNG] Navigationstimeout – lade direkt: {target}")
            frame.goto(target, wait_until="domcontentloaded", timeout=30000)
        else:
            raise

    frame.wait_for_load_state("networkidle", timeout=20000)
    print(f"[OK] Kundendetail geladen: {name}")
    return {"row_id": row_id, "name": name, "href": href, "kundennummer": customer_number}


def _normalize_text(value: str) -> str:
    return " ".join((value or "").replace("\xa0", " ").split())


def _extract_customer_details(frame: Frame) -> dict[str, str]:
    """Liest die Label/Wert-Zeilen aus der Kundendetailansicht."""
    table = frame.locator("#scn_datatable_outer_table")
    if table.count() == 0:
        raise RuntimeError("[FEHLER] Tabelle #scn_datatable_outer_table nicht gefunden.")

    rows = table.locator("tr")
    details: dict[str, str] = {}
    total_rows = rows.count()

    for idx in range(total_rows):
        row = rows.nth(idx)
        cells = row.locator("td")
        if cells.count() < 2:
            continue

        label = _normalize_text(cells.nth(0).inner_text()).rstrip(":").strip()
        value = _normalize_text(cells.nth(1).inner_text())

        if not label or not value:
            continue

        key = label
        suffix = 2
        while key in details:
            key = f"{label}_{suffix}"
            suffix += 1

        details[key] = value

    if not details:
        print("[WARNUNG] Keine Kundendetails extrahiert (alle Felder leer?).")

    return details


def _open_rechnungsoptionen(frame: Frame) -> Frame:
    """Klickt im Submenü auf 'Rechnungsoptionen' und wartet auf die Tabelle."""
    submenu = frame.locator("#tableOfSubmenue")
    link = submenu.locator("a", has_text="Rechnungsoptionen")
    if link.count() == 0:
        raise RuntimeError("[FEHLER] Link 'Rechnungsoptionen' im Submenü nicht gefunden.")

    href = link.first.get_attribute("href")

    try:
        with frame.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            link.first.click()
    except TimeoutError:
        if href:
            target = urljoin(config.BASE_URL, href)
            print(f"[WARNUNG] Navigationstimeout – lade Rechnungsoptionen direkt: {target}")
            frame.goto(target, wait_until="domcontentloaded", timeout=30000)
        else:
            raise

    try:
        frame.wait_for_selector("#verrechnungssaetze_tbl", timeout=15000)
        print("[OK] Rechnungsoptionen geöffnet.")
    except TimeoutError:
        print("[WARNUNG] Tabelle #verrechnungssaetze_tbl nicht sichtbar – es könnten keine Daten vorliegen.")
    return frame


def _extract_rechnungsoptionen(frame: Frame) -> dict:
    """Extrahiert Verrechnungssätze aus der Rechnungsoptionen-Ansicht."""
    table = frame.locator("#verrechnungssaetze_tbl")
    if table.count() == 0:
        return {}

    rows = table.locator("tr[id^='verrechnungssaetze_tbl_row_']")
    total = rows.count()
    data: list[dict[str, str]] = []

    for idx in range(total):
        row = rows.nth(idx)
        cells = row.locator("td")
        if cells.count() < 4:
            continue
        funktion = _normalize_text(cells.nth(1).inner_text())
        satz = _normalize_text(cells.nth(2).inner_text())
        gueltig = _normalize_text(cells.nth(3).inner_text())
        entry = {}
        if funktion:
            entry["funktion"] = funktion
        if satz:
            entry["verrechnungssatz"] = satz
        if gueltig:
            entry["gueltig_ab"] = gueltig
        if entry:
            data.append(entry)

    meta = {}
    if data:
        meta["verrechnungssaetze"] = data
    info_locator = frame.locator("#verrechnungssaetze_tbl_info")
    if info_locator.count() > 0:
        text = _normalize_text(info_locator.inner_text())
        if text:
            meta["info"] = text
    return meta


def _open_gesperrte_mitarbeiter(frame: Frame) -> Frame:
    """Klickt im Submenü auf 'Gesperrte Mitarbeiter'."""
    submenu = frame.locator("#tableOfSubmenue")
    link = submenu.locator("a", has_text="Gesperrte Mitarbeiter")
    if link.count() == 0:
        raise RuntimeError("[FEHLER] Link 'Gesperrte Mitarbeiter' im Submenü nicht gefunden.")

    href = link.first.get_attribute("href")

    try:
        with frame.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            link.first.click()
    except TimeoutError:
        if href:
            target = urljoin(config.BASE_URL, href)
            print(f"[WARNUNG] Navigationstimeout – lade 'Gesperrte Mitarbeiter' direkt: {target}")
            frame.goto(target, wait_until="domcontentloaded", timeout=30000)
        else:
            raise

    try:
        frame.wait_for_selector("table.tbl_design", timeout=15000)
        print("[OK] 'Gesperrte Mitarbeiter' geöffnet.")
    except TimeoutError:
        print("[WARNUNG] Tabelle mit gesperrten Mitarbeitern nicht sichtbar – evtl. keine Einträge.")
    return frame


def _open_kundenhistorie(frame: Frame) -> Frame:
    """Klickt im Submenü auf 'Kundenhistorie'."""
    submenu = frame.locator("#tableOfSubmenue")
    link = submenu.locator("a", has_text="Kundenhistorie")
    if link.count() == 0:
        raise RuntimeError("[FEHLER] Link 'Kundenhistorie' im Submenü nicht gefunden.")

    href = link.first.get_attribute("href")

    try:
        with frame.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            link.first.click()
    except TimeoutError:
        if href:
            target = urljoin(config.BASE_URL, href)
            print(f"[WARNUNG] Navigationstimeout – lade 'Kundenhistorie' direkt: {target}")
            frame.goto(target, wait_until="domcontentloaded", timeout=30000)
        else:
            raise

    frame.wait_for_selector("#tbl_kundenhistorie", timeout=15000)
    print("[OK] Kundenhistorie geöffnet.")
    return frame


def _open_ansprechpartner(frame: Frame) -> Frame:
    """Klickt im Submenü auf 'Ansprechpartner'."""
    submenu = frame.locator("#tableOfSubmenue")
    link = submenu.locator("a", has_text="Ansprechpartner")
    if link.count() == 0:
        raise RuntimeError("[FEHLER] Link 'Ansprechpartner' im Submenü nicht gefunden.")

    href = link.first.get_attribute("href")

    try:
        with frame.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            link.first.click()
    except TimeoutError:
        if href:
            target = urljoin(config.BASE_URL, href)
            print(f"[WARNUNG] Navigationstimeout – lade 'Ansprechpartner' direkt: {target}")
            frame.goto(target, wait_until="domcontentloaded", timeout=30000)
        else:
            raise

    frame.wait_for_selector("#ansprechpartner_tbl", timeout=15000)
    print("[OK] Ansprechpartner geöffnet.")
    return frame


def _extract_ansprechpartner(frame: Frame) -> list[dict[str, str]]:
    """Extrahiert alle Ansprechpartner-Zeilen."""
    table = frame.locator("#ansprechpartner_tbl")
    if table.count() == 0:
        return []

    length_select = frame.locator("#ansprechpartner_tbl_length select")
    if length_select.count() > 0:
        try:
            length_select.select_option(value="-1")
            frame.wait_for_load_state("networkidle")
        except Exception:
            pass

    rows = table.locator("tr[id^='ansprechpartner_tbl_row_']")
    total = rows.count()
    entries: list[dict[str, str]] = []

    for idx in range(total):
        row = rows.nth(idx)
        cells = row.locator("td")
        if cells.count() < 10:
            continue
        entry = {}
        anrede = _normalize_text(cells.nth(1).inner_text())
        if anrede:
            entry["anrede"] = anrede
        titel = _normalize_text(cells.nth(2).inner_text())
        if titel:
            entry["titel"] = titel
        vorname = _normalize_text(cells.nth(3).inner_text())
        if vorname:
            entry["vorname"] = vorname
        nachname = _normalize_text(cells.nth(4).inner_text())
        if nachname:
            entry["name"] = nachname
        position = _normalize_text(cells.nth(5).inner_text())
        if position:
            entry["position"] = position
        email = _normalize_text(cells.nth(6).inner_text())
        if email:
            entry["email"] = email
        telefon = _normalize_text(cells.nth(7).inner_text())
        if telefon:
            entry["telefon"] = telefon
        mobil = _normalize_text(cells.nth(8).inner_text())
        if mobil:
            entry["mobil"] = mobil
        fax = _normalize_text(cells.nth(9).inner_text())
        if fax:
            entry["fax"] = fax
        if entry:
            entries.append(entry)

    return entries


def _set_history_filters(frame: Frame, start_date: str, end_date: str) -> None:
    """Setzt den Zeitraumfilter und lädt alle Einträge."""
    frame.locator("#date_von").fill(start_date)
    frame.locator("#date_bis").fill(end_date)
    show_button = frame.locator("button.pointer", has=frame.locator("img.arrow_refresh"))
    if show_button.count() > 0:
        show_button.first.click()
        frame.wait_for_load_state("networkidle")

    length_select = frame.locator("#tbl_kundenhistorie_length select")
    if length_select.count() > 0:
        try:
            length_select.select_option(value="-1")
            frame.wait_for_load_state("networkidle")
        except Exception:
            pass


def _extract_kundenhistorie(frame: Frame) -> list[dict[str, str]]:
    """Liest die Historientabelle und liefert eine Liste von Einträgen."""
    table = frame.locator("#tbl_kundenhistorie")
    if table.count() == 0:
        return []

    rows = table.locator("tr[id^='tbl_kundenhistorie_row_']")
    total = rows.count()
    entries: list[dict[str, str]] = []

    for idx in range(total):
        row = rows.nth(idx)
        cells = row.locator("td")
        if cells.count() < 6:
            continue
        entry = {}
        datum = _normalize_text(cells.nth(1).inner_text())
        if datum:
            entry["datum"] = datum
        mitarbeiter = _normalize_text(cells.nth(2).inner_text())
        if mitarbeiter:
            entry["mitarbeiter"] = mitarbeiter
        ansprechpartner = _normalize_text(cells.nth(3).inner_text())
        if ansprechpartner:
            entry["ansprechpartner"] = ansprechpartner
        aktion = _normalize_text(cells.nth(4).inner_text())
        if aktion:
            entry["aktion"] = aktion
        bemerkung = _normalize_text(cells.nth(5).inner_text())
        if bemerkung:
            entry["bemerkung"] = bemerkung
        anhang = _normalize_text(cells.nth(6).inner_text()) if cells.count() > 6 else ""
        if anhang:
            entry["anhang"] = anhang
        if entry:
            entries.append(entry)

    return entries


def _extract_blocked_employees(frame: Frame) -> list[dict[str, str]]:
    """Extrahiert die Zeilen der gesperrten Mitarbeiter."""
    table_locator = frame.locator("table.tbl_design")
    if table_locator.count() == 0:
        return []
    table = table_locator.first

    rows = table.locator("tr")
    total = rows.count()
    entries: list[dict[str, str]] = []

    for idx in range(total):
        if idx == 0:
            continue  # Überschriftenzeile
        row = rows.nth(idx)
        cells = row.locator("td")
        if cells.count() < 5:
            continue
        entry = {}
        pers_nr = _normalize_text(cells.nth(0).inner_text())
        if pers_nr:
            entry["personalnummer"] = pers_nr
        name = _normalize_text(cells.nth(1).inner_text())
        if name:
            entry["mitarbeiter"] = name
        note = _normalize_text(cells.nth(2).inner_text())
        if note:
            entry["bemerkung"] = note
        date = _normalize_text(cells.nth(3).inner_text())
        if date:
            entry["datum"] = date
        author = _normalize_text(cells.nth(4).inner_text())
        if author:
            entry["eintrag_von"] = author
        if entry:
            entries.append(entry)

    return entries


def _append_csv_row(
    csv_path: Path,
    timestamp: str,
    customer_name: str,
    customer_number: str,
    details: dict,
):
    """Schreibt einen Datensatz mit Zeitstempel, Name, Kundennummer und JSON-Details."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    is_new_file = not csv_path.exists()
    payload = json.dumps(details, ensure_ascii=False, separators=(",", ":"))

    with csv_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter=";")
        if is_new_file:
            writer.writerow(["timestamp", "kunde", "kundennummer", "details_json"])
        writer.writerow([timestamp, customer_name, customer_number, payload])


def run_kunden_scraper(
    page: Page,
    max_customers: int = 1,
    csv_path: str | Path | None = None,
    timestamp: str | None = None,
) -> Path:
    """
    Öffnet kunden.php und klickt nacheinander auf den Firmennamen in jeder Zeile.
    Standardmäßig wird nur der erste Eintrag geöffnet.
    """
    csv_file = Path(csv_path) if csv_path else Path(config.EXPORT_DIR) / "kunden_details.csv"
    run_timestamp = timestamp or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    frame = _open_kunden_liste(page)
    total_rows = frame.locator("tr[id^='kunden_tbl_row_']").count()
    print(f"[INFO] Gefundene Kundenzeilen: {total_rows}")

    if total_rows == 0:
        raise RuntimeError("[FEHLER] Keine Kundenzeilen verfügbar.")

    if max_customers and max_customers > 0:
        to_process = min(total_rows, max_customers)
    else:
        to_process = total_rows

    if to_process == 0:
        raise RuntimeError("[FEHLER] Keine Kundenzeilen verfügbar (nach Limitierung).")
    if not max_customers or max_customers <= 0:
        print("[INFO] Kein Limit gesetzt – verarbeite alle Kundenzeilen.")

    for idx in range(to_process):
        if idx > 0:
            frame = _open_kunden_liste(page)
        info = _click_company_name(frame, idx)
        details = _extract_customer_details(frame)
        try:
            rechnung_frame = _open_rechnungsoptionen(frame)
            tmp = _extract_rechnungsoptionen(rechnung_frame)
            rechnungs_info = tmp if tmp else "na"
        except Exception as exc:
            print(f"[WARNUNG] Rechnungsoptionen nicht verfügbar: {exc}")
            rechnungs_info = "na"

        try:
            blocked_frame = _open_gesperrte_mitarbeiter(frame)
            tmp = _extract_blocked_employees(blocked_frame)
            blocked_employees = tmp if tmp else "na"
        except Exception as exc:
            print(f"[WARNUNG] Gesperrte Mitarbeiter nicht verfügbar: {exc}")
            blocked_employees = "na"

        try:
            history_frame = _open_kundenhistorie(frame)
            _set_history_filters(history_frame, "01.01.2023", datetime.now().strftime("%d.%m.%Y"))
            tmp = _extract_kundenhistorie(history_frame)
            history_entries = tmp if tmp else "na"
        except Exception as exc:
            print(f"[WARNUNG] Kundenhistorie nicht verfügbar: {exc}")
            history_entries = "na"

        try:
            contacts_frame = _open_ansprechpartner(frame)
            tmp = _extract_ansprechpartner(contacts_frame)
            contacts = tmp if tmp else "na"
        except Exception as exc:
            print(f"[WARNUNG] Ansprechpartner nicht verfügbar: {exc}")
            contacts = "na"
        customer_name = info.get("name") or details.get("Firma *") or "Unbekannter Kunde"
        customer_number = info.get("kundennummer") or details.get("Kundennummer") or ""
        payload = {
            "stammdaten": details,
            "rechnungsoptionen": rechnungs_info,
            "gesperrte_mitarbeiter": blocked_employees,
            "kundenhistorie": history_entries,
            "ansprechpartner": contacts,
        }
        _append_csv_row(csv_file, run_timestamp, customer_name, customer_number, payload)
        print(f"[OK] Details gespeichert für {customer_name} → {csv_file}")
        if idx + 1 < to_process:
            print("[INFO] Kehre zur Übersicht zurück, um den nächsten Kunden zu öffnen …")
        else:
            print("[INFO] Kundenklick abgeschlossen – stoppe wie angefordert nach diesem Schritt.")

    return csv_file
