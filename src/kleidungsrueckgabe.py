import csv
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import Frame, Locator, Page, TimeoutError, sync_playwright

from src import config

AUSGABE_CODES = {"0005", "90"}
RUECKGABE_CODES = {"22", "0007"}


def _wait_for_inhalt_frame(page: Page, timeout_seconds: int = 25) -> Frame:
    """Polling-Helfer, weil PersPlan die Inhalte in Frames steckt."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        frame = page.frame(name="inhalt")
        if frame:
            return frame
        time.sleep(0.5)
    raise RuntimeError("[FEHLER] Frame 'inhalt' konnte nicht gefunden werden.")


def _open_user_overview(page: Page) -> Frame:
    """Lädt user.php direkt in den Inhaltsframe."""
    frame = _wait_for_inhalt_frame(page)
    target_url = urljoin(config.BASE_URL, "user.php")
    print(f"[INFO] Öffne Benutzerübersicht: {target_url}")
    frame.goto(target_url, wait_until="domcontentloaded", timeout=30000)
    frame.wait_for_selector("tr[id^='user_tbl_row_']", timeout=20000)
    return frame


def _ensure_ausgeschiedene_filter(frame: Frame) -> None:
    """Aktiviert den Filter 'Ausgeschiedene', falls noch nicht aktiv."""
    radio = frame.locator("#filter_anzeige_3")
    if radio.count() == 0:
        raise RuntimeError("[FEHLER] Filter 'Ausgeschiedene' (#filter_anzeige_3) wurde nicht gefunden.")

    try:
        if radio.is_checked():
            print("[INFO] Filter 'Ausgeschiedene' bereits aktiv.")
            return
    except Exception:
        pass

    print("[AKTION] Setze Filter 'Ausgeschiedene' …")
    radio.click()
    frame.wait_for_load_state("networkidle")
    frame.wait_for_selector("tr[id^='user_tbl_row_']", timeout=20000)
    print("[OK] Filter angewendet.")


def _collect_employee_rows(frame: Frame, max_rows: int | None = None) -> list[dict]:
    """Extrahiert die wichtigsten Infos aus jeder Tabellenzeile."""
    rows = frame.locator("tr[id^='user_tbl_row_']")
    total = rows.count()
    print(f"[INFO] Gefundene Zeilen: {total}")
    employees: list[dict] = []

    target_total = total if not max_rows or max_rows <= 0 else min(total, max_rows)

    for i in range(target_total):
        row = rows.nth(i)
        link = row.locator("a.ma_akte_link_text")
        if link.count() == 0:
            link = row.locator("a.ma_akte_link_img")
        if link.count() == 0:
            continue

        href = link.first.get_attribute("href")
        if not href:
            continue

        secure_fragment = ""
        if "secureid=" in href:
            secure_fragment = href.split("secureid=", 1)[1]

        tds = row.locator("td")
        personalnummer = ""
        status = ""
        vorname = ""
        nachname = ""
        try:
            cell_count = tds.count()
            if cell_count > 0:
                personalnummer = tds.nth(0).inner_text().strip()
            if cell_count > 1:
                status = tds.nth(1).inner_text().strip()
            if cell_count > 2:
                vorname = _normalize_text(tds.nth(2).inner_text())
            if cell_count > 3:
                nachname = _normalize_text(tds.nth(3).inner_text())
        except Exception:
            pass

        telefon = ""
        mobil = ""
        tel_links = row.locator("a[href^='tel:']")
        tel_count = tel_links.count()
        if tel_count > 0:
            telefon = (tel_links.nth(0).get_attribute("href") or "").replace("tel:", "").strip()
        if tel_count > 1:
            mobil = (tel_links.nth(1).get_attribute("href") or "").replace("tel:", "").strip()
        elif telefon:
            mobil = telefon

        email = ""
        email_links = row.locator("a[href^='mailto:']")
        if email_links.count() > 0:
            email = (email_links.first.get_attribute("href") or "").replace("mailto:", "").strip()

        employees.append(
            {
                "row_id": row.get_attribute("id") or "",
                "user_id": row.get_attribute("data-user_id") or "",
                "href": href,
                "secure_fragment": secure_fragment,
                "personalnummer": personalnummer,
                "status": status,
                "vorname": vorname,
                "nachname": nachname,
                "name": " ".join(part for part in [vorname, nachname] if part).strip() or "n/a",
                "telefon": telefon,
                "mobil": mobil,
                "email": email,
            }
        )

        if max_rows and max_rows > 0 and len(employees) >= max_rows:
            break

    if not employees:
        raise RuntimeError("[FEHLER] Keine Mitarbeiterzeilen gefunden (tr#user_tbl_row_*).")

    return employees


def _navigate_to_zulagen(page: Page) -> None:
    """Springt über das Submenü zur Zulagen-Ansicht."""
    menu = page.locator("#tableOfSubmenue")
    try:
        menu.wait_for(state="visible", timeout=10000)
    except Exception:
        menu = page.locator("a", has_text="Zulagen")

    locator = menu.locator("a", has_text="Zulagen")
    if locator.count() == 0:
        locator = page.locator("a", has_text="Zulagen")
    if locator.count() == 0:
        print("[WARNUNG] Kein 'Zulagen'-Link gefunden – überspringe Navigation.")
        return

    link = locator.first
    href = link.get_attribute("href")
    print(f"[INFO] Navigiere zu 'Zulagen' (aktuelle URL: {page.url})")

    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            link.click()
        print("[OK] Zulagen via Menü geöffnet.")
    except TimeoutError:
        if href:
            target_url = urljoin(config.BASE_URL, href)
            print(f"[WARNUNG] Kein Navigationsevent – rufe direkt auf: {target_url}")
            page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
        else:
            print("[WARNUNG] Konnte Zulagen nicht öffnen (kein href).")
    except Exception as exc:
        if href:
            target_url = urljoin(config.BASE_URL, href)
            print(f"[WARNUNG] Klick fehlgeschlagen ({exc}) – Fallback GET: {target_url}")
            page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
        else:
            print(f"[FEHLER] Klick auf 'Zulagen' fehlgeschlagen: {exc}")

    try:
        page.wait_for_selector("table", timeout=7000)
    except Exception:
        pass


def _normalize_text(value: str) -> str:
    return " ".join((value or "").replace("\xa0", " ").split())


def _is_debug_employee(employee: dict | None) -> bool:
    if not employee:
        return False
    debug_rows = getattr(config, "KLEIDUNGS_DEBUG_ROWS", set())
    if not debug_rows:
        return False
    candidates = [
        employee.get("row_id"),
        employee.get("user_id"),
        employee.get("personalnummer"),
        employee.get("secure_fragment"),
    ]
    return any(candidate in debug_rows for candidate in candidates if candidate)


def _debug_log(employee: dict | None, message: str) -> None:
    if not _is_debug_employee(employee):
        return
    identifier = (
        employee.get("row_id")
        or employee.get("personalnummer")
        or employee.get("secure_fragment")
        or employee.get("name")
        or "n/a"
    )
    print(f"[KLEIDUNG-DEBUG:{identifier}] {message}")


def _canonicalize_code(code: str) -> str:
    normalized = (code or "").strip()
    if not normalized:
        return ""
    if normalized.isdigit():
        return normalized.lstrip("0") or "0"
    return normalized


def _match_configured_code(raw_code: str, configured_codes: set[str]) -> str | None:
    canonical = _canonicalize_code(raw_code)
    if not canonical:
        return None
    for configured in configured_codes:
        if canonical == _canonicalize_code(configured):
            return configured
    return None


def _get_column_index(table: Locator, header_fragment: str) -> int:
    headers = table.locator("thead th")
    total = headers.count()
    fragment = header_fragment.lower()
    for idx in range(total):
        text = headers.nth(idx).inner_text().strip().lower()
        if fragment in text:
            return idx
    return -1


def _parse_numeric_text(value: str | None) -> float | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    normalized = normalized.replace("\xa0", "").replace(" ", "")
    if "," in normalized and "." in normalized:
        normalized = normalized.replace(".", "").replace(",", ".")
    elif "," in normalized:
        normalized = normalized.replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


def _extract_cell_numeric(row: Locator, index: int) -> float | None:
    if index < 0:
        return None
    cells = row.locator("td")
    count = cells.count()
    if index >= count:
        return None
    cell = cells.nth(index)
    candidate = cell.get_attribute("data-order") or cell.inner_text()
    return _parse_numeric_text(candidate)


def _extract_lohnart_code(lohnart_text: str) -> str:
    normalized = _normalize_text(lohnart_text)
    if not normalized:
        return ""
    match = re.search(r"\d+", normalized)
    if not match:
        return ""
    return match.group(0)


def _select_amount(value_amount: float | None, ansatz_amount: float | None) -> float | None:
    if value_amount is None and ansatz_amount is None:
        return None
    if ansatz_amount is None:
        return value_amount
    if value_amount is None:
        return ansatz_amount

    # Wenn einer der Werte absolut deutlich größer ist (typisch Geldbetrag), nimm diesen.
    if abs(ansatz_amount) > abs(value_amount) + 0.01:
        return ansatz_amount
    if abs(value_amount) > abs(ansatz_amount) + 0.01:
        return value_amount

    # Wenn einer nahe bei 1 liegt (Ansatz), wähle den anderen.
    if abs(value_amount) <= 1.01 < abs(ansatz_amount):
        return ansatz_amount
    if abs(ansatz_amount) <= 1.01 < abs(value_amount):
        return value_amount

    # Fallback: nimm den Wert, der nicht Ganzzahl 1 ist.
    if abs(value_amount - 1.0) < 0.01 and abs(ansatz_amount - 1.0) >= 0.01:
        return ansatz_amount
    if abs(ansatz_amount - 1.0) < 0.01 and abs(value_amount - 1.0) >= 0.01:
        return value_amount

    # Letzte Option: Wert-Spalte bevorzugen.
    return value_amount


def _evaluate_kleidungsstatus(page: Page, employee: dict | None = None) -> dict:
    table = page.locator("#mitarbeiter_zulagen")
    try:
        table.wait_for(state="visible", timeout=5000)
    except Exception:
        pass

    try:
        page.wait_for_selector("#mitarbeiter_zulagen tbody tr", timeout=10000)
    except TimeoutError:
        return {
            "comment": "Zulagen-Tabelle konnte nicht geladen werden.",
            "ausgabe_codes": "",
            "rueckgabe_codes": "",
        }

    if table.count() == 0:
        return {
            "comment": "Zulagen-Tabelle nicht gefunden.",
            "ausgabe_codes": "",
            "rueckgabe_codes": "",
        }

    rows = table.locator("tbody tr")
    row_count = rows.count()
    print(f"[DEBUG] Zulagen-Tabelle: {row_count} Zeilen erkannt.")
    if row_count == 0 or rows.first.locator("td.dataTables_empty").count() > 0:
        return {
            "comment": "Keine Einträge vorhanden.",
            "ausgabe_codes": "",
            "rueckgabe_codes": "",
            "wert_diff": "",
        }

    value_col_index = _get_column_index(table, "wert")
    ansatz_col_index = _get_column_index(table, "ansatz")
    lohnart_col_index = _get_column_index(table, "lohnart")

    if value_col_index < 0:
        value_col_index = 3
    if ansatz_col_index < 0:
        ansatz_col_index = 4
    if lohnart_col_index < 0:
        lohnart_col_index = 6
    ausgabe_found: set[str] = set()
    rueckgabe_found: set[str] = set()
    balance_value = 0.0
    relevant_rows = 0

    for i in range(row_count):
        row = rows.nth(i)
        cells_locator = row.locator("td")
        cell_count = cells_locator.count()
        value_amount = _extract_cell_numeric(row, value_col_index)
        ansatz_amount = _extract_cell_numeric(row, ansatz_col_index)
        lohnart_text = ""
        lohnart_code = ""
        if lohnart_col_index < cell_count:
            lohnart_text = cells_locator.nth(lohnart_col_index).inner_text().strip()
            lohnart_code = _extract_lohnart_code(lohnart_text)

        matched_ausgabe = _match_configured_code(lohnart_code, AUSGABE_CODES)
        matched_rueckgabe = _match_configured_code(lohnart_code, RUECKGABE_CODES)
        if not matched_ausgabe and not matched_rueckgabe:
            continue

        amount = _select_amount(value_amount, ansatz_amount)
        if amount is None:
            _debug_log(
                employee,
                f"Zulage-Zeile {i}: lohnart='{lohnart_text or lohnart_code}' übersprungen "
                f"(Wert={value_amount!r}, Ansatz={ansatz_amount!r})",
            )
            continue

        if matched_ausgabe:
            ausgabe_found.add(matched_ausgabe)
        if matched_rueckgabe:
            rueckgabe_found.add(matched_rueckgabe)

        balance_value += amount
        relevant_rows += 1
        _debug_log(
            employee,
            f"Zulage-Zeile {i}: lohnart='{lohnart_text or lohnart_code}' "
            f"match(Ausgabe={matched_ausgabe or '-'}, Rueckgabe={matched_rueckgabe or '-'}) "
            f"-> Betrag {amount:+.2f} EUR (Wert={value_amount!r}, Ansatz={ansatz_amount!r}), "
            f"Zwischensaldo {balance_value:+.2f} EUR",
        )

    if ausgabe_found and not rueckgabe_found:
        comment = "Kleidungsausgabe, aber keine Rückgabe hinterlegt."
    elif not ausgabe_found and rueckgabe_found:
        comment = "Nur Rückgabe erfasst."
    elif not ausgabe_found and not rueckgabe_found:
        comment = "Keine relevanten Lohnarten gefunden."
    else:
        if abs(balance_value) > 0.01:
            comment = f"Saldo offen ({balance_value:+.2f} EUR)"
        else:
            comment = ""

    result = {
        "comment": comment,
        "ausgabe_codes": ", ".join(sorted(ausgabe_found)),
        "rueckgabe_codes": ", ".join(sorted(rueckgabe_found)),
        "wert_diff": f"{balance_value:.2f}" if relevant_rows > 0 else "",
    }
    _debug_log(
        employee,
        f"Ergebnis: Ausgaben={sorted(ausgabe_found) or '-'} "
        f"Rückgaben={sorted(rueckgabe_found) or '-'} "
        f"Saldo={balance_value:+.2f} EUR Kommentar='{comment or 'OK'}'",
    )
    return result


def _find_anchor_locator(frame: Frame, employee: dict) -> Locator | None:
    """Sucht den passenden Link innerhalb der Tabelle für einen Datensatz."""
    selectors: list[str] = []
    row_id = employee.get("row_id")
    if row_id:
        selectors.extend(
            [
                f"#{row_id} a.ma_akte_link_text",
                f"#{row_id} a.ma_akte_link_img",
            ]
        )
    secure_fragment = employee.get("secure_fragment")
    if secure_fragment:
        selectors.extend(
            [
                f"a.ma_akte_link_text[href*='{secure_fragment}']",
                f"a.ma_akte_link_img[href*='{secure_fragment}']",
            ]
        )

    for selector in selectors:
        locator = frame.locator(selector)
        if locator.count() > 0:
            return locator.first
    return None


def _open_akte_tab(page: Page, frame: Frame, employee: dict):
    """Versucht über einen echten Klick (neuer Tab) in die Akte zu wechseln."""
    locator = _find_anchor_locator(frame, employee)
    href = employee.get("href") or ""
    if locator:
        try:
            locator.scroll_into_view_if_needed()
        except Exception:
            pass
        print("[AKTION] Öffne Akte via Tabellen-Link …")
        try:
            with page.context.expect_page(timeout=20000) as popup_event:
                locator.click()
            akte_page = popup_event.value
            akte_page.wait_for_load_state("domcontentloaded", timeout=30000)
            return akte_page
        except TimeoutError:
            print("[WARNUNG] Kein neues Tab erhalten – versuche Direktaufruf.")
        except Exception as exc:
            print(f"[FEHLER] Klick auf Tabellen-Link fehlgeschlagen: {exc}")

    if not href:
        raise RuntimeError("[FEHLER] Es gibt keinen Link (href), um die Akte aufzurufen.")

    print("[WARNUNG] Nutze Direktaufruf.")
    akte_url = urljoin(config.BASE_URL, href)
    print(f"[INFO] Öffne Akte direkt: {akte_url}")
    fallback_page = page.context.new_page()
    fallback_page.goto(akte_url, wait_until="domcontentloaded", timeout=30000)
    return fallback_page


def _process_employee(page: Page, frame: Frame, employee: dict) -> tuple[dict, Frame]:
    """Öffnet die Akte (bevorzugt per Klick), liest Stammdaten und springt zu Zulagen."""
    akte_page = _open_akte_tab(page, frame, employee)

    try:
        contact = {
            "name": employee.get("name", "n/a"),
            "vorname": employee.get("vorname", ""),
            "nachname": employee.get("nachname", ""),
            "telefon": employee.get("telefon", ""),
            "mobil": employee.get("mobil", ""),
            "email": employee.get("email", "n/a"),
        }
        _navigate_to_zulagen(akte_page)
        zulagen_result = _evaluate_kleidungsstatus(akte_page, employee)
        contact.update(zulagen_result)
        contact.update({"akte_url": akte_page.url})
    finally:
        akte_page.close()
        page.bring_to_front()

    refreshed_frame = _wait_for_inhalt_frame(page)
    return contact, refreshed_frame


def run_kleidungsrueckgabe(headless: bool | None = None, slowmo_ms: int | None = None) -> Path:
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    state_path = Path(config.STATE_PATH)
    if not state_path.exists():
        raise RuntimeError(f"Kein gespeicherter Login-State unter {state_path}. Bitte zuerst 'login' ausführen.")

    export_dir = Path(config.EXPORT_DIR)
    export_dir.mkdir(parents=True, exist_ok=True)
    csv_path = export_dir / f"kleidungsrueckgabe_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        context = browser.new_context(storage_state=str(state_path))
        page = context.new_page()

        page.goto(config.BASE_URL, wait_until="load")
        frame = _open_user_overview(page)
        _ensure_ausgeschiedene_filter(frame)

        limit = config.MAX_MA_LOOP if isinstance(config.MAX_MA_LOOP, int) else 0
        kleidung_limit = config.KLEIDUNGS_MAX_ROWS if hasattr(config, "KLEIDUNGS_MAX_ROWS") else 0
        effective_limit = limit if limit and limit > 0 else kleidung_limit

        employees = _collect_employee_rows(frame, max_rows=effective_limit)

        if effective_limit and effective_limit > 0:
            print(f"[INFO] Verarbeite nur die ersten {effective_limit} Datensätze (konfiguriert).")

        with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
            fieldnames = [
                "laufende_nr",
                "row_id",
                "user_id",
                "personalnummer",
                "status",
                "vorname",
                "nachname",
                "name",
                "telefon",
                "mobil",
                "email",
                "comment",
                "ausgabe_codes",
                "rueckgabe_codes",
                "wert_diff",
                "akte_url",
            ]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for idx, employee in enumerate(employees, start=1):
                print(f"\n{'-' * 50}\n[INFO] Verarbeite Datensatz {idx}/{len(employees)} …")
                print(
                    "[DEBUG] row_id={row_id} user_id={user_id} persnr={pernr} href={href}".format(
                        row_id=employee.get("row_id") or "-",
                        user_id=employee.get("user_id") or "-",
                        pernr=employee.get("personalnummer") or "-",
                        href=employee.get("href") or "-",
                    )
                )
                try:
                    data, frame = _process_employee(page, frame, employee)
                    writer.writerow(
                        {
                            "laufende_nr": idx,
                            "row_id": employee["row_id"],
                            "user_id": employee["user_id"],
                            "personalnummer": employee["personalnummer"],
                            "status": employee["status"],
                            "vorname": data.get("vorname", ""),
                            "nachname": data.get("nachname", ""),
                            "name": data.get("name", "n/a"),
                            "telefon": data.get("telefon", ""),
                            "mobil": data.get("mobil", "n/a"),
                            "email": data.get("email", "n/a"),
                            "comment": data.get("comment", ""),
                            "ausgabe_codes": data.get("ausgabe_codes", ""),
                            "rueckgabe_codes": data.get("rueckgabe_codes", ""),
                            "wert_diff": data.get("wert_diff", ""),
                            "akte_url": data.get("akte_url", ""),
                        }
                    )
                    csv_file.flush()
                    print(f"[OK] Kontaktinfos gesichert für {data.get('name', 'n/a')}")
                except Exception as exc:
                    print(f"[FEHLER] Konnte Datensatz {idx} nicht verarbeiten: {exc}")

        browser.close()

    print(f"\n[ENDE] Export gespeichert unter: {csv_path}")
    return csv_path


if __name__ == "__main__":
    run_kleidungsrueckgabe(headless=False, slowmo_ms=150)
