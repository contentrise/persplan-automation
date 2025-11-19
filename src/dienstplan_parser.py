from playwright.sync_api import Page
import re
import time


DATE_PATTERN = re.compile(r"[A-Za-zÄÖÜäöü]{1,3}\.\s*\d{2}\.\d{2}\.\d{2}")


def _extract_datum(cell_values: list[str]) -> str:
    if len(cell_values) >= 2 and cell_values[1]:
        return cell_values[1]

    joined = " ".join(cell_values)
    match = DATE_PATTERN.search(joined)
    if match:
        return match.group(0)

    for value in cell_values:
        if DATE_PATTERN.match(value):
            return value

    for value in cell_values:
        if value:
            return value

    return ""


def _extract_status(row) -> str:
    status = row.locator(".statusContainer")
    if status.count() > 0:
        return status.first.inner_text().strip()
    return ""


def _row_has_assignment(cell_values: list[str]) -> bool:
    cleaned_values = [value.strip().lower() for value in cell_values]
    if any("feiertag" in value for value in cleaned_values):
        return False
    if any("keine schichten" in value for value in cleaned_values):
        return False
    payload = [value for value in cell_values[2:] if value]
    return len(payload) > 0


def extract_dienstplaene(page: Page, return_list: bool = False):
    """Liest die Tabelle #tbl_ma_dienstplane aus und erstellt dieselbe Struktur wie die Anfragen-Analyse."""
    print("[INFO] Analysiere Dienstplan-Tabelle …")

    for _ in range(60):
        if page.locator("#tbl_ma_dienstplane tr").count() > 0:
            break
        time.sleep(0.5)
    else:
        raise Exception("[FEHLER] Keine Tabelle mit ID #tbl_ma_dienstplane gefunden.")

    rows = page.locator("#tbl_ma_dienstplane tr[id^='tbl_ma_dienstplane_row_']")
    row_count = rows.count()
    print(f"[OK] {row_count} Tabellenzeilen gefunden.")

    if row_count == 0:
        print("[WARNUNG] Keine Einträge in der Tabelle gefunden.")
        return [] if return_list else None

    result_list = []

    for i in range(row_count):
        row = rows.nth(i)
        cells = row.locator("td")
        cell_values = [cells.nth(idx).inner_text().strip() for idx in range(cells.count())]
        joined = " ".join(cell_values).strip()
        if not joined:
            continue

        datum = _extract_datum(cell_values) or "?"
        uhrzeit = cell_values[2] if len(cell_values) > 2 else ""
        veranstaltung = cell_values[5] if len(cell_values) > 5 else ""
        status = _extract_status(row)

        if not _row_has_assignment(cell_values):
            beschreibung = joined
            lower = joined.lower()
            if "feiertag" in lower:
                beschreibung = f"Feiertag {datum} – keine Schichten am {datum}"
            elif "keine schichten" in lower:
                beschreibung = f"Keine Schichten am {datum}"
            else:
                beschreibung = f"Keine Schichten am {datum}"

            print(f"➖ {beschreibung}")
            result_list.append({
                "typ": "Keine Schichten",
                "datum": datum,
                "veranstaltung": "",
                "uhrzeit": "",
                "beschreibung": beschreibung,
                "eingeplant": status,
            })
            continue

        location = cell_values[4] if len(cell_values) > 4 else ""
        rolle = cell_values[7] if len(cell_values) > 7 else ""
        beschreibungsteile = []
        for part in (veranstaltung, location, rolle):
            if part:
                beschreibungsteile.append(part)
        beschreibung = " | ".join(beschreibungsteile) or "Dienst"
        beschreibung += f" – {uhrzeit or '–'} am {datum}"
        if status:
            beschreibung += f" (Status: {status})"

        print(f"✅ Dienst: {beschreibung}")
        result_list.append({
            "typ": "Dienst",
            "datum": datum,
            "veranstaltung": veranstaltung,
            "uhrzeit": uhrzeit,
            "beschreibung": beschreibung,
            "eingeplant": status,
        })

    print("[OK] Analyse der Dienstpläne abgeschlossen.\n")

    if return_list:
        return result_list
