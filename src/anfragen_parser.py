from playwright.sync_api import Page
import re
import time


DATE_PATTERN = re.compile(r"[A-Za-zÄÖÜäöü]{1,3}\.\s*\d{2}\.\d{2}\.\d{2}")


def _extract_datum_from_row(row) -> str:
    span_datum = row.locator("span.datum")
    if span_datum.count() > 0:
        datum_text = span_datum.first.inner_text().strip()
        if datum_text:
            return datum_text

    cells = row.locator("td")
    for idx in range(cells.count()):
        text = cells.nth(idx).inner_text().strip()
        if DATE_PATTERN.match(text):
            return text

    for idx in range(cells.count()):
        text = cells.nth(idx).inner_text().strip()
        if text:
            return text

    return ""


def _find_eingeplant_column(page: Page) -> int | None:
    headers = page.locator("#tbl_ma_anfragen th")
    for idx in range(headers.count()):
        header_text = headers.nth(idx).inner_text().strip()
        if "Eingeplant" in header_text:
            return idx
    return None


def _extract_eingeplant_from_row(row, index: int | None) -> str:
    if index is None:
        return ""

    cells = row.locator("td")
    if index < cells.count():
        return cells.nth(index).inner_text().strip()

    return ""


def _is_unimportant_row(row, joined_lower: str) -> bool:
    """
    Detects rows which only describe holidays or placeholders.
    A row still counts if it explicitly says "keine Anfragen" (we want those).
    """
    if "keine anfragen" in joined_lower:
        return False

    row_class = (row.get_attribute("class") or "").lower()
    if "feiertag" in row_class:
        return True

    if row.locator(".feiertag").count() > 0:
        return True

    if row.locator(".unwichtige_zeile").count() > 0:
        return True

    return False


def extract_anfragen(page: Page, return_list=False):
    """
    Liest die Tabelle #tbl_ma_anfragen aus und gibt im Terminal
    pro Zeile eine Zusammenfassung aus (Urlaub, keine Anfragen oder Veranstaltung).

    Wenn return_list=True, wird zusätzlich eine Liste von Dicts zurückgegeben:
    [
        {"typ": "Urlaub", "datum": "01.10.25", "beschreibung": "Urlaub am 01.10.25", "eingeplant": ""},
        {"typ": "Anfrage", "datum": "03.11.25", "veranstaltung": "Käfer Messe", "uhrzeit": "11:00 - 16:00h", "beschreibung": "Käfer Messe – 11:00 - 16:00h am 03.11.25 (Eingeplant: nein)", "eingeplant": "nein"}
    ]
    """
    print("[INFO] Analysiere Anfragen-Tabelle …")

    # Warte, bis Tabelle existiert
    for _ in range(60):
        if page.locator("#tbl_ma_anfragen tr").count() > 0:
            break
        time.sleep(0.5)
    else:
        raise Exception("[FEHLER] Keine Tabelle mit ID #tbl_ma_anfragen gefunden.")

    rows = page.locator("#tbl_ma_anfragen tr[id^='tbl_ma_anfragen_row_']")
    row_count = rows.count()
    print(f"[OK] {row_count} Tabellenzeilen gefunden.")

    eingeplant_column_index = _find_eingeplant_column(page)
    if eingeplant_column_index is None:
        print("[WARNUNG] Spalte 'Eingeplant' konnte nicht automatisch ermittelt werden.")

    if row_count == 0:
        print("[WARNUNG] Keine Einträge in der Tabelle gefunden.")
        return [] if return_list else None

    result_list = []

    for i in range(row_count):
        row = rows.nth(i)
        tds = row.locator("td")
        td_texts = [td.inner_text().strip() for td in tds.all()]
        joined = " ".join(td_texts)
        if not joined:
            continue

        joined_lower = joined.lower()
        eingeplant_value = _extract_eingeplant_from_row(row, eingeplant_column_index)

        if _is_unimportant_row(row, joined_lower):
            datum = _extract_datum_from_row(row) or "?"
            beschreibung = joined.strip() or "Keine Schichten"
            text = f"{beschreibung} – keine Schichten am {datum}"
            print(f"➖ {text}")
            result_list.append({
                "typ": "Keine Anfragen",
                "datum": datum,
                "veranstaltung": "",
                "uhrzeit": "",
                "beschreibung": text,
                "eingeplant": eingeplant_value
            })
            continue

        # Muster erkennen
        if "urlaub" in joined_lower:
            datum = _extract_datum_from_row(row) or "?"
            text = f"Urlaub am {datum}"
            print(f"➡️  {text}")
            result_list.append({
                "typ": "Urlaub",
                "datum": datum,
                "veranstaltung": "",
                "uhrzeit": "",
                "beschreibung": text,
                "eingeplant": eingeplant_value
            })

        elif "keine anfragen" in joined_lower:
            datum = _extract_datum_from_row(row) or "?"
            text = f"Keine Anfragen am {datum}"
            print(f"➖ {text}")
            result_list.append({
                "typ": "Keine Anfragen",
                "datum": datum,
                "veranstaltung": "",
                "uhrzeit": "",
                "beschreibung": text,
                "eingeplant": eingeplant_value
            })

        else:
            veranstaltung = ""
            uhrzeit = ""
            datum = ""

            if row.locator("td:nth-child(4)").count() > 0:
                uhrzeit = row.locator("td:nth-child(4)").inner_text().strip()
            if row.locator("td:nth-child(5)").count() > 0:
                veranstaltung = row.locator("td:nth-child(5)").inner_text().strip()
            datum = _extract_datum_from_row(row)

            text = f"{veranstaltung or '–'} – {uhrzeit or '–'} am {datum or '?'}"
            if eingeplant_value:
                text += f" (Eingeplant: {eingeplant_value})"
            print(f"✅ Anfrage: {text}")
            result_list.append({
                "typ": "Anfrage",
                "datum": datum,
                "veranstaltung": veranstaltung,
                "uhrzeit": uhrzeit,
                "beschreibung": text,
                "eingeplant": eingeplant_value
            })

    print("[OK] Analyse der Anfragen abgeschlossen.\n")

    if return_list:
        return result_list
