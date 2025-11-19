# src/mitarbeiter_loop.py
from playwright.sync_api import Page
import csv
import time
import re
from src.anfragen_parser import extract_anfragen
from src.dienstplan_parser import extract_dienstplaene
from src import config


def _clean_name_from_target(target_val: str) -> str:
    """Hilfsfunktion: Wandelt target="Amann__Cosmo_Valentin" in "Amann, Cosmo Valentin" um."""
    if not target_val:
        return ""
    cleaned = target_val.replace("__", ", ").replace("_", " ")
    return " ".join(cleaned.split()).strip()


def _extract_personalnummer(page: Page) -> str:
    """
    Liest die Personalnummer aus der Mitarbeiterakte.
    Erkennt Varianten wie:
      - PerNr.: 14655
      - Personal-Nr.: 14655
      - Personal Nr.: 14655
    """
    try:
        html = page.content()

        # Versuch 1: Regex direkt im HTML
        match = re.search(r"(?:PerNr\.|Personal[-\s]?Nr\.?)\s*:\s*(\d+)", html, re.IGNORECASE)
        if match:
            return match.group(1)

        # Versuch 2: Textknoten mit "Nr"
        locator = page.locator(":text('Nr')")
        for i in range(locator.count()):
            text = locator.nth(i).inner_text()
            match = re.search(r"(?:PerNr\.|Personal[-\s]?Nr\.?)\s*:\s*(\d+)", text, re.IGNORECASE)
            if match:
                return match.group(1)

        # Versuch 3: gezielt <span> mit "Personal-Nr"
        span_candidates = page.locator("span:has-text('Personal-Nr')")
        if span_candidates.count() > 0:
            text = span_candidates.first.inner_text()
            match = re.search(r"(\d+)", text)
            if match:
                return match.group(1)

    except Exception:
        pass

    return ""


def _extract_kommentar(page: Page) -> str:
    """
    Liest aus der Tabelle in der Anfragen-Ansicht den Status/Kommentar:
    - 'Keine Anfragen', 'Urlaub', 'Teilweise verfügbar' usw.
    - Wenn ein Drop-/Edit-Button vorhanden ist -> 'Schicht'
    """
    try:
        html = page.content()

        # Wenn Schicht-Button (drop/edit icon) existiert
        if re.search(r'class="[^"]*sprite_16x16[^"]*drop', html):
            return "Schicht"

        # Sonst Textinhalt der Tabelle lesen (erste TD-Zeile)
        match = re.search(
            r'<td[^>]*class="[^"]*liste_border_simple[^"]*"[^>]*>(.*?)</td>',
            html,
            re.IGNORECASE | re.DOTALL,
        )
        if match:
            text = re.sub("<[^<]+?>", "", match.group(1))  # HTML-Tags entfernen
            text = text.replace("&nbsp;", " ").strip()
            return " ".join(text.split())

    except Exception:
        pass

    return ""


def loop_all_mitarbeiter(page: Page, csv_path: str, view: str = "anfragen"):
    """
    Geht auf der Staffing-Seite alle Mitarbeiter durch,
    öffnet deren Akte, ruft Anfragen/Dienstpläne ab und schreibt Ergebnisse in CSV.
    view="anfragen" oder "dienstplan" steuert, welches Tab geöffnet wird.
    Nutzt config.MAX_MA_LOOP als Limit (0 = alle).
    """
    view_key = (view or "anfragen").lower().strip()
    if view_key not in {"anfragen", "dienstplan"}:
        raise ValueError("view muss 'anfragen' oder 'dienstplan' sein")

    tab_labels = ["Anfragen"] if view_key == "anfragen" else ["Dienstpläne", "Dienstplan"]
    parser = extract_anfragen if view_key == "anfragen" else extract_dienstplaene
    print("[INFO] Suche nach allen Mitarbeiter-Links …")
    frame_content = None

    # Frame 'inhalt' abwarten
    for _ in range(40):
        frame_content = page.frame(name="inhalt")
        if frame_content:
            break
        time.sleep(0.5)
    if not frame_content:
        raise Exception("[FEHLER] Frame 'inhalt' nicht gefunden.")

    # Mitarbeiter-Links finden
    img_links = frame_content.locator("#tbl_staffing a img[title='Zur MA-Akte']")
    total = img_links.count()
    if total == 0:
        raise Exception("[FEHLER] Keine Mitarbeiter-Links gefunden.")
    print(f"[OK] {total} Mitarbeiter gefunden.")

    # Limit aus Konfiguration
    limit = config.MAX_MA_LOOP if isinstance(config.MAX_MA_LOOP, int) else 0
    if limit and limit > 0:
        print(f"[INFO] Limit aktiviert – es werden nur die ersten {limit} Mitarbeitenden verarbeitet.")
        total = min(total, limit)
    else:
        print("[INFO] Kein Limit gesetzt – alle Mitarbeitenden werden verarbeitet.")

    results = []
    csv_written = False

    def _write_results_to_csv():
        nonlocal csv_written
        if not results:
            print("[WARNUNG] Keine Einträge gefunden – CSV nicht erstellt.")
            csv_written = True
            return
        print(f"[INFO] Speichere Ergebnisse in {csv_path} …")
        keys = results[0].keys()
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(results)
        csv_written = True
        print(f"[OK] CSV erfolgreich gespeichert mit {len(results)} Einträgen.")

    try:
        # Hauptloop
        for i in range(total):
            print(f"\n{'=' * 50}")
            print(f"[INFO] Bearbeite Mitarbeiter {i + 1}/{total} …")

            img = img_links.nth(i)
            row = img.locator("xpath=ancestor::tr[1]")
            anchor = img.locator("xpath=ancestor::a[1]")

            # Name aus Staffing-Zeile extrahieren
            employee_name = ""
            try:
                if row.locator("td:nth-child(2) b").count() > 0:
                    employee_name = row.locator("td:nth-child(2) b").first.inner_text().strip()
                if not employee_name and row.locator("td:nth-child(2)").count() > 0:
                    full_td = row.locator("td:nth-child(2)").first.inner_text().strip()
                    employee_name = full_td.split("\n", 1)[0].strip()
                if not employee_name:
                    target_val = anchor.first.get_attribute("target")
                    employee_name = _clean_name_from_target(target_val) if target_val else ""
            except Exception:
                pass

            if not employee_name:
                employee_name = f"MA_{i + 1}"

            print(f"[OK] Erkannter Name: {employee_name}")

            # Telefonnummer extrahieren
            phone_number = ""
            try:
                tel_link = row.locator("a[href^='tel:']")
                if tel_link.count() > 0:
                    href = tel_link.first.get_attribute("href")
                    if href and href.startswith("tel:"):
                        phone_number = href.replace("tel:", "").strip()
            except Exception:
                pass
            print(f"[OK] Telefonnummer erkannt: {phone_number or '–'}")

            # Tab öffnen
            new_page = None
            try:
                with page.context.expect_page(timeout=10000) as new_page_event:
                    if anchor.count() > 0:
                        anchor.first.scroll_into_view_if_needed()
                        anchor.first.click()
                    else:
                        img.scroll_into_view_if_needed()
                        img.click()

                new_page = new_page_event.value
                new_page.wait_for_load_state("domcontentloaded", timeout=15000)
                new_page.bring_to_front()
            except Exception as e:
                print(f"[FEHLER] Konnte Mitarbeiterakte nicht öffnen: {e}")
                if new_page:
                    try:
                        new_page.close()
                    except Exception:
                        pass
                time.sleep(0.5)
                continue

            try:
                # Personalnummer lesen
                personalnummer = _extract_personalnummer(new_page)
                if personalnummer:
                    print(f"[OK] Personalnummer erkannt: {personalnummer}")
                else:
                    print("[WARNUNG] Keine Personalnummer gefunden.")

                # Auf entsprechendes Tab klicken
                target_link = None
                for _ in range(60):
                    for label in tab_labels:
                        locator = new_page.locator("a", has_text=label)
                        if locator.count() > 0:
                            target_link = locator.first
                            break
                    if target_link:
                        break
                    time.sleep(0.25)

                if not target_link:
                    label_text = ", ".join(tab_labels)
                    raise Exception(f"Kein Link für '{label_text}' sichtbar.")

                target_link.scroll_into_view_if_needed()
                target_link.click()
                new_page.wait_for_load_state("domcontentloaded", timeout=10000)
                time.sleep(0.5)

                # Monat / Jahr setzen
                month_value = str(config.MONTH)
                year_value = str(config.YEAR)

                new_page.wait_for_selector("select#von_monat", timeout=8000)
                new_page.wait_for_selector("select#von_jahr", timeout=8000)

                new_page.select_option("select#von_monat", value=month_value)
                time.sleep(0.2)
                new_page.select_option("select#von_jahr", value=year_value)
                time.sleep(0.2)

                new_page.wait_for_load_state("networkidle", timeout=10000)

                # Kommentar (Status der Tabelle) extrahieren
                kommentar = ""
                if view_key == "anfragen":
                    kommentar = _extract_kommentar(new_page)
                    if kommentar:
                        print(f"[OK] Kommentar erkannt: {kommentar}")

                # Daten extrahieren
                eintraege = parser(new_page, return_list=True) or []

                for e in eintraege:
                    e["mitarbeiter"] = employee_name
                    e["personalnummer"] = personalnummer or ""
                    e["telefon"] = phone_number or ""
                    e["kommentar"] = kommentar or ""
                    results.append(e)

            except Exception as e:
                print(f"[FEHLER] Fehler bei Mitarbeiter {i + 1}: {e}")

            finally:
                try:
                    new_page.close()
                except Exception:
                    pass
                print("[OK] Tab geschlossen, zurück zur Staffing-Seite …")
                time.sleep(0.5)
    finally:
        if not csv_written:
            _write_results_to_csv()
