from playwright.sync_api import Page
import time
import csv
import os
import glob
import re
from stdnum import iban
from schwifty import IBAN


def parse_phone_number(raw_number: str):
    num = re.sub(r"[^\d+]", "", raw_number)
    country_code = "0049"
    local_number = num

    if num.startswith("+"):
        num = num.replace("+", "00")
    if num.startswith("0049"):
        country_code = "0049"
        local_number = num[4:]
    elif num.startswith("049"):
        country_code = "0049"
        local_number = num[3:]
    elif num.startswith("0"):
        country_code = "0049"
        local_number = num[1:]
    elif num.startswith("0039"):
        country_code = "0039"
        local_number = num[4:]
    elif num.startswith("0043"):
        country_code = "0043"
        local_number = num[4:]
    elif num.startswith("0041"):
        country_code = "0041"
        local_number = num[4:]

    if len(local_number) < 5:
        country_code = "0049"

    return country_code, local_number


def open_mitarbeiteranlage(page: Page):
    print("[INFO] Navigation: Administration → Mitarbeiter → Mitarbeiter anlegen")

    frame_top = None
    for _ in range(40):
        frame_top = page.frame(name="oben")
        if frame_top:
            break
        time.sleep(0.5)
    if not frame_top:
        raise Exception("[FEHLER] Frame 'oben' nicht gefunden.")

    admin_button = frame_top.locator("div.mainmenue_button_text", has_text="ADMINISTRATION")
    admin_button.wait_for(state="visible", timeout=8000)
    admin_button.click()

    frame_content = None
    for _ in range(60):
        frame_content = page.frame(name="inhalt")
        if frame_content and frame_content.locator("h2.reset_h2", has_text="Stammdaten").count() > 0:
            break
        time.sleep(0.5)
    if not frame_content:
        raise Exception("[FEHLER] ADMINISTRATION-Seite (Stammdaten) nicht erkannt.")

    frame_content.locator("a.jq_menueButtonMitIcon[title='Mitarbeiter']").first.click()

    for _ in range(60):
        if frame_content.locator("a[href='mitarbeiter_anlegen.php']").count() > 0:
            break
        time.sleep(0.5)

    add_button = frame_content.locator("a[href='mitarbeiter_anlegen.php']")
    add_button.first.scroll_into_view_if_needed()
    add_button.first.click()

    form_frame = None
    for _ in range(100):
        frame_content = page.frame(name="inhalt") or page.main_frame()
        if frame_content and frame_content.locator("form#maanlegen").count() > 0:
            form_frame = frame_content
            break
        time.sleep(0.5)
    if not form_frame:
        raise Exception("[FEHLER] Seite 'mitarbeiter_anlegen.php' nicht gefunden.")

    input_dir = "mitarbeiteranlage-input"
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    if not csv_files:
        raise Exception(f"[FEHLER] Keine CSV-Dateien im Ordner '{input_dir}' gefunden!")
    csv_path = csv_files[0]
    print(f"[INFO] Verwende CSV-Datei: {csv_path}")

    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        row = next(reader, None)
    if not row:
        raise Exception("[FEHLER] CSV-Datei enthält keine Datensätze!")

    mappings = {
        "Anrede": ["anrede"],
        "Vorname": ["vorname"],
        "Nachname": ["nachname"],
        "Email": ["email"],
        "Geburtsdatum": ["geburtsdatum"],
        "Geburtsort": ["geburtsort"],
        "Staatsbürgerschaft": ["staatsbuergerschaft"],
        "Straße und Hausnummer": ["anschrift"],
        "Postleitzahl": ["plz"],
        "Ort": ["ort"],
        "Bundesland": ["bundesland"],
        "Land": ["geburtsland", "land"],
        "Sozialversicherungsnummer": ["sozialversicherungsnummer"],
        "Personalausweisnummer": ["personalausweisnummer"],
    }

    print("[INFO] Formular wird ausgefüllt …")

    for csv_field, html_names in mappings.items():
        value = str(row.get(csv_field, "")).strip()
        if not value:
            print(f"[HINWEIS] Kein Wert für '{csv_field}', überspringe.")
            continue

        for html_name in html_names:
            locator = form_frame.locator(f"[name='{html_name}'], [id='{html_name}']")
            if locator.count() == 0:
                print(f"[WARNUNG] Feld '{html_name}' nicht gefunden – übersprungen.")
                continue

            el = locator.first
            tag = el.evaluate("el => el.tagName.toLowerCase()")

            try:
                if tag == "select":
                    options = [o.inner_text().strip() for o in el.locator("option").all()]
                    match = None
                    norm_val = value.lower().strip()
                    for opt in options:
                        if opt.lower().strip() == norm_val:
                            match = opt
                            break
                    if not match:
                        for opt in options:
                            if norm_val in opt.lower():
                                match = opt
                                break
                    if match:
                        el.select_option(label=match)
                        print(f"[OK] {html_name} (select) → {match}")
                    else:
                        if "deutsch" in norm_val:
                            el.select_option(label="Deutschland")
                            print(f"[OK] {html_name} (select fallback) → Deutschland")
                        else:
                            el.select_option(index=1)
                            print(f"[OK] {html_name} (select fallback index 1) → {value}")
                elif tag in ["input", "textarea"]:
                    if html_name.lower() in ["geburtsdatum", "geburts_datum", "birthday"]:
                        if "-" in value:
                            parts = value.split("-")
                            if len(parts) == 3:
                                value = f"{parts[2]}.{parts[1]}.{parts[0]}"
                        el.fill(value)
                        el.press("Enter")
                        print(f"[OK] {html_name} (format dd.mm.yyyy) → {value}")
                    else:
                        el.fill(value)
                        print(f"[OK] {html_name} → {value}")
            except Exception as e:
                print(f"[FEHLER] {html_name}: {e}")
                continue
            time.sleep(0.2)

    phone_raw = str(row.get("Mobil", "")).strip()
    if phone_raw:
        code, number = parse_phone_number(phone_raw)
        try:
            form_frame.locator("[name='laendervorwahl_mobil']").select_option(value=code)
            print(f"[OK] laendervorwahl_mobil (select) → {code}")
        except Exception as e:
            print(f"[FEHLER] Ländervorwahl nicht gesetzt ({code}): {e}")
        try:
            form_frame.locator("[name='mobil']").fill(number)
            print(f"[OK] mobil (number) → {number}")
        except Exception as e:
            print(f"[FEHLER] Mobilnummer nicht gesetzt: {e}")

    iban_value = str(row.get("IBAN", "")).strip()
    bic_value = str(row.get("BIC", "")).strip()

    if iban_value:
        if iban.is_valid(iban_value):
            print(f"[OK] IBAN gültig → {iban_value}")
            form_frame.locator("[name='iban']").fill(iban_value)
            if not bic_value:
                try:
                    iban_obj = IBAN(iban_value)
                    bic_value = iban_obj.bic or ""
                    if bic_value:
                        print(f"[AUTO] BIC aus IBAN ergänzt → {bic_value}")
                        form_frame.locator("[name='bic']").fill(bic_value)
                    else:
                        print("[HINWEIS] Keine BIC aus IBAN ableitbar.")
                except Exception as e:
                    print(f"[FEHLER] BIC-Autofill fehlgeschlagen: {e}")
            else:
                form_frame.locator("[name='bic']").fill(bic_value)
                print(f"[OK] BIC → {bic_value}")
        else:
            print(f"[FEHLER] IBAN ungültig → {iban_value}")

    for field, csv_name in {
        "bank": "Krankenkasse",
        "kontoinhaber": "Kontoinhaber",
        "steuernummer": "Steuer Identifikationsnummer"
    }.items():
        value = str(row.get(csv_name, "")).strip()
        if not value:
            print(f"[HINWEIS] Kein Wert für {csv_name}, überspringe.")
            continue
        try:
            form_frame.locator(f"[name='{field}']").fill(value)
            print(f"[OK] {field} → {value}")
        except Exception as e:
            print(f"[FEHLER] {field}: {e}")

    print("[FERTIG] Formularbefüllung abgeschlossen.")
    print("[INFO] Fenster bleibt 10 Sekunden offen …")
    time.sleep(10)
    print("[INFO] Browser wird geschlossen.")
