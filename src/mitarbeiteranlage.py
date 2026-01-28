from playwright.sync_api import Page
import time
import csv
import os
import glob
import re
import json
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


def _pick_value(payload: dict, keys: list[str]) -> str:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            for entry in value:
                text = str(entry).strip()
                if text:
                    return text
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _row_from_json(payload: dict) -> dict:
    return {
        "Anrede": _pick_value(payload, ["anrede"]),
        "Vorname": _pick_value(payload, ["vorname"]),
        "Nachname": _pick_value(payload, ["nachname"]),
        "Email": _pick_value(payload, ["email"]),
        "Geburtsdatum": _pick_value(payload, ["geburtsdatum"]),
        "Geburtsort": _pick_value(payload, ["geburtsort"]),
        "Staatsbürgerschaft": _pick_value(payload, ["staatsbuergerschaft"]),
        "Straße und Hausnummer": _pick_value(payload, ["anschrift"]),
        "Postleitzahl": _pick_value(payload, ["postleitzahl", "plz"]),
        "Ort": _pick_value(payload, ["ort"]),
        "Bundesland": _pick_value(payload, ["bundesland", "bundesland_copy", "bundesland_name"]),
        "Land": _pick_value(payload, ["land_copy", "land"]),
        "Sozialversicherungsnummer": _pick_value(payload, ["sozialversicherungsnummer"]),
        "Personalausweisnummer": _pick_value(payload, ["personalausweisnummer"]),
        "Mobil": _pick_value(payload, ["mobil"]),
        "IBAN": _pick_value(payload, ["iban"]),
        "BIC": _pick_value(payload, ["bic"]),
        "Krankenkasse": _pick_value(payload, ["krankenkasse"]),
        "Kontoinhaber": _pick_value(payload, ["kontoinhaber"]),
        "Steuer Identifikationsnummer": _pick_value(payload, ["steuernummer"]),
    }


def _normalize_date_ddmmyyyy(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    match = re.match(r"^(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})$", raw)
    if match:
        year, month, day = match.groups()
        return f"{day.zfill(2)}.{month.zfill(2)}.{year}"
    match = re.match(r"^(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})$", raw)
    if match:
        day, month, year = match.groups()
        return f"{day.zfill(2)}.{month.zfill(2)}.{year}"
    return raw


def load_mitarbeiteranlage_record() -> dict:
    input_dir = os.environ.get("PERSO_INPUT_DIR", "perso-input")
    json_candidates = glob.glob(os.path.join(input_dir, "*.json"))
    if not json_candidates:
        raise Exception("[FEHLER] Keine JSON-Datei im Ordner 'perso-input' gefunden.")
    if len(json_candidates) > 1:
        raise Exception("[FEHLER] Mehr als eine JSON-Datei im Ordner 'perso-input' gefunden.")

    json_path = json_candidates[0]
    print(f"[INFO] Verwende JSON-Datei: {json_path}")
    with open(json_path, encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise Exception("[FEHLER] JSON-Datei muss ein Objekt mit Feldern sein.")
    row = _row_from_json(payload)
    if not any(row.values()):
        raise Exception("[FEHLER] JSON-Datei enthält keine verwertbaren Felder.")
    return row


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

    row = load_mitarbeiteranlage_record()

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
        "Bundesland": ["bundesland", "bundesland_id", "bundeslandId", "bundesland_select"],
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
                        formatted = _normalize_date_ddmmyyyy(value)
                        el.evaluate(
                            """(node, val) => {
                                node.value = val;
                                node.dispatchEvent(new Event('input', { bubbles: true }));
                                node.dispatchEvent(new Event('change', { bubbles: true }));
                                node.dispatchEvent(new Event('blur', { bubbles: true }));
                            }""",
                            formatted,
                        )
                        print(f"[OK] {html_name} (format dd.mm.yyyy) → {formatted}")
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

    anrede = str(row.get("Anrede", "")).strip().lower()
    geschlecht_value = ""
    if "weiblich" in anrede or "frau" in anrede:
        geschlecht_value = "W"
    elif "maennlich" in anrede or "männlich" in anrede or "herr" in anrede or "mann" in anrede:
        geschlecht_value = "M"
    if geschlecht_value:
        try:
            gender_locator = form_frame.locator("[name='geschlecht'], [id='geschlecht']").first
            if gender_locator.count() > 0:
                tag = gender_locator.evaluate("el => el.tagName.toLowerCase()")
                if tag == "select":
                    try:
                        gender_locator.select_option(value=geschlecht_value)
                    except Exception:
                        gender_locator.select_option(label=geschlecht_value)
                    print(f"[OK] geschlecht → {geschlecht_value}")
                else:
                    gender_locator.fill(geschlecht_value)
                    print(f"[OK] geschlecht (input) → {geschlecht_value}")
        except Exception as e:
            print(f"[FEHLER] geschlecht: {e}")

    try:
        bank_field = form_frame.locator("[name='bank'], [id='bank']").first
        if bank_field.count() > 0:
            bank_field.fill("")
            print("[OK] bank → (leer)")
    except Exception as e:
        print(f"[FEHLER] bank leeren: {e}")

    print("[INFO] Versuche auf 'Hinzufügen' zu klicken …")
    add_clicked = False
    def _click_add_button() -> bool:
        for selector in [
            "button:has-text('Hinzufügen')",
            "button:has-text('hinzufügen')",
            "button#iban",
        ]:
            locator = form_frame.locator(selector)
            if locator.count() > 0:
                try:
                    locator.first.scroll_into_view_if_needed()
                    locator.first.click()
                    print(f"[OK] Hinzufügen geklickt ({selector})")
                    return True
                except Exception as e:
                    print(f"[WARNUNG] Klick fehlgeschlagen ({selector}): {e}")
                    continue
        return False

    for selector in [
        "button:has-text('Hinzufügen')",
        "button:has-text('hinzufügen')",
        "button#iban",
    ]:
        locator = form_frame.locator(selector)
        if locator.count() > 0:
            try:
                locator.first.scroll_into_view_if_needed()
                locator.first.click()
                print(f"[OK] Hinzufügen geklickt ({selector})")
                add_clicked = True
                break
            except Exception as e:
                print(f"[WARNUNG] Klick fehlgeschlagen ({selector}): {e}")
                continue

    if not add_clicked:
        print("[HINWEIS] Kein Hinzufügen-Button gefunden.")

    def _confirm_hinweis_modal() -> bool:
        print("[INFO] Prüfe auf Hinweis-Modal und bestätige …")
        try:
            found_any = False
            # akzeptiere auch mehrere Hinweis-Dialoge hintereinander
            for _ in range(40):
                clicked = False
                for frame in page.frames:
                    confirm_button = frame.locator(
                        "div.ui-dialog button:has-text('fortfahren'), button:has-text('fortfahren')"
                    )
                    if confirm_button.count() > 0 and confirm_button.first.is_visible():
                        confirm_button.first.scroll_into_view_if_needed()
                        confirm_button.first.click(force=True)
                        print("[OK] Hinweis-Modal bestätigt (fortfahren).")
                        found_any = True
                        clicked = True
                        break
                if clicked:
                    time.sleep(0.25)
                    continue
                time.sleep(0.25)
            if not found_any:
                print("[HINWEIS] Hinweis-Modal/fortfahren nicht gefunden.")
            return found_any
        except Exception as e:
            print(f"[WARNUNG] Hinweis-Modal prüfen fehlgeschlagen: {e}")
            return False

    _confirm_hinweis_modal()

    print("[INFO] Prüfe auf Fehler-Modal (SVNR) …")
    try:
        svnr_error_found = False
        for _ in range(40):
            for frame in page.frames:
                dialog = frame.locator("div.ui-dialog:has-text('Fehler')").first
                if dialog.count() == 0 or not dialog.is_visible():
                    continue
                text = dialog.inner_text().lower()
                if "sozialversicherungsnummer" in text and "geburtsdatum" in text:
                    print("[FEHLER] SVNR passt nicht zu Geburtsdatum/Geschlecht. Lösche SVNR und versuche erneut.")
                    svnr_error_found = True
                    ok_button = dialog.locator("button:has-text('Ok'), button:has-text('OK')").first
                    if ok_button.count() > 0 and ok_button.is_visible():
                        ok_button.click(force=True)
                    break
            if svnr_error_found:
                break
            time.sleep(0.25)

        if svnr_error_found:
            try:
                form_frame.locator("[name='sozialversicherungsnummer'], [id='sozialversicherungsnummer']").first.fill("")
                print("[OK] sozialversicherungsnummer → (leer)")
            except Exception as e:
                print(f"[FEHLER] sozialversicherungsnummer leeren: {e}")
            add_clicked = _click_add_button()
            if not add_clicked:
                print("[HINWEIS] Kein Hinzufügen-Button gefunden (Retry nach SVNR-Fehler).")
            else:
                _confirm_hinweis_modal()
    except Exception as e:
        print(f"[WARNUNG] Fehler-Modal prüfen fehlgeschlagen: {e}")

    print("[INFO] Warte 45 Sekunden für manuelle Auslese …")
    time.sleep(45)

    print("[INFO] Prüfe auf Logindaten-Modal und wähle E-Mail …")
    try:
        found = False
        for _ in range(40):
            for frame in page.frames:
                dialog = frame.locator(
                    "div.ui-dialog:has-text('Logindaten'), "
                    "div.ui-dialog:has-text('Wollen Sie die Logindaten'), "
                    "div.ui-dialog:has-text('Wollen Sie die Logindaten drucken'), "
                    "div.ui-dialog:has-text('E-Mail wurde versendet')"
                ).first
                if dialog.count() == 0 or not dialog.is_visible():
                    continue
                email_button = dialog.locator("button:has-text('E-Mail')").first
                if email_button.count() > 0 and email_button.is_visible():
                    email_button.scroll_into_view_if_needed()
                    email_button.click(force=True)
                    print("[OK] Logindaten-Modal bestätigt (E-Mail).")
                    found = True
                    break
            if found:
                break
            time.sleep(0.25)
        if not found:
            print("[HINWEIS] Logindaten-Modal/E-Mail nicht gefunden.")
    except Exception as e:
        print(f"[WARNUNG] Logindaten-Modal prüfen fehlgeschlagen: {e}")

    print("[INFO] Warte auf E-Mail-Erfolgsmeldung und schließe Modal …")
    try:
        closed = False
        for _ in range(40):
            for frame in page.frames:
                dialog = frame.locator(
                    "div.ui-dialog:has-text('E-Mail wurde versendet')"
                ).first
                if dialog.count() == 0 or not dialog.is_visible():
                    continue
                close_button = dialog.locator("button:has-text('Schließen')").first
                if close_button.count() > 0 and close_button.is_visible():
                    close_button.scroll_into_view_if_needed()
                    close_button.click(force=True)
                    print("[OK] E-Mail-Erfolgsmeldung geschlossen.")
                    closed = True
                    break
            if closed:
                break
            time.sleep(0.25)
        if not closed:
            print("[TIMEOUT] Keine Erfolgsmeldung gefunden oder geschlossen (E-Mail wurde versendet).")
    except Exception as e:
        print(f"[WARNUNG] Erfolgsmeldung prüfen fehlgeschlagen: {e}")
        print("[TIMEOUT] Erfolgsmeldung-Prüfung abgebrochen.")

    print("[FERTIG] Formularbefüllung abgeschlossen.")
    print("[INFO] Fenster bleibt 10 Sekunden offen …")
    time.sleep(10)
    print("[INFO] Browser wird geschlossen.")
