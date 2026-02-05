import json
import os
import re
import time
from pathlib import Path
from typing import Union
from urllib.parse import urljoin

from playwright.sync_api import Frame, Page, TimeoutError, sync_playwright

from src import config
from src.login import do_login


def _extract_bn(value: str) -> str:
    if not value:
        return ""
    trimmed = value.strip()
    if re.fullmatch(r"[A-Za-z0-9_]+", trimmed) and len(trimmed) >= 5:
        return trimmed
    match = re.search(r"\[Bn:\s*([^\]]+)\]", value)
    if match:
        return match.group(1).strip()
    match = re.search(r"\b(BN|Bn)\s*[:\-]?\s*([A-Za-z0-9_]+)\b", value)
    if match:
        return match.group(2).strip()
    return ""


def _wait_for_inhalt_frame(page: Page, timeout_seconds: int = 5) -> Frame | None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        frame = page.frame(name="inhalt")
        if frame:
            return frame
        time.sleep(0.2)
    return None


def _load_personalbogen_json() -> dict:
    input_dir = Path(os.environ.get("PERSO_INPUT_DIR", "perso-input"))
    candidates = list(input_dir.glob("*.json"))
    if not candidates:
        raise FileNotFoundError("[FEHLER] Keine JSON-Datei in 'perso-input' gefunden.")
    if len(candidates) > 1:
        raise FileNotFoundError("[FEHLER] Mehr als eine JSON-Datei in 'perso-input' gefunden.")
    json_path = candidates[0]
    print(f"[INFO] Verwende JSON-Datei: {json_path}")
    with json_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("[FEHLER] JSON-Datei muss ein Objekt sein.")
    if isinstance(payload.get("fragebogen"), dict):
        normalized = dict(payload["fragebogen"])
        if isinstance(payload.get("vertrag"), dict):
            normalized["vertrag"] = payload["vertrag"]
        return normalized
    return payload


def _pick_payload_value(payload: dict, keys: list[str]) -> str:
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


def _open_user_overview(page: Page) -> Union[Frame, Page]:
    target_url = urljoin(config.BASE_URL, "user.php")
    print(f"[INFO] Öffne Benutzerübersicht: {target_url}")
    page.goto(target_url, wait_until="domcontentloaded", timeout=30000)

    frame = _wait_for_inhalt_frame(page)
    target: Union[Frame, Page] = frame if frame else page
    if frame:
        frame.goto(target_url, wait_until="domcontentloaded", timeout=30000)
        target = frame

    target.wait_for_selector(
        ".scn_datatable_outer_table_user_tbl, div.dataTables_filter, #user_tbl_filter",
        timeout=20000,
    )
    target.wait_for_selector(
        ".scn_datatable_outer_table_user_tbl input[type='search'], input[aria-controls='user_tbl'], input[type='search']",
        timeout=20000,
    )
    target.wait_for_selector(
        ".scn_datatable_outer_table_user_tbl table#user_tbl tbody tr, table#user_tbl tbody tr",
        timeout=20000,
    )
    # ensure filter is set to "Alle"
    try:
        filter_all = target.locator("#filter_anzeige_0").first
        if filter_all.count() > 0 and not filter_all.is_checked():
            filter_all.click()
            print("[OK] Filter auf 'Alle' gesetzt.")
            # allow table to refresh
            time.sleep(0.8)
    except Exception:
        pass
    return target


def _locate_search_input(target: Union[Frame, Page]):
    selectors = [
        ".scn_datatable_outer_table_user_tbl div.dataTables_filter input[type='search']",
        ".scn_datatable_outer_table_user_tbl input[aria-controls='user_tbl']",
        ".scn_datatable_outer_table_user_tbl input[type='search']",
        "div.dataTables_filter input[type='search']",
        "#user_tbl_filter input[type='search']",
        "input[aria-controls='user_tbl']",
        "input[type='search']",
    ]
    for sel in selectors:
        locator = target.locator(sel).first
        if locator.count() > 0:
            return locator
    return target.locator("input").first


def _click_lastname_link(target: Union[Frame, Page], email: str) -> Page | None:
    rows = target.locator("table#user_tbl tbody tr")
    parent_page = target.page if isinstance(target, Frame) else target

    email_link = target.locator(f"a[href^='mailto:'][href*='{email}']")
    email_rows = rows.filter(has=email_link)
    row = email_rows.first if email_rows.count() > 0 else rows.first

    if row.count() == 0:
        print("[WARNUNG] Keine Zeilen in user_tbl gefunden.")
        return None

    link = row.locator("a.ma_akte_link_text, a.ma_akte_link_img").first
    if link.count() == 0:
        link = row.locator("a").first
    if link.count() == 0:
        print("[WARNUNG] Kein klickbarer Link in der Trefferzeile gefunden.")
        return None

    href = link.get_attribute("href") or ""
    if href:
        print("[AKTION] Öffne Mitarbeiterakte per Direktlink …")
        try:
            parent_page.goto(urljoin(config.BASE_URL, href), wait_until="domcontentloaded", timeout=20000)
            return parent_page
        except Exception as exc:
            print(f"[WARNUNG] Direktlink fehlgeschlagen: {exc}")

    print("[AKTION] Klicke Nachname in Trefferzeile …")

    try:
        with parent_page.context.expect_page(timeout=3000) as new_page_event:
            link.click()
        new_page = new_page_event.value
        new_page.wait_for_load_state("domcontentloaded", timeout=15000)
        return new_page
    except TimeoutError:
        pass

    link.click()
    deadline = time.time() + 12
    while time.time() < deadline:
        if "mitarbeiter_akte.php" in parent_page.url:
            return parent_page
        for frame in parent_page.frames:
            if "mitarbeiter_akte.php" in (frame.url or ""):
                return parent_page
            try:
                if frame.locator("#administration_user_stammdaten_tabs").count() > 0:
                    return parent_page
            except Exception:
                continue
        time.sleep(0.2)
    print(f"[DEBUG] Aktuelle URL (Page): {parent_page.url}")
    for idx, frame in enumerate(parent_page.frames):
        try:
            tabs = frame.locator("#administration_user_stammdaten_tabs").count()
        except Exception:
            tabs = -1
        print(f"[DEBUG] Frame {idx}: name={frame.name!r} url={frame.url!r} tabs={tabs}")
    if href:
        try:
            parent_page.goto(urljoin(config.BASE_URL, href), wait_until="domcontentloaded", timeout=15000)
        except Exception:
            pass
    return parent_page


def _open_lohnabrechnung_and_edit(page: Page) -> bool:
    try:
        target: Union[Frame, Page] = page
        frame = page.frame(name="inhalt")
        if frame:
            target = frame

        tab = target.locator("li[aria-controls='administration_user_stammdaten_tabs_lohnabrechnung'] a")
        if tab.count() == 0:
            tab = target.locator("a:has-text('Lohnabrechnung')")
        if tab.count() == 0:
            print("[WARNUNG] Tab 'Lohnabrechnung' nicht gefunden.")
            return False
        tab.first.scroll_into_view_if_needed()
        tab.first.click()

        try:
            target.wait_for_selector(
                "#administration_user_stammdaten_tabs_lohnabrechnung",
                timeout=8000,
            )
        except Exception:
            pass

        panel = target.locator("#administration_user_stammdaten_tabs_lohnabrechnung")
        edit_icon = panel.locator("img[src*='b_edit.png'][onclick*='makeEdited']").first
        if edit_icon.count() == 0:
            edit_icon = panel.locator("img[title='Bearbeiten']").first
        if edit_icon.count() == 0:
            edit_icon = target.locator("img[src*='b_edit.png'][onclick*='makeEdited']").first
        if edit_icon.count() == 0:
            edit_icon = target.locator("img[title='Bearbeiten']").first
        if edit_icon.count() == 0:
            print("[WARNUNG] Edit-Stift nicht gefunden.")
            return False
        try:
            edit_icon.scroll_into_view_if_needed()
        except Exception:
            pass
        edit_icon.click(force=True)
        print("[OK] Lohnabrechnung geöffnet und Edit-Stift geklickt.")
        return True
    except Exception as exc:
        print(f"[WARNUNG] Lohnabrechnung/Edit fehlgeschlagen: {exc}")
        return False


def _open_sedcard(page: Page) -> bool:
    deadline = time.time() + 10
    last_frames: list[Frame] = []
    while time.time() < deadline:
        candidates: list[Union[Frame, Page]] = [page]
        inhalt = page.frame(name="inhalt")
        if inhalt:
            candidates.append(inhalt)
        last_frames = page.frames
        candidates.extend(last_frames)

        for target in candidates:
            link = target.locator("#tableOfSubmenue a:has-text('Sedcard')").first
            if link.count() == 0:
                link = target.locator("a:has-text('Sedcard')").first
            if link.count() == 0:
                continue
            try:
                link.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                link.click()
                print("[OK] Submenü 'Sedcard' geklickt.")
                time.sleep(0.5)
                return True
            except Exception as exc:
                print(f"[WARNUNG] Submenü 'Sedcard' Klick fehlgeschlagen: {exc}")
                return False
        time.sleep(0.25)

    print("[WARNUNG] Submenü-Link 'Sedcard' nicht gefunden.")
    for idx, frame in enumerate(last_frames):
        try:
            count = frame.locator("a:has-text('Sedcard')").count()
        except Exception:
            count = -1
        print(f"[DEBUG] Frame {idx}: name={frame.name!r} url={frame.url!r} sedcard_links={count}")
    return False


def _open_vertragsdaten(page: Page) -> bool:
    deadline = time.time() + 10
    last_frames: list[Frame] = []
    while time.time() < deadline:
        candidates: list[Union[Frame, Page]] = [page]
        inhalt = page.frame(name="inhalt")
        if inhalt:
            candidates.append(inhalt)
        last_frames = page.frames
        candidates.extend(last_frames)

        for target in candidates:
            link = target.locator("#tableOfSubmenue a:has-text('Vertragsdaten')").first
            if link.count() == 0:
                link = target.locator("a:has-text('Vertragsdaten')").first
            if link.count() == 0:
                continue
            try:
                link.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                link.click()
                print("[OK] Submenü 'Vertragsdaten' geklickt.")
                time.sleep(0.5)
                return True
            except Exception as exc:
                print(f"[WARNUNG] Submenü 'Vertragsdaten' Klick fehlgeschlagen: {exc}")
                return False
        time.sleep(0.25)

    print("[WARNUNG] Submenü-Link 'Vertragsdaten' nicht gefunden.")
    for idx, frame in enumerate(last_frames):
        try:
            count = frame.locator("a:has-text('Vertragsdaten')").count()
        except Exception:
            count = -1
        print(f"[DEBUG] Frame {idx}: name={frame.name!r} url={frame.url!r} vertragsdaten_links={count}")
    return False


def _open_mitarbeiterinformationen(page: Page) -> bool:
    deadline = time.time() + 10
    last_frames: list[Frame] = []
    while time.time() < deadline:
        candidates: list[Union[Frame, Page]] = [page]
        inhalt = page.frame(name="inhalt")
        if inhalt:
            candidates.append(inhalt)
        last_frames = page.frames
        candidates.extend(last_frames)

        for target in candidates:
            link = target.locator("#tableOfSubmenue a:has-text('Mitarbeiterinformationen')").first
            if link.count() == 0:
                link = target.locator("a:has-text('Mitarbeiterinformationen')").first
            if link.count() == 0:
                continue
            try:
                link.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                _dismiss_ui_overlay(page)
                link.click()
                print("[OK] Submenü 'Mitarbeiterinformationen' geklickt.")
                time.sleep(0.5)
                return True
            except Exception as exc:
                print(f"[WARNUNG] Submenü 'Mitarbeiterinformationen' Klick fehlgeschlagen: {exc}")
                return False
        time.sleep(0.25)

    print("[WARNUNG] Submenü-Link 'Mitarbeiterinformationen' nicht gefunden.")
    for idx, frame in enumerate(last_frames):
        try:
            count = frame.locator("a:has-text('Mitarbeiterinformationen')").count()
        except Exception:
            count = -1
        print(f"[DEBUG] Frame {idx}: name={frame.name!r} url={frame.url!r} mitarbeiterinformationen_links={count}")
    return False


def _enter_sedcard_edit_mode(page: Page) -> bool:
    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    edit_icon = target.locator("img.edit[onclick*='makeEdited'], img[title='Bearbeiten']").first
    if edit_icon.count() == 0:
        print("[WARNUNG] Sedcard-Edit-Stift nicht gefunden.")
        return False
    try:
        edit_icon.scroll_into_view_if_needed()
    except Exception:
        pass
    try:
        edit_icon.click(force=True)
        print("[OK] Sedcard-Edit-Stift geklickt.")
    except Exception as exc:
        print(f"[WARNUNG] Sedcard-Edit-Stift Klick fehlgeschlagen: {exc}")
        return False

    probe = target.locator("#groesse, [name='groesse']").first
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            if probe.count() == 0:
                break
            disabled = probe.evaluate("el => el.disabled")
            if not disabled:
                break
        except Exception:
            pass
        time.sleep(0.2)
    return True


def _set_yes_no_select(locator, value: str) -> bool:
    if locator.count() == 0:
        return False
    normalized = str(value).strip().lower()
    if normalized in ["ja", "yes", "true", "1", "wahr"]:
        val = "1"
    elif normalized in ["nein", "no", "false", "0", "falsch"]:
        val = "0"
    else:
        return False
    try:
        locator.first.select_option(value=val)
        return True
    except Exception:
        return False


def _fill_sedcard_fields(page: Page, payload: dict) -> None:
    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    if not _enter_sedcard_edit_mode(page):
        return

    input_mappings = {
        "groesse": _pick_payload_value(payload, ["koerpergroesse"]),
        "konfektion": _pick_payload_value(payload, ["konfektionsgroesse"]),
        "schuhgroesse": _pick_payload_value(payload, ["schuhgroesse"]),
        "schulausbildung": _pick_payload_value(payload, ["schulausbildung"]),
        "fuehrerscheinart": _pick_payload_value(payload, ["fuehrerscheinklasse"]),
    }

    for field, value in input_mappings.items():
        if not value:
            continue
        locator = target.locator(f"[name='{field}'], #{field}")
        if _set_input_value(locator, value):
            print(f"[OK] sedcard {field} → {value}")
        else:
            print(f"[WARNUNG] sedcard {field} nicht gesetzt.")

    language_entries = _parse_language_entries(_pick_payload_value(payload, ["fremdsprachen"]))
    if language_entries:
        _fill_language_fields(target, language_entries)

    fuehrerschein_value = _pick_payload_value(payload, ["fuehrerschein"])
    if fuehrerschein_value:
        locator = target.locator("[name='fuehrerschein']")
        if _set_yes_no_select(locator, fuehrerschein_value):
            print(f"[OK] sedcard fuehrerschein → {fuehrerschein_value}")
        else:
            print("[WARNUNG] sedcard fuehrerschein nicht gesetzt.")

    pkw_value = _pick_payload_value(payload, ["pkw"])
    if pkw_value:
        locator = target.locator("[name='pkw']")
        if _set_yes_no_select(locator, pkw_value):
            print(f"[OK] sedcard pkw → {pkw_value}")
        else:
            print("[WARNUNG] sedcard pkw nicht gesetzt.")

    save_button = target.locator("button:has-text('Daten speichern')").first
    if save_button.count() > 0:
        try:
            save_button.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            save_button.click()
            print("[OK] Sedcard gespeichert (Daten speichern).")
        except Exception as exc:
            print(f"[WARNUNG] Sedcard speichern fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] Sedcard-Speichern-Button nicht gefunden.")


def _fill_grundlohn_history(page: Page) -> None:
    entries = [
        ("01.01.2026", "14,96"),
        ("01.09.2026", "15,33"),
        ("01.04.2027", "15,87"),
    ]

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    edit_icon = target.locator(
        "img.edit[onclick*=\"daten_historie\"][onclick*=\"'lohn'\"], "
        "img.edit[onclick*='daten_historie'][onclick*='lohn']"
    ).first
    if edit_icon.count() == 0:
        print("[WARNUNG] Grundlohn-Edit-Icon nicht gefunden.")
        return
    try:
        edit_icon.scroll_into_view_if_needed()
    except Exception:
        pass
    edit_icon.click(force=True)
    print("[OK] Grundlohn-Historie geöffnet.")

    dialog = page.locator("div.ui-dialog:has-text('Grundlohn-Historie')").first
    try:
        dialog.wait_for(state="visible", timeout=8000)
    except Exception:
        print("[WARNUNG] Grundlohn-Historie-Dialog nicht sichtbar.")
        return

    try:
        dialog_text = dialog.inner_text()
    except Exception:
        dialog_text = ""
    if all(date in dialog_text and amount in dialog_text for date, amount in entries):
        print("[INFO] Grundlohn-Historie bereits vorhanden – schließe Dialog.")
        close_button = dialog.locator("button:has-text('schließen'), button:has-text('Schließen')").first
        if close_button.count() > 0:
            try:
                close_button.click()
                print("[OK] Grundlohn-Dialog geschlossen.")
            except Exception as exc:
                print(f"[WARNUNG] Grundlohn-Dialog schließen fehlgeschlagen: {exc}")
        else:
            print("[WARNUNG] 'schließen' Button im Grundlohn-Dialog nicht gefunden.")
        return

    for date_value, amount_value in entries:
        value_input = dialog.locator("#daten_eintragen_wert").first
        date_input = dialog.locator("#daten_eintragen_gueltig_ab").first
        if value_input.count() == 0 or date_input.count() == 0:
            print("[WARNUNG] Eingabefelder im Grundlohn-Dialog nicht gefunden.")
            return
        value_input.fill(amount_value)
        date_input.fill(date_value)
        submit_button = dialog.locator("button:has-text('eintragen')").first
        if submit_button.count() == 0:
            print("[WARNUNG] 'eintragen'-Button im Grundlohn-Dialog nicht gefunden.")
            return
        submit_button.click()
        print(f"[OK] Grundlohn eingetragen → {date_value} = {amount_value}")
        time.sleep(0.5)

    close_button = dialog.locator("button:has-text('schließen'), button:has-text('Schließen')").first
    if close_button.count() > 0:
        try:
            close_button.click()
            print("[OK] Grundlohn-Dialog geschlossen.")
        except Exception as exc:
            print(f"[WARNUNG] Grundlohn-Dialog schließen fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] 'schließen' Button im Grundlohn-Dialog nicht gefunden.")


def _fill_vertrag_history(page: Page, payload: dict) -> None:
    vertrag = payload.get("vertrag") or {}
    if not isinstance(vertrag, dict):
        vertrag = {}
    contract_type = str(vertrag.get("contract_type", "")).strip().lower()
    hire_date = str(vertrag.get("hire_date", "")).strip()
    if not contract_type or not hire_date:
        print("[HINWEIS] Vertrag/Eintrittsdatum fehlt – überspringe Vertragshistorie.")
        return

    type_map = {
        "kb": "kurzf. Beschäftigte",
        "tz": "Teilzeit 80h",
        "gb": "GB - Minijob",
    }
    label = type_map.get(contract_type)
    if not label:
        print(f"[WARNUNG] Unbekannter contract_type: {contract_type!r}")
        return

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    edit_icon = target.locator(
        "img.edit[onclick*=\"daten_historie\"][onclick*=\"'vertrag_id'\"], "
        "img.edit[onclick*='daten_historie'][onclick*='vertrag_id']"
    ).first
    if edit_icon.count() == 0:
        print("[WARNUNG] Vertrag-Edit-Icon nicht gefunden.")
        return
    try:
        edit_icon.scroll_into_view_if_needed()
    except Exception:
        pass
    edit_icon.click(force=True)
    print("[OK] Vertragshistorie geöffnet.")

    dialog = page.locator("div.ui-dialog:has-text('Vertragshistorie')").first
    try:
        dialog.wait_for(state="visible", timeout=8000)
    except Exception:
        print("[WARNUNG] Vertragshistorie-Dialog nicht sichtbar.")
        return

    try:
        dialog_text = dialog.inner_text()
    except Exception:
        dialog_text = ""
    hire_date_ui = _format_date_for_ui(hire_date)
    hire_date_modal = _first_of_month(hire_date_ui)
    if label in dialog_text and hire_date_modal in dialog_text:
        print("[INFO] Vertragshistorie bereits vorhanden – schließe Dialog.")
        close_button = dialog.locator("button:has-text('schließen'), button:has-text('Schließen')").first
        if close_button.count() > 0:
            try:
                close_button.click()
                print("[OK] Vertrag-Dialog geschlossen.")
            except Exception as exc:
                print(f"[WARNUNG] Vertrag-Dialog schließen fehlgeschlagen: {exc}")
        else:
            print("[WARNUNG] 'schließen' Button im Vertrag-Dialog nicht gefunden.")
        return

    select = dialog.locator("#daten_eintragen_wert").first
    date_input = dialog.locator("#daten_eintragen_gueltig_ab").first
    if select.count() == 0 or date_input.count() == 0:
        print("[WARNUNG] Eingabefelder im Vertrag-Dialog nicht gefunden.")
        return
    select.select_option(label=label)
    date_input.fill(hire_date_modal)
    submit_button = dialog.locator("button:has-text('eintragen')").first
    if submit_button.count() == 0:
        print("[WARNUNG] 'eintragen'-Button im Vertrag-Dialog nicht gefunden.")
        return
    submit_button.click()
    print(f"[OK] Vertrag eingetragen → {label} ab {hire_date_modal}")
    time.sleep(0.5)

    close_button = dialog.locator("button:has-text('schließen'), button:has-text('Schließen')").first
    if close_button.count() > 0:
        try:
            close_button.click()
            print("[OK] Vertrag-Dialog geschlossen.")
        except Exception as exc:
            print(f"[WARNUNG] Vertrag-Dialog schließen fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] 'schließen' Button im Vertrag-Dialog nicht gefunden.")


def _fill_tage_fremd(page: Page, payload: dict) -> None:
    tage = _pick_payload_value(payload, ["tage_gearbeitet"])
    if not tage:
        print("[HINWEIS] Keine tage_gearbeitet im JSON – überspringe Tage Fremdfirmen.")
        return

    vertrag = payload.get("vertrag") or {}
    if not isinstance(vertrag, dict):
        vertrag = {}
    hire_date = str(vertrag.get("hire_date", "")).strip()
    if not hire_date:
        print("[HINWEIS] Kein hire_date im JSON – überspringe Tage Fremdfirmen.")
        return

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    edit_icon = target.locator(
        "img.edit[onclick*=\"daten_historie\"][onclick*=\"'tage_fremd'\"], "
        "img.edit[onclick*='daten_historie'][onclick*='tage_fremd']"
    ).first
    if edit_icon.count() == 0:
        print("[WARNUNG] Tage Fremdfirmen-Edit-Icon nicht gefunden.")
        return
    try:
        edit_icon.scroll_into_view_if_needed()
    except Exception:
        pass
    edit_icon.click(force=True)
    print("[OK] Tage Fremdfirmen-Historie geöffnet.")

    dialog = page.locator("div.ui-dialog").filter(has_text="gültig ab").first
    try:
        dialog.wait_for(state="visible", timeout=8000)
    except Exception:
        print("[WARNUNG] Tage Fremdfirmen-Dialog nicht sichtbar.")
        return

    try:
        dialog_text = dialog.inner_text()
    except Exception:
        dialog_text = ""
    if tage in dialog_text and hire_date in dialog_text:
        print("[INFO] Tage Fremdfirmen bereits vorhanden – schließe Dialog.")
        close_button = dialog.locator("button:has-text('schließen'), button:has-text('Schließen')").first
        if close_button.count() > 0:
            try:
                close_button.click()
                print("[OK] Tage Fremdfirmen-Dialog geschlossen.")
            except Exception as exc:
                print(f"[WARNUNG] Tage Fremdfirmen-Dialog schließen fehlgeschlagen: {exc}")
        else:
            print("[WARNUNG] 'schließen' Button im Tage Fremdfirmen-Dialog nicht gefunden.")
        return

    value_input = dialog.locator("#daten_eintragen_wert").first
    date_input = dialog.locator("#daten_eintragen_gueltig_ab").first
    if value_input.count() == 0 or date_input.count() == 0:
        print("[WARNUNG] Eingabefelder im Tage Fremdfirmen-Dialog nicht gefunden.")
        return
    value_input.fill(tage)
    date_input.fill(hire_date)
    submit_button = dialog.locator("button:has-text('eintragen')").first
    if submit_button.count() == 0:
        print("[WARNUNG] 'eintragen'-Button im Tage Fremdfirmen-Dialog nicht gefunden.")
        return
    submit_button.click()
    print(f"[OK] Tage Fremdfirmen eingetragen → {tage} ab {hire_date}")
    time.sleep(0.5)

    close_button = dialog.locator("button:has-text('schließen'), button:has-text('Schließen')").first
    if close_button.count() > 0:
        try:
            close_button.click()
            print("[OK] Tage Fremdfirmen-Dialog geschlossen.")
        except Exception as exc:
            print(f"[WARNUNG] Tage Fremdfirmen-Dialog schließen fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] 'schließen' Button im Tage Fremdfirmen-Dialog nicht gefunden.")


def _fill_sonstiges(page: Page, payload: dict) -> None:
    value = _pick_payload_value(payload, ["aufmerksam_geworden_durch"])
    if not value:
        print("[HINWEIS] Kein aufmerksam_geworden_durch im JSON – überspringe Sonstiges.")
        return

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    edit_icon = target.locator(
        "img.edit[onclick*=\"feld_aendern\"][onclick*=\"'sonstiges'\"], "
        "img.edit[onclick*='feld_aendern'][onclick*='sonstiges']"
    ).first
    if edit_icon.count() == 0:
        print("[WARNUNG] Sonstiges-Edit-Icon nicht gefunden.")
        return
    try:
        edit_icon.scroll_into_view_if_needed()
    except Exception:
        pass
    edit_icon.click(force=True)
    print("[OK] Sonstiges-Dialog geöffnet.")

    dialog = page.locator("div.ui-dialog").first
    try:
        dialog.wait_for(state="visible", timeout=8000)
    except Exception:
        print("[WARNUNG] Sonstiges-Dialog nicht sichtbar.")
        return

    input_field = dialog.locator("input[type='text'], textarea").first
    if input_field.count() == 0:
        print("[WARNUNG] Sonstiges-Eingabefeld nicht gefunden.")
        return
    input_field.fill(value)

    save_button = dialog.locator(
        "button:has-text('speichern'), button:has-text('Speichern'), "
        "button:has-text('OK'), button:has-text('Ok'), button:has-text('Übernehmen')"
    ).first
    if save_button.count() > 0:
        try:
            save_button.click()
            print(f"[OK] Sonstiges gesetzt → {value}")
        except Exception as exc:
            print(f"[WARNUNG] Sonstiges speichern fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] Sonstiges-Speichern-Button nicht gefunden.")


def _fill_eintritt_austritt(page: Page, payload: dict) -> None:
    vertrag = payload.get("vertrag") or {}
    if not isinstance(vertrag, dict):
        vertrag = {}
    hire_date = str(vertrag.get("hire_date", "")).strip()
    befristung_bis = str(vertrag.get("befristung_bis", "")).strip()
    contract_type = str(vertrag.get("contract_type", "")).strip()
    if not hire_date:
        print("[HINWEIS] Kein hire_date im JSON – überspringe Ein-/Austritt.")
        return

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    edit_icon = target.locator(
        "img.edit[onclick*='eintritt_austritt_editor'], "
        "img.edit[onclick*='eintritt_austritt']"
    ).first
    if edit_icon.count() == 0:
        print("[WARNUNG] Eintritt/Austritt-Edit-Icon nicht gefunden.")
        return
    try:
        edit_icon.scroll_into_view_if_needed()
    except Exception:
        pass
    edit_icon.click(force=True)
    print("[OK] Ein-/Austrittsdatum-Dialog geöffnet.")

    dialog = page.locator("div.ui-dialog:has-text('Ein-/Austrittsdatum ändern')").first
    try:
        dialog.wait_for(state="visible", timeout=8000)
    except Exception:
        print("[WARNUNG] Ein-/Austrittsdatum-Dialog nicht sichtbar.")
        return

    try:
        dialog_text = dialog.inner_text()
    except Exception:
        dialog_text = ""
    expected_end = befristung_bis if befristung_bis else "unbefristet"
    remark = contract_type.upper()
    if hire_date in dialog_text and expected_end in dialog_text and (contract_type in dialog_text or remark in dialog_text):
        print("[INFO] Ein-/Austritt bereits vorhanden – schließe Dialog.")
        close_button = dialog.locator("button:has-text('Schließen'), button:has-text('schließen')").first
        if close_button.count() > 0:
            try:
                close_button.click()
                print("[OK] Ein-/Austrittsdatum-Dialog geschlossen.")
            except Exception as exc:
                print(f"[WARNUNG] Ein-/Austrittsdatum-Dialog schließen fehlgeschlagen: {exc}")
        return

    eintritt_input = dialog.locator("#eintrittsdatum_neu").first
    austritt_input = dialog.locator("#austrittsdatum_neu").first
    bemerkung_input = dialog.locator("#bemerkung").first
    if eintritt_input.count() == 0 or austritt_input.count() == 0 or bemerkung_input.count() == 0:
        print("[WARNUNG] Ein-/Austrittsdatum-Felder nicht gefunden.")
        return
    eintritt_input.fill(hire_date)
    austritt_input.fill(befristung_bis)
    remark = contract_type.upper()
    bemerkung_input.fill(remark)

    save_button = dialog.locator("button:has-text('Speichern')").first
    if save_button.count() == 0:
        print("[WARNUNG] Ein-/Austrittsdatum-Speichern-Button nicht gefunden.")
        return
    save_button.click()
    print(f"[OK] Ein-/Austritt gesetzt → {hire_date} bis {befristung_bis or 'unbefristet'} ({remark})")
    time.sleep(0.5)

    warn_dialog = page.locator("div.ui-dialog:has-text('Warnung')").first
    try:
        warn_dialog.wait_for(state="visible", timeout=4000)
        fortfahren = warn_dialog.locator("button:has-text('Fortfahren')").first
        if fortfahren.count() > 0:
            fortfahren.click()
            print("[OK] Warnung bestätigt (Fortfahren).")
    except Exception:
        pass

    close_button = dialog.locator("button:has-text('Schließen'), button:has-text('schließen')").first
    if close_button.count() > 0:
        try:
            close_button.click()
            print("[OK] Ein-/Austrittsdatum-Dialog geschlossen.")
        except Exception as exc:
            print(f"[WARNUNG] Ein-/Austrittsdatum-Dialog schließen fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] 'Schließen' Button im Ein-/Austrittsdatum-Dialog nicht gefunden.")


def _find_angebot_file() -> str:
    input_dir = Path(os.environ.get("PERSO_INPUT_DIR", "perso-input"))
    pdfs = list(input_dir.glob("*.pdf"))
    if not pdfs:
        return ""
    # Poller speichert den Vertrag standardmäßig unter vertrag.pdf.
    for path in pdfs:
        if path.name.lower() == "vertrag.pdf":
            return str(path)
    # Prefer files containing "angebot" (case-insensitive).
    for path in pdfs:
        if "angebot" in path.name.lower():
            return str(path)
    return str(pdfs[0])


def _format_date_for_ui(date_str: str) -> str:
    if not date_str:
        return ""
    match = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", date_str)
    if match:
        year, month, day = match.groups()
        return f"{day}.{month}.{year}"
    if re.match(r"^\d{2}\.\d{2}\.\d{4}$", date_str):
        return date_str
    return date_str


def _first_of_month(date_str: str) -> str:
    ui = _format_date_for_ui(date_str)
    match = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", ui)
    if not match:
        return ui
    _day, month, year = match.groups()
    return f"01.{month}.{year}"


def _build_vertrag_bemerkung(payload: dict) -> str:
    vertrag = payload.get("vertrag") or {}
    if not isinstance(vertrag, dict):
        return ""
    contract_type = str(vertrag.get("contract_type", "")).strip().lower()
    hire_date = str(vertrag.get("hire_date", "")).strip()
    if not contract_type or not hire_date:
        return ""
    type_map = {"kb": "KB", "tz": "TZ", "gb": "GB"}
    type_label = type_map.get(contract_type, contract_type.upper())
    hire_date_ui = _format_date_for_ui(hire_date)
    return f"Arbeitsvertrag {type_label} zum {hire_date_ui}"


def _open_document_upload_dialog(page: Page):
    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    add_button = target.locator("button:has-text('Dokument hinzufügen')").first
    if add_button.count() == 0:
        print("[WARNUNG] 'Dokument hinzufügen' Button nicht gefunden.")
        return
    try:
        add_button.scroll_into_view_if_needed()
    except Exception:
        pass
    add_button.click()
    print("[OK] Dokument hinzufügen geöffnet.")

    dialog = page.locator("div.ui-dialog:has-text('Dokument hinzufügen')").first
    try:
        dialog.wait_for(state="visible", timeout=8000)
    except Exception:
        print("[WARNUNG] Dokument-Dialog nicht sichtbar.")
        return None
    return dialog


def _upload_document_with_modal(
    page: Page,
    file_path: str,
    folder_label: str,
    folder_value: str,
    bemerkung_text: str = "",
    gueltig_bis: str = "",
) -> bool:
    dialog = _open_document_upload_dialog(page)
    if dialog is None:
        return False

    # Dropzone creates a hidden file input on click; use file chooser fallback.
    try:
        dropzone = dialog.locator("#maDokDropzone").first
        if dropzone.count() == 0:
            raise RuntimeError("Dropzone not found")
        with page.expect_file_chooser(timeout=5000) as fc_info:
            dropzone.click()
        file_chooser = fc_info.value
        file_chooser.set_files(file_path)
        print(f"[OK] Datei ausgewählt → {Path(file_path).name}")
    except Exception:
        file_input = dialog.locator("input[type='file']").first
        if file_input.count() == 0:
            print("[WARNUNG] Datei-Input im Dokument-Dialog nicht gefunden.")
            return False
        file_input.set_input_files(file_path)
        print(f"[OK] Datei ausgewählt → {Path(file_path).name}")

    table_body = dialog.locator("#tableAuflistungDateien tbody").first
    try:
        table_body.wait_for(state="visible", timeout=8000)
    except Exception:
        pass

    row = table_body.locator("tr").first
    deadline = time.time() + 10
    while time.time() < deadline and row.count() == 0:
        time.sleep(0.2)
        row = table_body.locator("tr").first
    if row.count() == 0:
        print("[WARNUNG] Upload-Row nicht erschienen.")
        return False

    if bemerkung_text:
        bemerkung_input = row.locator(
            "textarea[name*='bemerkung'], textarea[id^='fileExtras_'], textarea"
        ).first
        if bemerkung_input.count() > 0:
            bemerkung_input.fill(bemerkung_text)
            print(f"[OK] Bemerkung gesetzt → {bemerkung_text}")
        else:
            print("[WARNUNG] Bemerkung-Feld im Upload-Row nicht gefunden.")

    if gueltig_bis:
        gueltig_input = row.locator(
            "input[name*='gueltig_bis'], input[id^='fileExtrasGueltigBis'], input.datepicker"
        ).first
        if gueltig_input.count() > 0:
            _set_input_value_force(gueltig_input, gueltig_bis)
            print(f"[OK] Gültig bis gesetzt → {gueltig_bis}")
        else:
            print("[WARNUNG] Gültig-bis-Feld im Upload-Row nicht gefunden.")

    folder_select = row.locator("select").first
    if folder_select.count() > 0:
        try:
            folder_select.select_option(label=folder_label)
        except Exception:
            try:
                folder_select.select_option(value=folder_value)
            except Exception:
                pass
        print(f"[OK] Ordner gesetzt → {folder_label}")
    else:
        print("[WARNUNG] Ordner-Auswahl nicht gefunden.")

    save_button = dialog.locator("button:has-text('Speichern')").first
    if save_button.count() == 0:
        print("[WARNUNG] Dokument-Speichern-Button nicht gefunden.")
        return False
    save_button.click()
    print("[OK] Dokument gespeichert.")
    return True


def _find_input_file_by_stem(stem: str) -> str:
    input_dir = Path(os.environ.get("PERSO_INPUT_DIR", "perso-input"))
    candidates = sorted(input_dir.glob(f"{stem}.*"))
    if not candidates:
        return ""
    return str(candidates[0])


def _upload_arbeitsvertrag(page: Page, payload: dict) -> None:
    pdf_path = _find_angebot_file()
    if not pdf_path:
        print("[HINWEIS] Kein Angebots-/Vertrags-PDF in perso-input gefunden – überspringe Dokument-Upload.")
        return
    _upload_document_with_modal(
        page=page,
        file_path=pdf_path,
        folder_label="- Arbeitsvertrag",
        folder_value="3",
        bemerkung_text=_build_vertrag_bemerkung(payload),
    )


def _upload_additional_documents(page: Page, payload: dict) -> None:
    uploads = payload.get("uploads") if isinstance(payload, dict) else {}
    if not isinstance(uploads, dict):
        uploads = {}

    valid_until_infektionsschutz = ""
    if isinstance(uploads.get("infektionsschutz"), dict):
        valid_until_infektionsschutz = _format_date_for_ui(str(uploads["infektionsschutz"].get("validUntil", "")).strip())

    jobs = [
        ("personalbogen", "Personalbogen", "- Personalbogen, Rentenbefreiung & Agenda", "5", ""),
        ("zusatzvereinbarung", "Zusatzvereinbarung", "Dokumente", "1", ""),
        ("sicherheitsbelehrung", "Sicherheitsbelehrung", "Dokumente", "1", ""),
        ("immatrikulation", "Immatrikulations-/Schulbescheinigung", "- Imma/Schul", "2", ""),
        (
            "infektionsschutz",
            f"Infektionsschutzbelehrung{f' vom {valid_until_infektionsschutz}' if valid_until_infektionsschutz else ''}",
            "- Infektionsschutzbelehrung",
            "9",
            valid_until_infektionsschutz,
        ),
    ]

    print("[INFO] Starte Upload zusätzlicher Dokumente …")
    for stem, bemerkung, folder_label, folder_value, gueltig_bis in jobs:
        file_path = _find_input_file_by_stem(stem)
        if not file_path:
            print(f"[HINWEIS] Zusatzdokument nicht gefunden: {stem}.* (in PERSO_INPUT_DIR)")
            continue
        print(f"[INFO] Lade zusätzliches Dokument hoch: {Path(file_path).name}")
        uploaded = _upload_document_with_modal(
            page=page,
            file_path=file_path,
            folder_label=folder_label,
            folder_value=folder_value,
            bemerkung_text=bemerkung,
            gueltig_bis=gueltig_bis,
        )
        if not uploaded:
            print(f"[WARNUNG] Upload fehlgeschlagen: {Path(file_path).name}")


def _set_input_value(locator, value: str) -> bool:
    if locator.count() == 0:
        return False
    locator.first.evaluate(
        """(node, val) => {
            node.value = val;
            node.dispatchEvent(new Event('input', { bubbles: true }));
            node.dispatchEvent(new Event('change', { bubbles: true }));
            node.dispatchEvent(new Event('blur', { bubbles: true }));
        }""",
        value,
    )
    return True


def _set_input_value_force(locator, value: str) -> bool:
    if locator.count() == 0:
        return False
    locator.first.evaluate(
        """(node, val) => {
            node.removeAttribute('readonly');
            node.removeAttribute('disabled');
            node.value = val;
            node.dispatchEvent(new Event('input', { bubbles: true }));
            node.dispatchEvent(new Event('change', { bubbles: true }));
            node.dispatchEvent(new Event('blur', { bubbles: true }));
        }""",
        value,
    )
    return True


def _set_select_value(locator, value: str) -> bool:
    if locator.count() == 0:
        return False
    try:
        locator.first.select_option(value=value)
        return True
    except Exception:
        return False


def _set_select_value_with_fallback(locator, value: str, label: str | None = None) -> bool:
    if locator.count() == 0:
        return False
    try:
        locator.first.evaluate("(node) => { node.removeAttribute('disabled'); }")
    except Exception:
        pass
    if value and _set_select_value(locator, value):
        return True
    if label:
        try:
            locator.first.select_option(label=label)
            return True
        except Exception:
            return False


def _get_select_value(locator) -> str:
    if locator.count() == 0:
        return ""
    try:
        return str(locator.first.evaluate("(node) => node.value") or "").strip()
    except Exception:
        return ""


def _force_set_select_value(locator, value: str) -> bool:
    if locator.count() == 0:
        return False
    try:
        locator.first.evaluate(
            """(node, val) => {
                node.removeAttribute('disabled');
                node.value = val;
                node.dispatchEvent(new Event('input', { bubbles: true }));
                node.dispatchEvent(new Event('change', { bubbles: true }));
                node.dispatchEvent(new Event('blur', { bubbles: true }));
            }""",
            value,
        )
        return True
    except Exception:
        return False


def _set_select_value_logged(locator, value: str, field_label: str) -> None:
    if locator.count() == 0:
        print(f"[WARNUNG] Feld nicht gefunden: {field_label}")
        return
    try:
        locator.first.evaluate("(node) => { node.removeAttribute('disabled'); }")
    except Exception:
        pass
    ok = _set_select_value(locator, value)
    actual = _get_select_value(locator)
    if ok and actual == value:
        print(f"[OK] {field_label} gesetzt → {value}")
        return
    if not ok or actual != value:
        forced = _force_set_select_value(locator, value)
        actual = _get_select_value(locator)
        if forced and actual == value:
            print(f"[OK] {field_label} per Fallback gesetzt → {value}")
            return
    print(f"[WARNUNG] {field_label} nicht gesetzt (soll={value}, ist={actual or '—'})")
    return False


def _parse_language_entries(value) -> list[dict]:
    if not value:
        return []
    if isinstance(value, list):
        parts = [str(item).strip() for item in value if str(item).strip()]
        raw = ", ".join(parts)
    else:
        raw = str(value).strip()
    if not raw:
        return []
    items = [part.strip() for part in re.split(r"[,\n;/]+", raw) if part.strip()]
    entries: list[dict] = []
    for item in items:
        match = re.match(r"^(.*?)\s*\((.*?)\)\s*$", item)
        if match:
            language = match.group(1).strip()
            level = match.group(2).strip()
        else:
            language = item.strip()
            level = ""
        if language:
            entries.append({"language": language, "level": level})
    return entries


def _fill_language_fields(target: Union[Frame, Page], entries: list[dict]) -> None:
    pairs = [
        ("sprache01a", "sprache01b"),
        ("sprache02a", "sprache02b"),
        ("sprache03a", "sprache03b"),
        ("sprache04a", "sprache04b"),
    ]
    for idx, (lang_field, level_field) in enumerate(pairs):
        if idx >= len(entries):
            break
        entry = entries[idx]
        language = entry.get("language", "")
        level = entry.get("level", "")
        if language:
            loc = target.locator(f"[name='{lang_field}'], #{lang_field}")
            if _set_input_value(loc, language):
                print(f"[OK] sedcard {lang_field} → {language}")
            else:
                print(f"[WARNUNG] sedcard {lang_field} nicht gesetzt.")
        if level:
            loc = target.locator(f"[name='{level_field}'], #{level_field}")
            if _set_input_value(loc, level):
                print(f"[OK] sedcard {level_field} → {level}")
            else:
                print(f"[WARNUNG] sedcard {level_field} nicht gesetzt.")
    if len(entries) > len(pairs):
        extras = ", ".join([e.get("language", "") for e in entries[len(pairs):] if e.get("language")])
        if extras:
            loc = target.locator("[name='sprache04'], #sprache04")
            if _set_input_value(loc, extras):
                print(f"[OK] sedcard sprache04 → {extras}")
            else:
                print("[WARNUNG] sedcard sprache04 nicht gesetzt.")


def _map_schulabschluss_to_value(value) -> str | None:
    if not value:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if "ohne" in normalized:
        return "1"
    if "haupt" in normalized or "volks" in normalized:
        return "2"
    if "mittlere" in normalized or "reife" in normalized or "realschule" in normalized or "gleichwertig" in normalized:
        return "3"
    if "abitur" in normalized:
        return "4"
    if "unbekannt" in normalized:
        return "9"
    return None


def _fill_stammdaten_fields(page: Page, payload: dict) -> None:
    schulabschluss_raw = _pick_payload_value(payload, ["schulabschluss"])
    if not schulabschluss_raw:
        print("[HINWEIS] Kein Schulabschluss im JSON – überspringe Stammdaten.")
        return

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    tab = target.locator("li[aria-controls='administration_user_stammdaten_tabs_stammdaten'] a").first
    if tab.count() == 0:
        tab = target.locator("a:has-text('Stammdaten')").first
    if tab.count() == 0:
        print("[WARNUNG] Tab 'Stammdaten' nicht gefunden.")
        return

    try:
        tab.scroll_into_view_if_needed()
    except Exception:
        pass
    tab.click()

    panel = target.locator("#administration_user_stammdaten_tabs_stammdaten").first
    try:
        panel.wait_for(state="visible", timeout=8000)
    except Exception:
        pass

    edit_icon = panel.locator("img[src*='b_edit.png'][onclick*='makeEdited'], img[title='Bearbeiten']").first
    if edit_icon.count() == 0:
        edit_icon = target.locator("img[src*='b_edit.png'][onclick*='makeEdited'], img[title='Bearbeiten']").first
    if edit_icon.count() > 0:
        try:
            edit_icon.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            edit_icon.click(force=True)
            print("[OK] Stammdaten Edit-Stift geklickt.")
        except Exception as exc:
            print(f"[WARNUNG] Stammdaten Edit-Stift nicht klickbar: {exc}")
    else:
        print("[WARNUNG] Stammdaten Edit-Stift nicht gefunden.")

    value = _map_schulabschluss_to_value(schulabschluss_raw)
    if value:
        loc = panel.locator("#schulabschluss_taetigkeitschluessel, [name='schulabschluss_taetigkeitschluessel']")
        label = None
        try:
            label = loc.locator(f"option[value='{value}']").first.inner_text()
        except Exception:
            label = None
        if _set_select_value_with_fallback(loc, value, label=label):
            print(f"[OK] Stammdaten schulabschluss → {schulabschluss_raw}")
        else:
            print("[WARNUNG] Stammdaten schulabschluss nicht gesetzt.")
    else:
        print(f"[WARNUNG] Schulabschluss nicht gemappt: {schulabschluss_raw}")

    save_button = panel.locator(
        "input[type='submit'].speichern, input[type='submit'][value*='Daten speichern'], button:has-text('Daten speichern')"
    ).first
    if save_button.count() > 0:
        try:
            save_button.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            save_button.click()
            print("[OK] Stammdaten gespeichert.")
        except Exception as exc:
            print(f"[WARNUNG] Stammdaten speichern fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] Stammdaten Speichern-Button nicht gefunden.")


def _dismiss_ui_overlay(page: Page) -> None:
    overlay = page.locator("div.ui-widget-overlay.ui-front").first
    try:
        if overlay.count() > 0 and overlay.is_visible():
            page.keyboard.press("Escape")
            time.sleep(0.2)
    except Exception:
        pass
    try:
        if overlay.count() > 0 and overlay.is_visible():
            close_button = page.locator(
                "div.ui-dialog:visible button:has-text('Schließen'), "
                "div.ui-dialog:visible button:has-text('Fertig'), "
                "div.ui-dialog:visible button.ui-dialog-titlebar-close"
            ).first
            if close_button.count() > 0:
                close_button.click()
                time.sleep(0.2)
    except Exception:
        pass


def _select_autocomplete_by_bn(
    target: Union[Frame, Page],
    input_locator,
    bn: str,
    fallback_text: str,
    field_label: str = "krankenkasse",
) -> bool:
    locator_count = input_locator.count()
    if locator_count == 0:
        print(f"[WARNUNG] {field_label}: Eingabefeld nicht gefunden – übersprungen.")
        return False
    if not bn:
        if fallback_text:
            print(f"[WARNUNG] {field_label}: BN fehlt, versuche Fallback-Text → {fallback_text}")
        else:
            print(f"[WARNUNG] {field_label}: BN fehlt und kein Fallback-Text – übersprungen.")
            return False
    try:
        input_locator.first.click()
    except Exception:
        pass
    input_locator.first.fill(bn)

    list_locator = target.locator("ul.ui-autocomplete li.ui-menu-item")
    deadline = time.time() + 6
    while time.time() < deadline:
        item = list_locator.filter(has_text=f"[Bn: {bn}]").first
        if item.count() > 0 and item.is_visible():
            item.click()
            print(f"[OK] {field_label}: Autocomplete Treffer → [Bn: {bn}]")
            return True
        time.sleep(0.2)

    if fallback_text:
        _set_input_value(input_locator, fallback_text)
        print(f"[WARNUNG] {field_label}: Kein Autocomplete Treffer für BN {bn} – Fallback gesetzt → {fallback_text}")
    return False


def _fill_notfallkontakt(page: Page, payload: dict) -> None:
    name = _pick_payload_value(payload, ["notfall_name", "notfallkontakt_name"])
    relation = _pick_payload_value(payload, ["verwandschaftsgrad", "notfallkontakt_relation"])
    phone = _pick_payload_value(payload, ["notfall_tel", "notfallkontakt_tel", "notfallkontakt_telefon"])
    nested = payload.get("notfallkontakt")
    if isinstance(nested, dict):
        name = name or _pick_payload_value(nested, ["name", "notfall_name", "notfallkontakt_name"])
        relation = relation or _pick_payload_value(nested, ["relation", "verwandschaftsgrad", "notfallkontakt_relation"])
        phone = phone or _pick_payload_value(nested, ["telefon", "phone", "notfall_tel", "notfallkontakt_tel"])
    print(f"[DEBUG] Notfallkontakt Werte: name='{name}' relation='{relation}' phone='{phone}'")
    if not any([name, relation, phone]):
        print("[HINWEIS] Kein Notfallkontakt im JSON – überspringe.")
        return
    print("[INFO] Öffne Notfallkontakt und trage Werte ein …")

    candidates: list[Union[Frame, Page]] = [page]
    frame = page.frame(name="inhalt")
    if frame:
        candidates.append(frame)
    candidates.extend(page.frames)

    target: Union[Frame, Page] | None = None
    tab = None
    tab_selectors = [
        "#administration_user_stammdaten_tabs a[href='#administration_user_stammdaten_tabs_notfallkontakt']",
        "li[aria-controls='administration_user_stammdaten_tabs_notfallkontakt'] a",
        "a:has-text('Notfallkontakt')",
    ]
    for candidate in candidates:
        for selector in tab_selectors:
            candidate_tab = candidate.locator(selector).first
            if candidate_tab.count() > 0:
                target = candidate
                tab = candidate_tab
                break
        if tab is not None:
            break

    if not tab or not target:
        print("[WARNUNG] Tab 'Notfallkontakt' nicht gefunden.")
        return
    try:
        tab.scroll_into_view_if_needed()
    except Exception:
        pass
    tab.click()

    panel = target.locator("#administration_user_stammdaten_tabs_notfallkontakt").first
    try:
        panel.wait_for(state="visible", timeout=8000)
    except Exception:
        print("[WARNUNG] Notfallkontakt-Panel nicht sichtbar (Timeout).")

    edit_icon = panel.locator("img[src*='b_edit.png'][onclick*='makeEdited'], img[title='Bearbeiten']").first
    if edit_icon.count() > 0:
        try:
            edit_icon.scroll_into_view_if_needed()
        except Exception:
            pass
        edit_icon.click(force=True)
        print("[OK] Notfallkontakt Edit-Stift geklickt.")
    else:
        print("[WARNUNG] Notfallkontakt Edit-Stift nicht gefunden.")

    if name:
        loc = panel.locator("#notfallkontakt_name, [name='notfallkontakt_name']")
        print(f"[DEBUG] notfallkontakt_name Locator count={loc.count()}")
        if _set_input_value_force(loc, name):
            print(f"[OK] notfallkontakt_name → {name}")
    if phone:
        loc = panel.locator("#notfallkontakt_telefon, [name='notfallkontakt_telefon']")
        print(f"[DEBUG] notfallkontakt_telefon Locator count={loc.count()}")
        if _set_input_value_force(loc, phone):
            print(f"[OK] notfallkontakt_telefon → {phone}")
    if relation:
        loc = panel.locator("#notfallkontakt_relation, [name='notfallkontakt_relation']")
        print(f"[DEBUG] notfallkontakt_relation Locator count={loc.count()}")
        if _set_input_value_force(loc, relation):
            print(f"[OK] notfallkontakt_relation → {relation}")

    save_button = panel.locator("input[type='submit'].speichern, input[type='submit'][value*='Daten speichern']").first
    print(f"[DEBUG] Notfallkontakt Speichern-Button count={save_button.count()}")
    if save_button.count() > 0:
        try:
            save_button.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            save_button.click()
            print("[OK] Notfallkontakt gespeichert.")
        except Exception as exc:
            print(f"[WARNUNG] Notfallkontakt speichern fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] Notfallkontakt Speichern-Button nicht gefunden.")


def _resolve_lohnabrechnung_values(payload: dict) -> dict:
    variant = str(payload.get("form_variant", "")).strip().lower()
    if variant == "geringfuegig":
        variant = "gb"
    krankenkasse_pf = str(payload.get("krankenkasse", "") or "").strip()
    krankenkasse_bn = (
        str(payload.get("krankenkasse_bn") or payload.get("krankenkasse_bn_nummer") or payload.get("krankenkasse_bn_nr") or "")
        .strip()
    )
    if not krankenkasse_bn:
        krankenkasse_bn = _extract_bn(krankenkasse_pf)

    vertrag = payload.get("vertrag") or {}
    if not isinstance(vertrag, dict):
        vertrag = {}
    contract_type = str(vertrag.get("contract_type", "")).strip().lower()

    if contract_type == "kb":
        krankenkasse = "Knappschaft Hauptverwaltung [Bn: 98000006]"
        tatsaechliche = krankenkasse_pf
        tatsaechliche_bn = krankenkasse_bn
        personengruppe = "110"
        vertragsform = "4"
        steuerklasse = "1"
    elif contract_type == "gb":
        krankenkasse = krankenkasse_pf
        tatsaechliche = ""
        tatsaechliche_bn = ""
        personengruppe = "109"
        vertragsform = "2"
        steuerklasse = "M"
    elif contract_type == "tz":
        krankenkasse = krankenkasse_pf
        tatsaechliche = ""
        tatsaechliche_bn = ""
        personengruppe = "101"
        vertragsform = "2"
        steuerklasse = "1"
    elif variant == "kb":
        krankenkasse = "Knappschaft Hauptverwaltung [Bn: 98000006]"
        tatsaechliche = krankenkasse_pf
        tatsaechliche_bn = krankenkasse_bn
        personengruppe = "110"
        vertragsform = "4"
        steuerklasse = "1"
    elif variant == "gb":
        krankenkasse = krankenkasse_pf
        tatsaechliche = ""
        tatsaechliche_bn = ""
        personengruppe = "109"
        vertragsform = "2"
        steuerklasse = "M"
    else:
        krankenkasse = krankenkasse_pf
        tatsaechliche = ""
        tatsaechliche_bn = ""
        personengruppe = "101"
        vertragsform = "2"
        steuerklasse = "1"

    return {
        "variant": variant,
        "krankenkasse": krankenkasse,
        "krankenkasse_bn": "98000006" if contract_type == "kb" else ("98000006" if variant == "kb" else krankenkasse_bn),
        "tatsaechliche_krankenkasse": tatsaechliche,
        "tatsaechliche_bn": tatsaechliche_bn,
        "personengruppe": personengruppe,
        "vertragsform": vertragsform,
        "steuerklasse": steuerklasse,
        "taetigkeitsbezeichnung": "63301",
    }


def _fill_lohnabrechnung_fields(page: Page, payload: dict) -> None:
    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    values = _resolve_lohnabrechnung_values(payload)
    panel = target.locator("#administration_user_stammdaten_tabs_lohnabrechnung")

    try:
        panel.wait_for(state="visible", timeout=8000)
    except Exception:
        pass

    krankenkasse_input = panel.locator("#krankenkasse")
    _select_autocomplete_by_bn(
        target,
        krankenkasse_input,
        values["krankenkasse_bn"],
        values["krankenkasse"],
        "krankenkasse",
    )
    if values["tatsaechliche_krankenkasse"]:
        tatsaechliche_input = panel.locator("#tatsaechliche_krankenkasse")
        _select_autocomplete_by_bn(
            target,
            tatsaechliche_input,
            values["tatsaechliche_bn"],
            values["tatsaechliche_krankenkasse"],
            "tatsaechliche_krankenkasse",
        )
    print(
        "[INFO] Lohnabrechnung Zielwerte: "
        f"personengruppe={values['personengruppe']}, "
        f"vertragsform={values['vertragsform']}, "
        f"steuerklasse={values['steuerklasse']}"
    )
    _set_select_value_logged(panel.locator("#personengruppe"), values["personengruppe"], "Personengruppe")
    _set_input_value(panel.locator("#taetigkeitsbezeichnung"), values["taetigkeitsbezeichnung"])
    _set_select_value_logged(panel.locator("#vertragsform_taetigkeitschluessel"), values["vertragsform"], "Vertragsform")
    _set_select_value_logged(
        panel.locator("#arbeitnehmerueberlassung_taetigkeitschluessel"),
        "2",
        "Arbeitnehmerüberlassung",
    )
    _set_select_value_logged(panel.locator("#steuerklasse"), values["steuerklasse"], "Steuerklasse")

    try:
        target.evaluate(
            "typeof taetigkeitsschluessel_generieren === 'function' && taetigkeitsschluessel_generieren()"
        )
    except Exception:
        pass


def _fill_vertragsdaten(page: Page) -> None:
    entries = [
        ("01.01.2026", "14,96"),
        ("01.09.2026", "15,33"),
        ("01.04.2027", "15,87"),
    ]

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    tab = target.locator("a:has-text('Vertragsdaten')").first
    if tab.count() == 0:
        print("[HINWEIS] Tab 'Vertragsdaten' nicht gefunden – überspringe Vertragsdaten.")
        return

    href = tab.get_attribute("href") or ""
    tab.click()

    panel = target
    if href.startswith("#"):
        panel = target.locator(href)
    try:
        panel.wait_for(state="visible", timeout=8000)
    except Exception:
        pass

    rows = panel.locator("tr")
    filled = 0
    for i in range(rows.count()):
        if filled >= len(entries):
            break
        row = rows.nth(i)
        date_input = row.locator(
            "input[type='text'].datepicker, input[type='text'][name*='datum'], input[type='text'][id*='datum'], "
            "input[type='text'][name*='von'], input[type='text'][id*='von']"
        )
        amount_input = row.locator(
            "input[type='text'][name*='lohn'], input[type='text'][id*='lohn'], input[type='text'][name*='betrag'], "
            "input[type='text'][id*='betrag'], input[type='text'][name*='stunden'], input[type='text'][id*='stunden']"
        )
        if date_input.count() == 0 or amount_input.count() == 0:
            continue
        date_value, amount_value = entries[filled]
        _set_input_value(date_input, date_value)
        _set_input_value(amount_input, amount_value)
        filled += 1

    if filled < len(entries):
        print("[HINWEIS] Vertragsdaten unvollständig gesetzt – bitte HTML/Selector prüfen.")


def _click_daten_speichern(page: Page, timeout_seconds: float = 6.0) -> bool:
    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    panel = target.locator("#administration_user_stammdaten_tabs_lohnabrechnung")
    if panel.count() > 0:
        try:
            panel.first.wait_for(state="visible", timeout=4000)
        except Exception:
            pass

    selectors = [
        "#administration_user_stammdaten_tabs_lohnabrechnung input.editWorker.button.speichern.showElement",
        "#administration_user_stammdaten_tabs_lohnabrechnung input[type='submit'][value='Daten speichern']",
        "input.editWorker.button.speichern.showElement",
        "input[type='submit'][value='Daten speichern']",
        "div[style*='padding-top:10px'] input[type='submit'][value='Daten speichern']",
        "form input[type='submit'][value='Daten speichern']",
    ]
    button = None
    for sel in selectors:
        locator = target.locator(sel).first
        if locator.count() > 0:
            button = locator
            break
    if button is None:
        try:
            clicked = target.evaluate(
                """() => {
                    const panel = document.querySelector('#administration_user_stammdaten_tabs_lohnabrechnung');
                    const btn = panel?.querySelector("input[type='submit'][value='Daten speichern']");
                    if (btn) { btn.click(); return true; }
                    const fallback = document.querySelector("input[type='submit'][value='Daten speichern']");
                    if (fallback) { fallback.click(); return true; }
                    return false;
                }"""
            )
        except Exception:
            clicked = False
        if clicked:
            print("[OK] 'Daten speichern' geklickt (JS fallback).")
            return True
        return False
    deadline = time.time() + max(1.0, timeout_seconds)
    while time.time() < deadline:
        try:
            button.wait_for(state="visible", timeout=800)
            button.scroll_into_view_if_needed()
            button.click()
            print("[OK] 'Daten speichern' geklickt.")
            return True
        except Exception:
            time.sleep(0.3)
    return False


def _click_fertig_in_dialog(page: Page, timeout_seconds: float = 3.0) -> bool:
    dialog = page.locator(
        "div.ui-dialog.ui-dialog-buttons:has(button:has-text('Fertig')), "
        "div.ui-dialog.ui-widget.ui-widget-content.ui-corner-all.ui-front.ui-dialog-buttons"
        ":has(button:has-text('Fertig'))"
    ).first
    try:
        dialog.wait_for(state="visible", timeout=int(timeout_seconds * 1000))
    except Exception:
        return False
    fertig_button = dialog.locator("button:has-text('Fertig')").first
    if fertig_button.count() == 0:
        return False
    try:
        fertig_button.click()
        print("[OK] Modal bestätigt: 'Fertig'.")
        return True
    except Exception:
        return False


def _wait_for_dialog_closed(page: Page, timeout_seconds: float = 6.0) -> None:
    dialog = page.locator("div.ui-dialog.ui-dialog-buttons").first
    try:
        dialog.wait_for(state="hidden", timeout=int(timeout_seconds * 1000))
    except Exception:
        pass


def run_mitarbeiter_vervollstaendigen(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    wait_seconds: int = 45,
):
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    state_path = Path(config.STATE_PATH)
    if not state_path.exists():
        raise RuntimeError(f"[FEHLER] Kein gespeicherter Login-State unter {state_path}. Bitte zuerst 'login' ausführen.")

    payload = _load_personalbogen_json()
    email = str(payload.get("email", "")).strip()
    if not email:
        raise RuntimeError("[FEHLER] Keine E-Mail im personalbogen-JSON gefunden.")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        context = browser.new_context(storage_state=str(state_path))
        page = context.new_page()

        print("[INFO] Lade Startseite mit gespeicherter Session …")
        page.goto(config.BASE_URL, wait_until="domcontentloaded")

        try:
            target = _open_user_overview(page)
        except Exception as exc:
            print(f"[WARNUNG] Übersicht nicht geladen (Session evtl. abgelaufen): {exc} – versuche Login …")
            page = browser.new_page()
            do_login(page)
            target = _open_user_overview(page)

        search_input = _locate_search_input(target)
        if search_input.count() == 0:
            raise RuntimeError("[FEHLER] Suchfeld in user.php nicht gefunden.")

        search_input.fill(email)
        time.sleep(0.2)
        print(f"[INFO] Suche nach E-Mail: {email}")

        target_page = _click_lastname_link(target, email)
        if target_page:
            if _open_lohnabrechnung_and_edit(target_page):
                _fill_lohnabrechnung_fields(target_page, payload)
                if _click_fertig_in_dialog(target_page, timeout_seconds=5.0):
                    _wait_for_dialog_closed(target_page, timeout_seconds=6.0)
                if not _click_daten_speichern(target_page, timeout_seconds=8.0):
                    print("[WARNUNG] 'Daten speichern' nicht gefunden/geklickt.")
            _fill_stammdaten_fields(target_page, payload)
            _fill_notfallkontakt(target_page, payload)
            if _open_sedcard(target_page):
                print("[INFO] Sedcard geöffnet.")
                _fill_sedcard_fields(target_page, payload)
            if _open_vertragsdaten(target_page):
                print("[INFO] Vertragsdaten geöffnet.")
                _fill_grundlohn_history(target_page)
                _fill_vertrag_history(target_page, payload)
                _fill_tage_fremd(target_page, payload)
                _fill_sonstiges(target_page, payload)
                _fill_eintritt_austritt(target_page, payload)
            if _open_mitarbeiterinformationen(target_page):
                print("[INFO] Mitarbeiterinformationen geöffnet.")
                _upload_arbeitsvertrag(target_page, payload)
                _upload_additional_documents(target_page, payload)
            print(f"[INFO] Pause für manuelle Schritte ({wait_seconds}s) …")
            deadline = time.time() + max(1, wait_seconds)
            while time.time() < deadline:
                _click_fertig_in_dialog(target_page, timeout_seconds=0.5)
                time.sleep(0.5)
        else:
            print("[INFO] Kein Treffer geklickt – keine Pause.")

        browser.close()
