import json
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
    input_dir = Path("perso-input")
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
    return payload


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


def _set_select_value(locator, value: str) -> bool:
    if locator.count() == 0:
        return False
    try:
        locator.first.select_option(value=value)
        return True
    except Exception:
        return False


def _select_autocomplete_by_bn(target: Union[Frame, Page], input_locator, bn: str, fallback_text: str) -> bool:
    if input_locator.count() == 0 or not bn:
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
            return True
        time.sleep(0.2)

    if fallback_text:
        _set_input_value(input_locator, fallback_text)
    return False


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

    if variant == "kb":
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
        "krankenkasse_bn": "98000006" if variant == "kb" else krankenkasse_bn,
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
    )
    if values["tatsaechliche_krankenkasse"]:
        tatsaechliche_input = panel.locator("#tatsaechliche_krankenkasse")
        _select_autocomplete_by_bn(
            target,
            tatsaechliche_input,
            values["tatsaechliche_bn"],
            values["tatsaechliche_krankenkasse"],
        )
    _set_select_value(panel.locator("#personengruppe"), values["personengruppe"])
    _set_input_value(panel.locator("#taetigkeitsbezeichnung"), values["taetigkeitsbezeichnung"])
    _set_select_value(panel.locator("#vertragsform_taetigkeitschluessel"), values["vertragsform"])
    _set_select_value(panel.locator("#arbeitnehmerueberlassung_taetigkeitschluessel"), "2")
    _set_select_value(panel.locator("#steuerklasse"), values["steuerklasse"])

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
            print(f"[INFO] Pause für manuelle Schritte ({wait_seconds}s) …")
            deadline = time.time() + max(1, wait_seconds)
            while time.time() < deadline:
                _click_fertig_in_dialog(target_page, timeout_seconds=0.5)
                time.sleep(0.5)
        else:
            print("[INFO] Kein Treffer geklickt – keine Pause.")

        browser.close()
