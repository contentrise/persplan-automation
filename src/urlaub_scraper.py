import argparse
import calendar
import re
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from src import config


def get_last_day_of_month(year, month):
    last_day = calendar.monthrange(year, month)[1]
    return f"{last_day:02d}.{month:02d}.{year}"

def get_first_day_of_month(year, month):
    return f"01.{month:02d}.{year}"


def get_first_day_of_next_month(year, month):
    if month == 12:
        return f"01.01.{year+1}"
    else:
        return f"01.{month+1:02d}.{year}"


def month_name_de(month):
    monate = [
        "Januar", "Februar", "März", "April", "Mai", "Juni",
        "Juli", "August", "September", "Oktober", "November", "Dezember"
    ]
    return monate[month - 1]


def log_korrektur(name, aktion):
    log_path = Path(config.EXPORT_DIR) / "urlaub_korrekturen.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {name} → {aktion}\n")


def log_warnung(name, eintritt, austritt):
    warn_log = Path(config.EXPORT_DIR) / "urlaub_warnungen.log"
    warn_log.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(warn_log, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {name} → Eintritt {eintritt}, Austritt {austritt} (Eintrittsänderung übersprungen)\n")


def close_blocking_dialogs(page):
    """Schließt sichtbare jQuery-Dialoge, die Klicks auf die Tabelle blockieren."""
    dialogs = page.locator("div.ui-dialog:visible")
    count = dialogs.count()
    if count == 0:
        return

    for i in range(count):
        dialog = dialogs.nth(i)
        # Urlaub-Formular bewusst offen lassen.
        if dialog.locator("#form_edit_krank_urlaub").count() > 0:
            continue

        close_btn = dialog.locator(
            ".ui-dialog-titlebar-close, button:has-text('Schließen'), "
            "button:has-text('Abbrechen'), button:has-text('OK'), "
            "span.ui-button-text:has-text('Fortfahren')"
        ).first
        if close_btn.count() > 0:
            try:
                close_btn.click(force=True, timeout=1500)
            except Exception:
                pass

    time.sleep(0.2)


def clear_blocking_overlays(page):
    try:
        page.wait_for_selector(".ui-widget-overlay", state="detached", timeout=1500)
        return
    except PlaywrightTimeoutError:
        pass

    try:
        page.wait_for_selector(".ui-widget-overlay", state="hidden", timeout=800)
        return
    except PlaywrightTimeoutError:
        pass

    try:
        page.evaluate(
            "() => document.querySelectorAll('.ui-widget-overlay').forEach(e => e.remove())"
        )
    except Exception:
        pass


def log_step(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[STEP {ts}] {msg}")


def read_dialog_message(page, dialog, timeout_ms: int = 4000, poll_ms: int = 200) -> str:
    """Liest Statusmeldungen nach dem Speichern, ohne lange Pausen zu erzeugen."""
    deadline = time.monotonic() + (timeout_ms / 1000.0)
    while time.monotonic() < deadline:
        try:
            msg_out = page.locator("#msg_out p")
            if msg_out.count() > 0:
                msg = msg_out.inner_text(timeout=min(800, timeout_ms)).strip()
                if msg:
                    return msg
        except Exception:
            pass

        try:
            if dialog.count() > 0:
                msg = dialog.locator(".ui-dialog-content").first.inner_text(
                    timeout=min(800, timeout_ms)
                ).strip()
                if msg:
                    return msg
        except Exception:
            pass

        time.sleep(poll_ms / 1000.0)

    return ""


def handle_anfragen_delete_dialog(page, name: str, context: str) -> int | None:
    """Bestätigt 'Anfragen löschen'-Dialoge und loggt die Anzahl gelöschter Anfragen."""
    try:
        dialogs = page.locator("div.ui-dialog:visible")
        count = dialogs.count()
    except Exception:
        return None

    for i in range(count):
        dialog = dialogs.nth(i)
        if dialog.locator("button:has-text('Anfragen löschen')").count() == 0:
            continue

        try:
            text = dialog.locator(".ui-dialog-content").inner_text(timeout=1000).strip()
        except Exception:
            text = ""

        anfragen = None
        m = re.search(r"hat der Mitarbeiter\\s+(\\d+)\\s+Anfragen", text, re.IGNORECASE)
        if m:
            try:
                anfragen = int(m.group(1))
            except ValueError:
                anfragen = None

        try:
            dialog.locator("button:has-text('Anfragen löschen')").first.click()
            log_step(f"Dialog 'Anfragen löschen' bestätigt ({context})")
        except Exception:
            pass

        if anfragen is not None:
            log_korrektur(name, f"{context}: {anfragen} Anfragen gelöscht")
            print(f"[INFO] {name}: {anfragen} Anfragen gelöscht ({context})")
        else:
            log_korrektur(name, f"{context}: Anfragen gelöscht (Anzahl unbekannt)")
            print(f"[INFO] {name}: Anfragen gelöscht ({context})")

        try:
            dialog.wait_for(state="detached", timeout=5000)
        except PlaywrightTimeoutError:
            pass

        return anfragen

    return None


def ensure_dialog_closed(page, dialog, reason):
    try:
        dialog.wait_for(state="detached", timeout=3000)
        log_step(f"Dialog geschlossen ({reason})")
        return
    except PlaywrightTimeoutError:
        log_step(f"Dialog noch offen – schließe ({reason})")

    try:
        dialog.locator(
            ".ui-dialog-titlebar-close, button:has-text('Schließen'), "
            "button:has-text('Abbrechen'), button:has-text('OK')"
        ).first.click(force=True, timeout=2000)
    except Exception:
        pass

    close_blocking_dialogs(page)
    clear_blocking_overlays(page)

    try:
        dialog.wait_for(state="detached", timeout=3000)
        log_step(f"Dialog geschlossen nach Force-Close ({reason})")
    except PlaywrightTimeoutError:
        log_step(f"Dialog weiterhin offen ({reason})")


def click_urlaub_cell(page, td, onclick):
    close_blocking_dialogs(page)
    clear_blocking_overlays(page)
    try:
        td.click(timeout=5000)
        return
    except PlaywrightTimeoutError:
        print("[WARNUNG] Klick blockiert – schließe Dialog und versuche erneut …")
        close_blocking_dialogs(page)
        clear_blocking_overlays(page)
        try:
            td.click(force=True, timeout=5000)
            return
        except PlaywrightError:
            pass

    # Letzter Fallback: onclick direkt ausführen.
    page.evaluate("code => window.eval(code)", onclick)


def run_urlaub_scraper(headless=None, slowmo_ms=None):
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms
    print("[INFO] Starte Urlaubsplanung-Scraper …")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        context = browser.new_context(storage_state=config.STATE_PATH)
        page = context.new_page()

        print("[INFO] Öffne Urlaubsplanung-Seite …")
        page.goto(config.BASE_URL + "urlaubsplanung.php", wait_until="domcontentloaded")
        time.sleep(2)

        print(f"[INFO] Setze Monat={config.URLAUB_MONTH}, Jahr={config.URLAUB_YEAR}")
        try:
            page.select_option("#zeitraum_monat", str(config.URLAUB_MONTH))
            page.select_option("#zeitraum_jahr", str(config.URLAUB_YEAR))
            page.select_option("#filter_vertragstypen", ["8", "9", "4", "2", "10", "3"])
            page.select_option("#filter_ma_gruppe", ["-1"])
            page.click("button.pointer:has(span:has-text('Anzeigen'))", timeout=8000)
        except Exception as e:
            print(f"[WARNUNG] Filter konnten nicht vollständig gesetzt werden: {e}")

        page.wait_for_selector("table#tbl_urlaubsplanung", timeout=15000)
        time.sleep(2)

        rows = page.locator("tr[id^='tbl_urlaubsplanung_row_']")
        total = rows.count()
        print(f"[INFO] Gefundene Mitarbeiterzeilen: {total}")

        processed = 0
        skipped = 0

        for i in range(total):
            start_ts = time.monotonic()
            row = rows.nth(i)
            try:
                name = row.locator("td").first.inner_text().strip()
            except Exception:
                continue

            tds = row.locator("td").all()
            td_htmls = [td.inner_html() for td in tds]
            if any("U<br>" in html or "MA hat Urlaub" in html for html in td_htmls):
                skipped += 1
                continue
            if any("VA" in html or "MA ist eingeplant" in html for html in td_htmls):
                skipped += 1
                continue

            print(f"\n✅ {name} hat keinen Einsatz und keinen Urlaub – Kandidat gefunden!")
            log_step("Starte Bearbeitung Mitarbeiter")

            # Vorab: Eintrittsdatum prüfen. Falls Eintritt im Urlaubsmonat liegt,
            # Eintritt auf den ersten Tag des Folgemonats setzen und KEINEN Urlaub eintragen.
            try:
                akte_link = row.locator("a[href*='mitarbeiter_akte.php']").first
                with page.context.expect_page() as akte_event:
                    akte_link.click()
                akte_page = akte_event.value
                akte_page.wait_for_load_state("domcontentloaded")

                print("[INFO] Öffne Tab 'Vertragsdaten' …")
                akte_page.locator("a:has-text('Vertragsdaten')").first.click()
                akte_page.wait_for_load_state("domcontentloaded")
                time.sleep(0.6)
                print("[OK] Vertragsdaten-Tab geöffnet.")

                eintritt_text = akte_page.locator("#eintrittsdatum").inner_text().strip()
                eintritt_date = datetime.strptime(eintritt_text, "%d.%m.%Y")

                if eintritt_date.month == config.URLAUB_MONTH and eintritt_date.year == config.URLAUB_YEAR:
                    print("[INFO] Eintritt liegt im Urlaubsmonat – setze neuen Eintritt und überspringe Urlaub …")
                    print("[AKTION] Suche korrektes Edit-Icon …")
                    all_icons = akte_page.locator("img.edit.sprite_16x16.pointer")
                    count = all_icons.count()
                    print(f"[DEBUG] Gefundene Edit-Icons: {count}")

                    target_icon = None
                    for j in range(count):
                        el = all_icons.nth(j)
                        code = el.get_attribute("onclick") or ""
                        if "openUiWindowReloaded" in code and "eintritt" in code:
                            target_icon = el
                            break

                    if not target_icon:
                        raise RuntimeError("Kein Edit-Icon für Eintritt gefunden")

                    onclick_code = target_icon.get_attribute("onclick")
                    akte_page.evaluate(f"window.eval(`{onclick_code}`)")

                    akte_page.wait_for_selector("div.ui-dialog form#formEditEinAustrittsdatum", timeout=20000)
                    print("[OK] Modal erkannt – ändere Eintrittsdaten …")

                    neues_datum = get_first_day_of_next_month(config.URLAUB_YEAR, config.URLAUB_MONTH)
                    bemerkung = f"keine Schicht im {month_name_de(config.URLAUB_MONTH)}"
                    akte_page.fill("#eintrittsdatum_neu", neues_datum)
                    akte_page.fill("#bemerkung", bemerkung)
                    akte_page.click("span.abstand_links_8:has-text('Speichern')")
                    time.sleep(0.8)

                    for sec in range(10):
                        if akte_page.locator("span.ui-button-text", has_text="Fortfahren").count() > 0:
                            akte_page.locator("span.ui-button-text", has_text="Fortfahren").click()
                            print(f"[OK] 'Fortfahren' bestätigt ({sec}s).")
                            break
                        time.sleep(1)

                    handle_anfragen_delete_dialog(akte_page, name, "Eintrittsänderung (vor Urlaub)")

                    close_btn = akte_page.locator("button:has-text('Schließen')").first
                    if close_btn.count() > 0:
                        close_btn.click()
                        try:
                            akte_page.locator("form#formEditEinAustrittsdatum").wait_for(
                                state="detached",
                                timeout=8000,
                            )
                        except PlaywrightTimeoutError:
                            pass

                    log_korrektur(name, f"Eintritt auf {neues_datum} geändert ({bemerkung})")
                    akte_page.close()
                    page.bring_to_front()
                    processed += 1
                    elapsed = time.monotonic() - start_ts
                    log_step(f"Fertig in {elapsed:.1f}s")
                    print(f"[OK] Eintritt angepasst – Urlaub übersprungen für {name}")
                    continue

                akte_page.close()
                page.bring_to_front()
            except Exception as e:
                print(f"[WARNUNG] Eintrittsprüfung fehlgeschlagen ({name}): {e}")

            target_td = None
            for td in tds:
                onclick = td.get_attribute("onclick") or ""
                if "xajax_krank_urlaub_edit" in onclick:
                    target_td = td
                    print(f"[AKTION] Klicke auf Urlaubseintrag-Feld ({onclick})")
                    click_urlaub_cell(page, td, onclick)
                    break
            if not target_td:
                print("⚠ Keine klickbare Zelle gefunden – überspringe.")
                continue

            try:
                page.wait_for_selector("#form_edit_krank_urlaub", timeout=10000)
                print(f"[INFO] Modal geöffnet für {name} …")

                dialog = page.locator("div.ui-dialog:visible:has(#form_edit_krank_urlaub)").last
                form = dialog.locator("#form_edit_krank_urlaub")
                bezahlt_box = form.locator("#bezahlt")
                if bezahlt_box.is_checked():
                    bezahlt_box.uncheck()
                    print("   → 'bezahlt' deaktiviert")

                form.locator("#tageskennzeichen").select_option("UU")
                print("   → Fehlzeitenkennzeichnung = 'UU'")

                first_day_str = get_first_day_of_month(config.URLAUB_YEAR, config.URLAUB_MONTH)
                von_field = form.locator("#von")
                page.evaluate(
                    "(args) => args.el.value = args.value",
                    {"el": von_field.element_handle(), "value": first_day_str}
                )
                print(f"   → 'von' = {first_day_str}")

                last_day_str = get_last_day_of_month(config.URLAUB_YEAR, config.URLAUB_MONTH)
                bis_field = form.locator("#bis")
                page.evaluate(
                    "(args) => args.el.value = args.value",
                    {"el": bis_field.element_handle(), "value": last_day_str}
                )
                print(f"   → 'bis' = {last_day_str}")

                log_step("Speichere Urlaubseintrag (erstes Modal)")
                dialog.locator("button:has-text('Speichern')").click()
                time.sleep(1.5)

                msg = read_dialog_message(page, dialog)
                msg_l = msg.lower()
                not_entered = "nicht eingetreten" in msg_l

                if "abgeglichene schichten" in msg_l:
                    print("[WARNUNG] Abgeglichene Schichten – setze 'von' auf den 02. des Monats (im selben Modal) und speichere erneut …")
                    second_day_str = f"02.{config.URLAUB_MONTH:02d}.{config.URLAUB_YEAR}"
                    von_field = form.locator("#von")
                    page.evaluate(
                        "(args) => args.el.value = args.value",
                        {"el": von_field.element_handle(), "value": second_day_str}
                    )
                    log_korrektur(name, f"Abgeglichene Schichten: von={second_day_str} gesetzt")
                    log_step("Speichere erneut nach Abgleich-Korrektur")
                    dialog.locator("button:has-text('Speichern')").click()
                    time.sleep(1.5)
                    msg_retry = read_dialog_message(page, dialog)
                    msg_retry_l = msg_retry.lower()
                    if "nicht eingetreten" in msg_retry_l:
                        log_step("Hinweis nach Retry: Mitarbeiter nicht eingetreten")
                        not_entered = True

                ensure_dialog_closed(page, dialog, "nach erstem Speichern")

                if not_entered:
                    print("[WARNUNG] Mitarbeiter nicht eingetreten – starte Eintritts-Korrektur …")
                    ensure_dialog_closed(page, dialog, "vor Eintritts-Korrektur")
                    akte_link = row.locator("a[href*='mitarbeiter_akte.php']").first
                    with page.context.expect_page() as new_tab_event:
                        akte_link.click()
                    akte_page = new_tab_event.value
                    akte_page.wait_for_load_state("domcontentloaded")

                    print("[INFO] Öffne Tab 'Vertragsdaten' …")
                    akte_page.locator("a:has-text('Vertragsdaten')").first.click()
                    akte_page.wait_for_load_state("domcontentloaded")
                    time.sleep(1)
                    print("[OK] Vertragsdaten-Tab geöffnet.")

                    eintritt_text = akte_page.locator("#eintrittsdatum").inner_text().strip()
                    eintritt_date = datetime.strptime(eintritt_text, "%d.%m.%Y")

                    austritt_text = ""
                    for sel in ["#austritt", "#austrittsdatum", "div[id^='austritt']"]:
                        if akte_page.locator(sel).count() > 0:
                            austritt_text = akte_page.locator(sel).inner_text().strip()
                            if austritt_text and "." in austritt_text:
                                break

                    if austritt_text:
                        print(f"[INFO] Austrittsdatum erkannt: {austritt_text}")
                    else:
                        print("[INFO] Kein Austrittsdatum gefunden (vermutlich unbefristet).")

                    # Eintritt prüfen
                    if eintritt_date.month == config.URLAUB_MONTH and eintritt_date.year == config.URLAUB_YEAR:
                        # Neue Logik – erst prüfen, ob Austritt im selben Monat liegt
                        if austritt_text:
                            try:
                                austritt_date = datetime.strptime(austritt_text, "%d.%m.%Y")
                                last_day = calendar.monthrange(config.URLAUB_YEAR, config.URLAUB_MONTH)[1]
                                if austritt_date <= datetime(config.URLAUB_YEAR, config.URLAUB_MONTH, last_day):
                                    log_warnung(name, eintritt_text, austritt_text)
                                    print(f"[WARNUNG] {name}: Eintrittsänderung übersprungen – Austritt {austritt_text} im selben/älteren Monat.")
                                    print("[INFO] Setze Urlaub bis Austrittsdatum statt Eintritt zu ändern …")
                                    akte_page.close()
                                    page.bring_to_front()

                                    for td in row.locator("td").all():
                                        onclick = td.get_attribute("onclick") or ""
                                        if "xajax_krank_urlaub_edit" in onclick:
                                            click_urlaub_cell(page, td, onclick)
                                            break

                                    page.wait_for_selector("#form_edit_krank_urlaub", timeout=10000)
                                    print(f"[INFO] Urlaub-Modal erneut geöffnet für {name} …")

                                    dialog = page.locator("div.ui-dialog:visible:has(#form_edit_krank_urlaub)").last
                                    form = dialog.locator("#form_edit_krank_urlaub")
                                    bezahlt_box = form.locator("#bezahlt")
                                    if bezahlt_box.is_checked():
                                        bezahlt_box.uncheck()

                                    form.locator("#tageskennzeichen").select_option("UU")
                                    first_day_str = get_first_day_of_month(config.URLAUB_YEAR, config.URLAUB_MONTH)
                                    von_field = form.locator("#von")
                                    page.evaluate(
                                        "(args) => args.el.value = args.value",
                                        {"el": von_field.element_handle(), "value": first_day_str}
                                    )
                                    bis_field = form.locator("#bis")
                                    page.evaluate(
                                        "(args) => args.el.value = args.value",
                                        {"el": bis_field.element_handle(), "value": austritt_text}
                                    )
                                    log_step("Speichere Urlaubseintrag (Austritt-Korrektur)")
                                    dialog.locator("button:has-text('Speichern')").click()
                                    time.sleep(1.5)
                                    msg2 = read_dialog_message(page, dialog)
                                    msg2_l = msg2.lower()
                                    if "abgeglichene schichten" in msg2_l:
                                        print("[WARNUNG] Abgeglichene Schichten – setze 'von' auf den 02. des Monats (im selben Modal) und speichere erneut …")
                                        second_day_str = f"02.{config.URLAUB_MONTH:02d}.{config.URLAUB_YEAR}"
                                        von_field = form.locator("#von")
                                        page.evaluate(
                                            "(args) => args.el.value = args.value",
                                            {"el": von_field.element_handle(), "value": second_day_str}
                                        )
                                        log_korrektur(name, f"Abgeglichene Schichten: von={second_day_str} gesetzt (Austritt bis {austritt_text})")
                                        log_step("Speichere erneut nach Abgleich-Korrektur (Austritt)")
                                        dialog.locator("button:has-text('Speichern')").click()
                                        time.sleep(1.5)
                                    ensure_dialog_closed(page, dialog, "nach Austritt-Korrektur")
                                    log_korrektur(name, f"Urlaub bis Austrittsdatum {austritt_text} gesetzt")
                                    continue
                            except Exception as e:
                                print(f"[FEHLER] Austrittsprüfung fehlgeschlagen: {e}")

                        print("[INFO] Eintritt liegt im Urlaubsmonat – setze neuen Eintritt …")

                        print("[AKTION] Suche korrektes Edit-Icon …")
                        all_icons = akte_page.locator("img.edit.sprite_16x16.pointer")
                        count = all_icons.count()
                        print(f"[DEBUG] Gefundene Edit-Icons: {count}")

                        target_icon = None
                        for j in range(count):
                            el = all_icons.nth(j)
                            code = el.get_attribute("onclick") or ""
                            if "openUiWindowReloaded" in code and "eintritt" in code:
                                target_icon = el
                                break

                        onclick_code = target_icon.get_attribute("onclick")
                        akte_page.evaluate(f"window.eval(`{onclick_code}`)")

                        akte_page.wait_for_selector("div.ui-dialog form#formEditEinAustrittsdatum", timeout=20000)
                        print("[OK] Modal erkannt – ändere Eintrittsdaten …")

                        neues_datum = get_first_day_of_next_month(config.URLAUB_YEAR, config.URLAUB_MONTH)
                        bemerkung = f"keine Schicht im {month_name_de(config.URLAUB_MONTH)}"
                        akte_page.fill("#eintrittsdatum_neu", neues_datum)
                        akte_page.fill("#bemerkung", bemerkung)
                        akte_page.click("span.abstand_links_8:has-text('Speichern')")
                        time.sleep(0.8)

                        for sec in range(10):
                            if akte_page.locator("span.ui-button-text", has_text="Fortfahren").count() > 0:
                                akte_page.locator("span.ui-button-text", has_text="Fortfahren").click()
                                print(f"[OK] 'Fortfahren' bestätigt ({sec}s).")
                                break
                            time.sleep(1)

                        handle_anfragen_delete_dialog(akte_page, name, "Eintrittsänderung (nach Nicht-Eintritt)")

                        close_btn = akte_page.locator("button:has-text('Schließen')").first
                        if close_btn.count() > 0:
                            close_btn.click()
                            try:
                                akte_page.locator("form#formEditEinAustrittsdatum").wait_for(
                                    state="detached",
                                    timeout=8000,
                                )
                            except PlaywrightTimeoutError:
                                pass

                        log_korrektur(name, f"Eintritt auf {neues_datum} geändert ({bemerkung})")
                        akte_page.close()
                        page.bring_to_front()
                        continue

                    else:
                        print("[INFO] Eintritt nicht im Urlaubsmonat – prüfe Austritt …")
                        if austritt_text:
                            try:
                                austritt_date = datetime.strptime(austritt_text, "%d.%m.%Y")
                                if austritt_date.month == config.URLAUB_MONTH and austritt_date.year == config.URLAUB_YEAR:
                                    print(f"[INFO] Austritt im Urlaubsmonat erkannt ({austritt_text}) – passe Urlaubseintrag an …")
                                    akte_page.close()
                                    page.bring_to_front()

                                    for td in row.locator("td").all():
                                        onclick = td.get_attribute("onclick") or ""
                                        if "xajax_krank_urlaub_edit" in onclick:
                                            click_urlaub_cell(page, td, onclick)
                                            break

                                    page.wait_for_selector("#form_edit_krank_urlaub", timeout=10000)
                                    print(f"[INFO] Urlaub-Modal erneut geöffnet für {name} …")

                                    dialog = page.locator("div.ui-dialog:visible:has(#form_edit_krank_urlaub)").last
                                    form = dialog.locator("#form_edit_krank_urlaub")
                                    bezahlt_box = form.locator("#bezahlt")
                                    if bezahlt_box.is_checked():
                                        bezahlt_box.uncheck()

                                    form.locator("#tageskennzeichen").select_option("UU")
                                    first_day_str = get_first_day_of_month(config.URLAUB_YEAR, config.URLAUB_MONTH)
                                    von_field = form.locator("#von")
                                    page.evaluate(
                                        "(args) => args.el.value = args.value",
                                        {"el": von_field.element_handle(), "value": first_day_str}
                                    )
                                    bis_field = form.locator("#bis")
                                    page.evaluate(
                                        "(args) => args.el.value = args.value",
                                        {"el": bis_field.element_handle(), "value": austritt_text}
                                    )
                                    dialog.locator("button:has-text('Speichern')").click()
                                    time.sleep(1.5)
                                    msg2 = read_dialog_message(page, dialog)
                                    msg2_l = msg2.lower()
                                    if "abgeglichene schichten" in msg2_l:
                                        print("[WARNUNG] Abgeglichene Schichten – setze 'von' auf den 02. des Monats (im selben Modal) und speichere erneut …")
                                        second_day_str = f"02.{config.URLAUB_MONTH:02d}.{config.URLAUB_YEAR}"
                                        von_field = form.locator("#von")
                                        page.evaluate(
                                            "(args) => args.el.value = args.value",
                                            {"el": von_field.element_handle(), "value": second_day_str}
                                        )
                                        log_korrektur(name, f"Abgeglichene Schichten: von={second_day_str} gesetzt (Austritt bis {austritt_text})")
                                        dialog.locator("button:has-text('Speichern')").click()
                                        time.sleep(1.5)
                                    try:
                                        dialog.wait_for(state="detached", timeout=8000)
                                    except PlaywrightTimeoutError:
                                        close_blocking_dialogs(page)
                                        clear_blocking_overlays(page)
                                    log_korrektur(name, f"Urlaub bis Austrittsdatum {austritt_text} gesetzt")
                                    continue
                            except Exception as e:
                                print(f"[FEHLER] Konnte Austritt {austritt_text} nicht verarbeiten: {e}")
                                continue

                        akte_page.close()

                processed += 1
                elapsed = time.monotonic() - start_ts
                log_step(f"Fertig in {elapsed:.1f}s")
                print(f"[OK] Urlaubseintrag abgeschlossen für {name}")

            except Exception as e:
                print(f"[FEHLER] Fehler bei {name}: {e}")
                continue

        print(f"\n[ENDE] Fertig. Erfolgreich: {processed}, übersprungen: {skipped}")
        browser.close()


def main():
    parser = argparse.ArgumentParser(description="Urlaubsplanung-Scraper")
    parser.add_argument("--headless", choices=["true", "false"], default=None)
    parser.add_argument("--slowmo", type=int, default=None)
    args = parser.parse_args()

    headless = None if args.headless is None else (args.headless.lower() == "true")
    run_urlaub_scraper(headless=headless, slowmo_ms=args.slowmo)


if __name__ == "__main__":
    main()
