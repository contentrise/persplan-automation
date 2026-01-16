import argparse
import calendar
import time
from datetime import datetime
from pathlib import Path

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

            target_td = None
            for td in tds:
                onclick = td.get_attribute("onclick") or ""
                if "xajax_krank_urlaub_edit" in onclick:
                    target_td = td
                    print(f"[AKTION] Klicke auf Urlaubseintrag-Feld ({onclick})")
                    td.click()
                    break
            if not target_td:
                print("⚠ Keine klickbare Zelle gefunden – überspringe.")
                continue

            try:
                page.wait_for_selector("#form_edit_krank_urlaub", timeout=10000)
                print(f"[INFO] Modal geöffnet für {name} …")

                bezahlt_box = page.locator("#bezahlt")
                if bezahlt_box.is_checked():
                    bezahlt_box.uncheck()
                    print("   → 'bezahlt' deaktiviert")

                page.select_option("#tageskennzeichen", "UU")
                print("   → Fehlzeitenkennzeichnung = 'UU'")

                first_day_str = get_first_day_of_month(config.URLAUB_YEAR, config.URLAUB_MONTH)
                von_field = page.locator("#von")
                page.evaluate(
                    "(args) => args.el.value = args.value",
                    {"el": von_field.element_handle(), "value": first_day_str}
                )
                print(f"   → 'von' = {first_day_str}")

                last_day_str = get_last_day_of_month(config.URLAUB_YEAR, config.URLAUB_MONTH)
                bis_field = page.locator("#bis")
                page.evaluate(
                    "(args) => args.el.value = args.value",
                    {"el": bis_field.element_handle(), "value": last_day_str}
                )
                print(f"   → 'bis' = {last_day_str}")

                page.locator("button:has-text('Speichern')").click()
                time.sleep(2)

                if page.locator("#msg_out p").count() > 0:
                    msg = page.locator("#msg_out p").inner_text().strip()
                    if "nicht eingetreten" in msg.lower():
                        print("[WARNUNG] Mitarbeiter nicht eingetreten – starte Eintritts-Korrektur …")
                        page.locator(".ui-dialog-titlebar-close").first.click()
                        time.sleep(1)

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
                                    if austritt_date <= datetime(config.URLAUB_YEAR, config.URLAUB_MONTH, 31):
                                        log_warnung(name, eintritt_text, austritt_text)
                                        print(f"[WARNUNG] {name}: Eintrittsänderung übersprungen – Austritt {austritt_text} im selben/älteren Monat.")
                                        akte_page.close()
                                        page.bring_to_front()
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

                            for sec in range(10):
                                if akte_page.locator("span.ui-button-text", has_text="Fortfahren").count() > 0:
                                    akte_page.locator("span.ui-button-text", has_text="Fortfahren").click()
                                    print(f"[OK] 'Fortfahren' bestätigt ({sec}s).")
                                    break
                                time.sleep(1)

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
                                                td.click()
                                                break

                                        page.wait_for_selector("#form_edit_krank_urlaub", timeout=10000)
                                        print(f"[INFO] Urlaub-Modal erneut geöffnet für {name} …")

                                        bezahlt_box = page.locator("#bezahlt")
                                        if bezahlt_box.is_checked():
                                            bezahlt_box.uncheck()

                                        page.select_option("#tageskennzeichen", "UU")
                                        first_day_str = get_first_day_of_month(config.URLAUB_YEAR, config.URLAUB_MONTH)
                                        von_field = page.locator("#von")
                                        page.evaluate(
                                            "(args) => args.el.value = args.value",
                                            {"el": von_field.element_handle(), "value": first_day_str}
                                        )
                                        bis_field = page.locator("#bis")
                                        page.evaluate(
                                            "(args) => args.el.value = args.value",
                                            {"el": bis_field.element_handle(), "value": austritt_text}
                                        )
                                        page.locator("button:has-text('Speichern')").click()
                                        time.sleep(1)
                                        log_korrektur(name, f"Urlaub bis Austrittsdatum {austritt_text} gesetzt")
                                        continue
                                except Exception as e:
                                    print(f"[FEHLER] Konnte Austritt {austritt_text} nicht verarbeiten: {e}")
                                    continue

                            akte_page.close()

                processed += 1
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
