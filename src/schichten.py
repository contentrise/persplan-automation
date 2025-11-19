from playwright.sync_api import Page
from src import config
import time


def open_schichtplan(page: Page):
    print("[INFO] Suche Frame 'oben' …")
    frame_top = None
    for _ in range(40):
        frame_top = page.frame(name="oben")
        if frame_top:
            print("[OK] Frame 'oben' gefunden.")
            break
        time.sleep(0.5)
    else:
        raise Exception("[FEHLER] Frame 'oben' nicht gefunden.")

    print("[INFO] Klicke auf 'PLANUNG' …")
    selectors = [
        ("div.mainmenue_button_text", "PLANUNG"),
        ("div.mainmenue_button", "PLANUNG"),
        ("a", "PLANUNG"),
    ]
    button = None
    for selector, text in selectors:
        loc = frame_top.locator(selector, has_text=text)
        if loc.count() > 0:
            button = loc.first
            break
    if not button:
        fallback = frame_top.locator("text=PLANUNG")
        if fallback.count() > 0:
            button = fallback.first

    if not button:
        raise Exception("[FEHLER] Button 'PLANUNG' konnte nicht gefunden werden.")

    try:
        button.scroll_into_view_if_needed(timeout=2000)
    except Exception:
        pass
    try:
        button.wait_for(state="visible", timeout=8000)
        button.click()
    except Exception as exc:
        raise Exception("[FEHLER] Button 'PLANUNG' konnte nicht angeklickt werden.") from exc

    # --- Warte bis Loader verschwindet ---
    print("[INFO] Warte bis Ladeanimation beendet ist …")
    frame_content = None
    for _ in range(80):
        frame_content = page.frame(name="inhalt")
        if frame_content:
            loader = frame_content.locator("img[src*='bigLoader.gif']")
            if loader.count() == 0 or not loader.first.is_visible():
                print("[OK] Ladeanimation beendet, Seite bereit.")
                break
        time.sleep(0.5)
    else:
        print("[WARNUNG] Kein sichtbarer Loader gefunden – fahre fort …")

    # --- Klicke auf Staffing ---
    print("[INFO] Suche nach Staffing-Link …")
    for _ in range(60):
        frame_content = page.frame(name="inhalt")
        if frame_content:
            staffing_link = frame_content.locator("a[href*='planung.php?link=staffing']")
            if staffing_link.count() > 0:
                print("[OK] Staffing-Link gefunden, klicke …")
                staffing_link.first.click()
                break
        time.sleep(0.5)
    else:
        print("[WARNUNG] Kein Staffing-Link gefunden – rufe direkt auf …")
        page.evaluate("""() => { parent.inhalt.location='/planung.php?link=staffing'; }""")

    # --- Warten, bis Staffing-DOM sichtbar ist ---
    print("[INFO] Warte auf Staffing-DOM …")
    for _ in range(100):
        frame_content = page.frame(name="inhalt")
        if frame_content and frame_content.locator("select#monat").count() > 0:
            print("[OK] Staffing-DOM erkannt – Seite vollständig geladen.")
            break
        time.sleep(0.5)
    else:
        raise Exception("[FEHLER] Staffing-DOM nicht gefunden – Seite evtl. nicht korrekt geladen.")

    # ========================
    # 🟢 Filter + Monat setzen & Anzeigen klicken
    # ========================
    try:
        # Vertragstyp setzen
        vertragstyp_value = str(config.VERTRAGSTYP)
        print(f"[INFO] Wähle Vertragstyp (value={vertragstyp_value}) aus …")
        frame_content.wait_for_selector("select[name='filter_vertragstypen[]']", timeout=5000)
        frame_content.select_option("select[name='filter_vertragstypen[]']", value=vertragstyp_value)
        time.sleep(0.8)

        # Monat setzen
        month_value = str(config.MONTH)
        print(f"[INFO] Setze Monat auf {month_value} …")
        frame_content.wait_for_selector("select#monat", timeout=5000)
        frame_content.select_option("select#monat", value=month_value)
        time.sleep(0.8)

        # Klick auf "Anzeigen"
        print("[INFO] Klicke auf 'Anzeigen' …")
        button = frame_content.locator("span.abstand_links_8", has_text="Anzeigen")
        button.wait_for(state="visible", timeout=5000)
        button.click()
        print("[OK] Filter & Monat angewendet, Ansicht wird geladen …")

        # Warte bis Loader verschwindet
        for _ in range(60):
            loader = frame_content.locator("img[src*='bigLoader.gif']")
            if loader.count() == 0 or not loader.first.is_visible():
                print("[OK] Ansicht fertig geladen.")
                break
            time.sleep(0.5)
        else:
            print("[WARNUNG] Kein Ladeende erkannt – fahre fort …")

        time.sleep(2)

    except Exception as e:
        print(f"[WARNUNG] Konnte Filter oder Monat nicht anwenden: {e}")

    # --- Debug-Vorschau ---
    html = frame_content.content()
    print(html[:800])

    print("[OK] Staffing-Ansicht gefiltert – fahre fort …")
    return frame_content
