from playwright.sync_api import Page
from src import config
import time


def open_first_mitarbeiterakte(page: Page):
    """
    Öffnet auf der Staffing-Seite den ersten Mitarbeiter-Link ("Zur MA-Akte"),
    erkennt automatisch den neuen Tab, klickt dort auf 'Anfragen',
    und filtert dort den gewünschten Monat/Jahr aus configuration.txt.
    """
    print("[INFO] Suche nach erstem Mitarbeiter-Link …")
    frame_content = None

    # Frame 'inhalt' finden
    for _ in range(40):
        frame_content = page.frame(name="inhalt")
        if frame_content:
            break
        time.sleep(0.5)
    if not frame_content:
        raise Exception("[FEHLER] Frame 'inhalt' nicht gefunden.")

    # Tabelle mit MA-Links finden
    for _ in range(60):
        links = frame_content.locator("#tbl_staffing a img[title='Zur MA-Akte']")
        if links.count() > 0:
            print(f"[OK] {links.count()} Mitarbeiter-Link(s) gefunden.")
            break
        time.sleep(0.5)
    else:
        raise Exception("[FEHLER] Keine Mitarbeiter-Tabelle gefunden (tbl_staffing).")

    # Popup-Event vorbereiten
    print("[INFO] Klicke auf ersten 'Zur MA-Akte'-Link und warte auf neuen Tab …")
    with page.context.expect_page() as new_page_event:
        first_link = frame_content.locator("#tbl_staffing a img[title='Zur MA-Akte']").first
        first_link.scroll_into_view_if_needed()
        first_link.click()

    # Neuer Tab
    new_page = new_page_event.value
    new_page.wait_for_load_state("domcontentloaded", timeout=15000)
    print(f"[OK] Neuer Tab erkannt: {new_page.url}")
    new_page.bring_to_front()

    # --- Warte auf "Anfragen"-Link ---
    print("[INFO] Warte auf DOM der Mitarbeiterakte …")
    for _ in range(60):
        if new_page.locator("a", has_text="Anfragen").count() > 0:
            print("[OK] DOM vollständig – 'Anfragen'-Link gefunden.")
            break
        time.sleep(0.5)
    else:
        raise Exception("[FEHLER] Kein 'Anfragen'-Link auf der Mitarbeiterakte gefunden.")

    # --- Klick auf "Anfragen" ---
    try:
        print("[INFO] Klicke auf 'Anfragen' …")
        anfragen_link = new_page.locator("a", has_text="Anfragen").first
        anfragen_link.scroll_into_view_if_needed()
        anfragen_link.click()
        print("[OK] 'Anfragen'-Link erfolgreich angeklickt.")

        # Warte auf neue Ansicht
        new_page.wait_for_load_state("domcontentloaded", timeout=10000)
        time.sleep(2)
    except Exception as e:
        raise Exception(f"[FEHLER] Klick auf 'Anfragen' fehlgeschlagen: {e}")

    # --- Monat / Jahr auswählen ---
    try:
        month_value = str(config.MONTH)
        year_value = str(config.YEAR)
        print(f"[INFO] Setze Monat={month_value}, Jahr={year_value} in der Anfragen-Ansicht …")

        # Warte bis Dropdowns sichtbar sind
        new_page.wait_for_selector("select#von_monat", timeout=8000)
        new_page.wait_for_selector("select#von_jahr", timeout=8000)

        # Monat wählen
        new_page.select_option("select#von_monat", value=month_value)
        time.sleep(0.5)

        # Jahr wählen
        new_page.select_option("select#von_jahr", value=year_value)
        time.sleep(0.5)

        # Formular-Reload abwarten (onchange-Trigger)
        print("[INFO] Filter angewendet – warte auf Reload …")
        new_page.wait_for_load_state("networkidle", timeout=10000)
        print("[OK] Monat/Jahr erfolgreich gesetzt & Seite neu geladen.")

    except Exception as e:
        print(f"[WARNUNG] Konnte Monat/Jahr in Anfragen-Ansicht nicht setzen: {e}")

    print("[OK] Anfragen-Ansicht gefiltert und aktiv.")
    time.sleep(2)
    return new_page


def click_anfragen_tab(page: Page):
    """
    Wird vom Loop aufgerufen, um in der geöffneten Mitarbeiterakte
    direkt auf 'Anfragen' zu klicken und Monat/Jahr zu setzen.
    """
    print("[INFO] Suche Link 'Anfragen' …")
    link = page.locator("a", has_text="Anfragen")
    if link.count() == 0:
        raise Exception("Kein 'Anfragen'-Link in der Akte gefunden.")
    
    link.first.scroll_into_view_if_needed()
    link.first.click()
    print("[OK] Auf 'Anfragen' geklickt – warte auf Tabelle …")

    for _ in range(60):
        if page.locator("#tbl_ma_anfragen").count() > 0:
            print("[OK] Tabelle #tbl_ma_anfragen geladen.")
            break
        time.sleep(0.5)
    else:
        raise Exception("Tabelle #tbl_ma_anfragen nicht gefunden.")

    month_value = str(config.MONTH)
    year_value = str(config.YEAR)
    print(f"[INFO] Setze Filter Monat={month_value}, Jahr={year_value} …")

    try:
        page.select_option("select#von_monat", value=month_value)
        time.sleep(0.4)
        page.select_option("select#von_jahr", value=year_value)
        time.sleep(0.8)
        page.wait_for_load_state("networkidle", timeout=10000)
        print("[OK] Monat/Jahr gesetzt & Seite aktualisiert.")
    except Exception as e:
        print(f"[WARNUNG] Konnte Filter nicht anwenden: {e}")

    return page
