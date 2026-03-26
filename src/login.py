from playwright.sync_api import Page, expect
from src import config
import time

def do_login(page: Page):
    print("[INFO] Rufe Loginseite auf …")
    page.goto(config.BASE_URL, wait_until="load")  # Frameset vollständig laden

    # 🕐 Warte bis Frame „inhalt“ erscheint (max. 20s)
    print("[INFO] Warte auf Frame 'inhalt' …")
    for _ in range(40):  # 40 × 0.5s = 20s
        frame = page.frame(name="inhalt")
        if frame:
            print("[OK] Frame 'inhalt' gefunden.")
            break
        time.sleep(0.5)
    else:
        raise Exception("[FEHLER] Frame 'inhalt' nicht gefunden – Frameset evtl. nicht vollständig geladen.")

    # Sicherheitshalber warten, bis Formular sichtbar ist
    print("[INFO] Warte auf Loginformular im Frame …")
    login_field = frame.locator("#loginName")
    if login_field.count() == 0:
        # Bereits eingeloggt oder andere Seite geladen
        print("[INFO] Loginformular nicht sichtbar – Session vermutlich aktiv.")
        frame.page.context.storage_state(path=config.STATE_PATH)
        print(f"[OK] Session gespeichert unter: {config.STATE_PATH}")
        return
    login_field.wait_for(state="visible", timeout=15000)
    print("[OK] Loginformular erkannt.")

    username = config.USERNAME
    password = config.PASSWORD
    if not username or not password:
        raise Exception("Fehlende Zugangsdaten in .env (PERSPLAN_USER / PERSPLAN_PASS).")

    print("[INFO] Fülle Loginformular aus …")
    frame.fill("#loginName", username)
    frame.fill("#loginPassword", password)
    frame.click("#loginSubmitButton")

    print("[INFO] Formular abgesendet – warte auf Weiterleitung …")
    frame.wait_for_load_state("networkidle", timeout=20000)
    time.sleep(2)

    # Statt URL prüfen wir jetzt den Inhalt (da PersPlan Frame-Inhalte ersetzt)
    html = frame.content()

    if "#loginName" in html or "Passwort" in html:
        # Formular immer noch da → vermutlich kein erfolgreicher Login
        if frame.locator("#error-display-content").is_visible():
            msg = frame.locator("#error-display-content").inner_text()
            raise Exception(f"[FEHLER] Login fehlgeschlagen: {msg}")
        else:
            raise Exception("[FEHLER] Login fehlgeschlagen – keine Weiterleitung erkannt.")
    else:
        print("[OK] Login erfolgreich erkannt.")
        title = frame.title()
        print(f"[OK] Dashboard-Titel: {title}")
        # Session speichern für spätere Wiederverwendung
        frame.page.context.storage_state(path=config.STATE_PATH)
        print(f"[OK] Session gespeichert unter: {config.STATE_PATH}")
