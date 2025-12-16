import argparse
import time
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import Frame, Page, TimeoutError, sync_playwright

from src import config
from src.login import do_login


DEFAULT_PROFILE_URL = (
    "https://greatstaff.persplan.net/"
    "mitarbeiter_akte.php?secureid=2.2ebclWqeyIFJD1fwBdusS8esmY4v150hiSrpPktt6ZM"
)
DEFAULT_FILE = Path("import_vertragsanpassung") / "Vertragsanpassungen Stunden 11-2025-001.pdf"
DEFAULT_REMARK = "Import Vertragsanpassungen"
DEFAULT_FOLDER_ID = "1"  # "Dokumente"


def _wait_for_inhalt_frame(page: Page, timeout_seconds: int = 5) -> Frame | None:
    """Wait briefly for the optional 'inhalt' frame to exist."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        frame = page.frame(name="inhalt")
        if frame:
            return frame
        time.sleep(0.2)
    return None


def _ensure_logged_in(browser):
    """Create a context that is logged in, reusing storage_state if possible."""
    state_path = Path(config.STATE_PATH)
    context = None
    page = None
    if state_path.exists():
        try:
            print(f"[INFO] Verwende gespeicherten Login-State aus {state_path}")
            context = browser.new_context(storage_state=str(state_path))
            page = context.new_page()
            page.goto(config.BASE_URL, wait_until="domcontentloaded", timeout=20000)
            frame = _wait_for_inhalt_frame(page, timeout_seconds=3)
            target = frame if frame else page
            if target.locator("#loginName").count() > 0:
                raise RuntimeError("Login-Formular sichtbar – Session abgelaufen.")
            return context, page
        except Exception as exc:
            print(f"[WARNUNG] Gespeicherter State ungültig ({exc}) – führe Login erneut durch.")
            if context:
                context.close()
            context = None
            page = None

    print("[INFO] Starte manuellen Login …")
    page = browser.new_page()
    do_login(page)
    return page.context, page


def _navigate_to_mitarbeiterinformationen(page: Page) -> Page:
    """Open the 'Mitarbeiterinformationen' tab for the current Mitarbeiterakte."""
    frame = _wait_for_inhalt_frame(page, timeout_seconds=2)
    target = frame if frame else page
    link = target.locator("#tableOfSubmenue a", has_text="Mitarbeiterinformationen").first
    if link.count() == 0:
        raise RuntimeError("Link 'Mitarbeiterinformationen' nicht gefunden.")

    href = link.get_attribute("href") or ""
    print("[AKTION] Öffne Tab 'Mitarbeiterinformationen' …")
    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=20000):
            link.click()
        return page
    except TimeoutError:
        print("[INFO] Navigation hat keinen Seitenwechsel ausgelöst – prüfe Inhalt …")
    except Exception as exc:
        print(f"[WARNUNG] Klick auf 'Mitarbeiterinformationen' fehlgeschlagen: {exc}")

    if href:
        target_url = urljoin(config.BASE_URL, href)
        print(f"[INFO] Fallback: direktes Laden {target_url}")
        target.goto(target_url, wait_until="domcontentloaded", timeout=20000)
    return page


def _open_document_dialog(page: Page):
    """Click 'Dokument hinzufügen' and wait for the dropzone modal."""
    frame = _wait_for_inhalt_frame(page, timeout_seconds=2)
    target = frame if frame else page
    button = target.locator("button", has_text="Dokument hinzufügen").first
    if button.count() == 0:
        raise RuntimeError("Button 'Dokument hinzufügen' nicht gefunden.")
    button.click()
    page.wait_for_selector("#maDokDropzone", timeout=15000, state="visible")
    page.wait_for_selector("input.dz-hidden-input", timeout=15000, state="attached")
    print("[OK] Upload-Dialog geöffnet.")


def _upload_file(page: Page, file_path: Path, remark: str, folder_id: str):
    """Upload the file via Dropzone, fill remark and select folder."""
    if not file_path.exists():
        raise FileNotFoundError(f"Datei nicht gefunden: {file_path}")

    dropzone = page.locator("#maDokDropzone").first
    dropzone.click()
    page.wait_for_selector("input.dz-hidden-input", timeout=15000, state="attached")
    file_input = page.locator("input.dz-hidden-input").last
    file_input.set_input_files(str(file_path))
    print(f"[INFO] Datei hochgeladen: {file_path.name}")

    table = page.locator("#tableAuflistungDateien tbody tr")
    page.wait_for_selector("#tableAuflistungDateien tbody tr", timeout=30000)
    row = table.last

    if remark:
        textarea = row.locator("textarea").first
        if textarea.count() > 0:
            textarea.fill(remark)
            print(f"[INFO] Bemerkung gesetzt: {remark}")

    if folder_id:
        select = row.locator("select").first
        if select.count() > 0:
            select.select_option(folder_id)
            print(f"[INFO] Ordner gewählt (ID {folder_id}).")


def _save_upload(page: Page, hold_seconds: float):
    """Click the 'Speichern' button inside the modal and optionally hold."""
    save_button = page.locator("#formDateiupload button", has_text="Speichern").first
    if save_button.count() == 0:
        raise RuntimeError("Speichern-Button nicht im Upload-Dialog gefunden.")

    save_button.click()
    page.wait_for_timeout(2000)
    print("[INFO] Upload gespeichert (XAJAX-Call abgeschickt).")
    if hold_seconds > 0:
        print(f"[INFO] Halte Dialog {hold_seconds} Sekunden offen …")
        time.sleep(hold_seconds)


def run_testscraper(
    profile_url: str,
    file_path: Path,
    remark: str = DEFAULT_REMARK,
    folder_id: str = DEFAULT_FOLDER_ID,
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    hold_seconds: float = 2.0,
):
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms or 0)
        context, page = _ensure_logged_in(browser)

        print(f"[INFO] Öffne Mitarbeiterakte: {profile_url}")
        page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
        mitarb_page = _navigate_to_mitarbeiterinformationen(page)

        _open_document_dialog(mitarb_page)
        _upload_file(mitarb_page, file_path, remark, folder_id)
        _save_upload(mitarb_page, hold_seconds)

        print("[OK] Fertig – Browser schließen.")
        browser.close()


def main():
    parser = argparse.ArgumentParser(description="Testet den Upload eines Dokuments in der Mitarbeiterakte.")
    parser.add_argument("--url", default=DEFAULT_PROFILE_URL, help="Ziel-URL der Mitarbeiterakte.")
    parser.add_argument(
        "--file",
        default=str(DEFAULT_FILE),
        help="Pfad zur hochzuladenden Datei (Standard: erste Vertragsanpassung).",
    )
    parser.add_argument("--remark", default=DEFAULT_REMARK, help="Text für das Bemerkungsfeld.")
    parser.add_argument("--folder", default=DEFAULT_FOLDER_ID, help="Ordner-ID aus dem Dropdown (z. B. 1 = Dokumente).")
    parser.add_argument("--headless", choices=["true", "false"], default=None, help="Playwright headless-Modus überschreiben.")
    parser.add_argument("--slowmo", type=int, default=None, help="Playwright slow_mo in Millisekunden.")
    parser.add_argument("--hold", type=float, default=2.0, help="Sekunden, die der Dialog nach dem Speichern offen bleiben soll.")
    args = parser.parse_args()

    headless = None
    if args.headless is not None:
        headless = args.headless.lower() == "true"

    run_testscraper(
        profile_url=args.url,
        file_path=Path(args.file),
        remark=args.remark,
        folder_id=args.folder,
        headless=headless,
        slowmo_ms=args.slowmo,
        hold_seconds=args.hold,
    )


if __name__ == "__main__":
    main()
