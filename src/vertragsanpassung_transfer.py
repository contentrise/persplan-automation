import io
import json
import os
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

from src import config
from src.login import do_login
from src.mitarbeiter_vervollstaendigen import (
    _locate_search_input,
    _open_user_overview,
    _open_mitarbeiterinformationen,
    _upload_document_with_modal,
    _extract_documents_table,
    _document_present,
)


class _Tee:
    def __init__(self, primary, buffer):
        self.primary = primary
        self.buffer = buffer

    def write(self, data):
        try:
            self.primary.write(data)
        except Exception:
            pass
        try:
            self.buffer.write(data)
        except Exception:
            pass
        return len(data)

    def flush(self):
        try:
            self.primary.flush()
        except Exception:
            pass
        try:
            self.buffer.flush()
        except Exception:
            pass


def _load_payload() -> dict:
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
    return payload


def _find_pdf_file() -> str:
    input_dir = Path(os.environ.get("PERSO_INPUT_DIR", "perso-input"))
    preferred = input_dir / "vertragsanpassung.pdf"
    if preferred.exists():
        return str(preferred)
    candidates = sorted(input_dir.glob("*.pdf"))
    if not candidates:
        return ""
    return str(candidates[0])


def _click_first_row(target):
    rows = target.locator("table#user_tbl tbody tr")
    parent_page = target.page if hasattr(target, "page") else target
    try:
        row_count = rows.count()
    except Exception:
        row_count = 0
    if row_count == 0:
        print("[WARNUNG] Keine Trefferzeilen gefunden.")
        return None
    row = rows.first
    link = row.locator("a.ma_akte_link_text, a.ma_akte_link_img").first
    if link.count() == 0:
        link = row.locator("a").first
    if link.count() == 0:
        print("[WARNUNG] Kein klickbarer Link in der Trefferzeile gefunden.")
        return None
    href = link.get_attribute("href") or ""
    if href:
        try:
            parent_page.goto(href if href.startswith("http") else f"{config.BASE_URL}{href}", wait_until="domcontentloaded", timeout=20000)
            return parent_page
        except Exception as exc:
            print(f"[WARNUNG] Direktlink fehlgeschlagen: {exc}")
    try:
        link.scroll_into_view_if_needed()
    except Exception:
        pass
    try:
        with parent_page.context.expect_page(timeout=3000) as new_page_event:
            link.click()
        new_page = new_page_event.value
        new_page.wait_for_load_state("domcontentloaded", timeout=15000)
        return new_page
    except Exception:
        try:
            link.click()
        except Exception:
            try:
                link.evaluate("el => el.click()")
            except Exception as exc:
                print(f"[WARNUNG] Klick auf Trefferzeile fehlgeschlagen: {exc}")
                return None
    return parent_page


def run_vertragsanpassung_transfer(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
):
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    payload = _load_payload()
    personalnummer = str(payload.get("personalnummer", "")).strip()
    phone = str(payload.get("phone", "")).strip()
    description = str(payload.get("description", "")).strip()
    if not personalnummer and not phone:
        raise RuntimeError("[FEHLER] Keine Personalnummer oder Telefonnummer im Payload gefunden.")
    if not description:
        raise RuntimeError("[FEHLER] Keine Beschreibung im Payload gefunden.")

    pdf_path = _find_pdf_file()
    if not pdf_path:
        raise RuntimeError("[FEHLER] Keine PDF-Datei im PERSO_INPUT_DIR gefunden.")

    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    prev_stdout = sys.stdout
    prev_stderr = sys.stderr
    sys.stdout = _Tee(prev_stdout, stdout_buffer)
    sys.stderr = _Tee(prev_stderr, stderr_buffer)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
            context = browser.new_context()
            page = context.new_page()

            print("[INFO] Login wird gestartet …")
            do_login(page)
            target = _open_user_overview(page)

            search_input = _locate_search_input(target)
            if search_input.count() == 0:
                raise RuntimeError("[FEHLER] Suchfeld in user.php nicht gefunden.")

            search_term = personalnummer or phone
            search_input.fill(search_term)
            time.sleep(0.3)
            if personalnummer:
                print(f"[INFO] Suche nach Personalnummer: {personalnummer}")
            else:
                print(f"[INFO] Suche nach Telefon: {phone}")

            target_page = _click_first_row(target)
            if not target_page:
                print("[INFO] Kein Treffer geklickt.")
                browser.close()
                return

            if _open_mitarbeiterinformationen(target_page):
                print("[OK] Mitarbeiterinformationen geöffnet.")
                docs_before = _extract_documents_table(target_page)
                if _document_present(docs_before, ["vertragsanpassung", description]):
                    print("[INFO] Vertragsanpassung bereits vorhanden – überspringe Upload.")
                    browser.close()
                    return
                print(f"[INFO] Lade Vertragsanpassung hoch: {Path(pdf_path).name}")
                _upload_document_with_modal(
                    page=target_page,
                    file_path=pdf_path,
                    folder_label="- Arbeitsvertrag",
                    folder_value="3",
                    bemerkung_text=description,
                )
                docs_after = _extract_documents_table(target_page)
                if _document_present(docs_after, ["vertragsanpassung", description]):
                    print("[OK] Vertragsanpassung hochgeladen.")
                else:
                    print("[WARNUNG] Upload wurde nicht bestätigt.")
            else:
                print("[WARNUNG] Mitarbeiterinformationen konnten nicht geöffnet werden.")

            browser.close()
    finally:
        sys.stdout = prev_stdout
        sys.stderr = prev_stderr


if __name__ == "__main__":
    run_vertragsanpassung_transfer()
