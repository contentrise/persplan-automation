import argparse
import json
import logging
import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from src import config
from src.login import do_login

CACHE_PATH = Path(".cache/persplan_user_map.json")

logging.basicConfig(
    level=os.environ.get("SHIFT_APPLY_LOG_LEVEL", "INFO"),
    format="[%(asctime)s] %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("shift_apply")


def _dump_debug(page, suffix: str, reason: str) -> None:
    export_dir = Path(config.EXPORT_DIR) / "debug"
    export_dir.mkdir(parents=True, exist_ok=True)
    safe_suffix = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in suffix)
    base = export_dir / f"shift_apply_{safe_suffix}"
    try:
        page.screenshot(path=str(base.with_suffix(".png")), full_page=True)
    except Exception:
        pass
    try:
        html = page.content()
        base.with_suffix(".html").write_text(html, encoding="utf-8")
    except Exception:
        pass
    try:
        base.with_suffix(".txt").write_text(reason, encoding="utf-8")
    except Exception:
        pass


def _load_cache():
    if not CACHE_PATH.exists():
        return {}
    try:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cache(cache):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _open_user_table(page):
    url = urljoin(config.BASE_URL, "user.php")
    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    frame = page.frame(name="inhalt")
    target = frame if frame else page
    target.wait_for_selector("table#user_tbl tbody tr", timeout=20000)
    return target


def _find_user_id_by_persnr(page, persnr):
    if not persnr:
        return ""
    LOGGER.info("Suche User per PersNr: %s", persnr)
    target = _open_user_table(page)
    search = target.locator("#user_tbl_filter input[type='search'], input[aria-controls='user_tbl']").first
    search.fill(persnr)
    time.sleep(0.5)
    rows = target.locator("table#user_tbl tbody tr")
    count = rows.count()
    for i in range(count):
        row = rows.nth(i)
        try:
            cell = row.locator("td").first.inner_text().strip()
        except Exception:
            cell = ""
        if cell == persnr:
            return row.get_attribute("data-user_id") or ""
    return ""


def _find_user_id_by_query(page, query):
    if not query:
        return ""
    LOGGER.info("Suche User per Query: %s", query)
    target = _open_user_table(page)
    search = target.locator("#user_tbl_filter input[type='search'], input[aria-controls='user_tbl']").first
    search.fill(query)
    time.sleep(0.5)
    rows = target.locator("table#user_tbl tbody tr")
    count = rows.count()
    for i in range(count):
        row = rows.nth(i)
        try:
            text = " ".join(row.locator("td").all_inner_texts()).strip()
        except Exception:
            text = ""
        if query.lower() in text.lower():
            return row.get_attribute("data-user_id") or ""
    return ""


def _open_event(page, event_url):
    LOGGER.info("Öffne Event: %s", event_url)
    page.goto(event_url, wait_until="domcontentloaded", timeout=30000)
    try:
        page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass


def _find_submit_target(page):
    selector = "select[name='different_user_id']"
    if page.locator(selector).count() > 0:
        return page
    for frame in page.frames:
        try:
            if frame.locator(selector).count() > 0:
                return frame
        except Exception:
            continue
    return None


def _extract_popup_url(onclick_value: str) -> str:
    if not onclick_value:
        return ""
    # openWindow('termin_input.php?...') or openWindow("termin_input.php?...")
    match = re.search(r"openWindow\(['\"]([^'\"]+)['\"]", onclick_value)
    if not match:
        # Fallback: any termin_input.php?... URL in handler string
        match = re.search(r"(termin_input\.php\?[^'\"\s)]+)", onclick_value)
    if not match:
        return ""
    return match.group(1)


def _click_apply_for_shift(page, schicht_id):
    LOGGER.info("Suche Schichtzeile für ID: %s", schicht_id)
    row_selector = f"tr:has(#cb_{schicht_id})"
    row = page.locator(row_selector).first
    if row.count() == 0:
        raise RuntimeError(f"Schicht-ID {schicht_id} nicht gefunden")
    button = row.locator("img.group_add").first
    if button.count() == 0:
        raise RuntimeError("Buchungsanfrage-Button nicht gefunden")
    onclick_value = button.get_attribute("onclick") or ""
    if not onclick_value:
        try:
            onclick_value = button.evaluate(
                "el => el.getAttribute('onclick') || (el.onclick ? el.onclick.toString() : '')"
            )
        except Exception:
            onclick_value = ""
    popup_path = _extract_popup_url(onclick_value)
    if popup_path:
        popup_url = urljoin(config.BASE_URL, popup_path)
        LOGGER.info("Öffne Buchungsanfrage-URL direkt: %s", popup_url)
        popup = page.context.new_page()
        popup.goto(popup_url, wait_until="domcontentloaded", timeout=30000)
        try:
            popup.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        return popup
    if onclick_value:
        LOGGER.warning("Kein Popup-Link im onclick gefunden: %s", onclick_value[:200])
    LOGGER.info("Klicke Buchungsanfrage-Button für Schicht %s", schicht_id)
    try:
        with page.context.expect_page(timeout=15000) as popup_event:
            button.click()
        popup = popup_event.value
        popup.wait_for_load_state("domcontentloaded", timeout=15000)
        return popup
    except PlaywrightTimeoutError:
        LOGGER.warning("Kein Popup geöffnet – prüfe Formular im aktuellen Tab")
        button.click()
        return page


def _submit_form(target, user_id, remark):
    LOGGER.info("Sende Buchungsanfrage für User-ID: %s", user_id)
    try:
        LOGGER.info("Formular-URL: %s", target.url)
    except Exception:
        pass
    # Falls Login-Seite statt Formular geladen wurde
    try:
        if target.locator("#loginName").count() > 0:
            raise RuntimeError("LOGIN_REQUIRED: Formular-Seite ist Login (Session fehlt im Popup).")
    except Exception:
        pass
    target = _find_submit_target(target) or target
    target.wait_for_selector("select[name='different_user_id']", timeout=15000)
    target.select_option("select[name='different_user_id']", str(user_id))
    if remark is not None:
        target.fill("#bemerkung", remark)
    target.locator("input.button[value='senden'], input[name='send_it']").first.click()
    try:
        target.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass


def run(payload, headless: bool | None = None):
    persnr = str(payload.get("persNr") or "").strip()
    email = str(payload.get("email") or "").strip()
    phone = str(payload.get("phone") or "").strip()
    event_url = str(payload.get("event_url") or "").strip()
    schicht_id = str(payload.get("shift_id") or "").strip()
    remark = payload.get("remark") or ""
    provided_user_id = str(payload.get("persplan_user_id") or "").strip()

    if not event_url or not schicht_id:
        raise RuntimeError("event_url oder shift_id fehlt")

    cache = _load_cache()
    resolved_user_id = provided_user_id or cache.get(persnr) or ""

    LOGGER.info(
        "Shift-Apply Start: persNr=%s, email=%s, phone=%s, provided_user_id=%s, cached_user_id=%s, event_url=%s, shift_id=%s",
        persnr or "-",
        email or "-",
        phone or "-",
        provided_user_id or "-",
        cache.get(persnr) or "-",
        event_url or "-",
        schicht_id or "-",
    )

    headless = config.HEADLESS if headless is None else headless
    LOGGER.info("Headless=%s", headless)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(storage_state=str(config.STATE_PATH))
        page = context.new_page()
        active_page = page
        page.goto(config.BASE_URL, wait_until="load")
        do_login(page)

        try:
            # Immer versuchen zu suchen; falls Suche fehlschlägt, kann Cache als Fallback dienen.
            searched_user_id = ""
            if persnr:
                searched_user_id = _find_user_id_by_persnr(page, persnr)
            if not searched_user_id and email:
                searched_user_id = _find_user_id_by_query(page, email)
            if not searched_user_id and phone:
                searched_user_id = _find_user_id_by_query(page, phone)

            if searched_user_id:
                resolved_user_id = searched_user_id
                LOGGER.info("User gefunden: persNr=%s → user_id=%s", persnr or "-", resolved_user_id)
                if persnr:
                    cache[persnr] = resolved_user_id
                    _save_cache(cache)
            elif not resolved_user_id:
                raise RuntimeError(
                    "USER_NOT_FOUND: Kein Persplan-User gefunden (PersNr/Email/Telefon)."
                )
            else:
                LOGGER.warning("User-Suche schlug fehl, verwende Cache/User-ID: %s", resolved_user_id)

            _open_event(page, event_url)
            popup = _click_apply_for_shift(page, schicht_id)
            active_page = popup
            _submit_form(popup, resolved_user_id, remark)
            popup.close()
        except Exception as exc:
            _dump_debug(active_page, schicht_id or "unknown", str(exc))
            browser.close()
            raise

        browser.close()

    return {
        "success": True,
        "resolved_user_id": resolved_user_id,
        "message": "Buchungsanfrage gesendet",
    }


def main():
    parser = argparse.ArgumentParser(description="Shift Apply")
    parser.add_argument("--payload-file", required=True)
    parser.add_argument("--headless", choices=["true", "false"], default=None)
    args = parser.parse_args()

    payload_path = Path(args.payload_file)
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    headless = None
    if args.headless is not None:
        headless = args.headless == "true"
    try:
        result = run(payload, headless=headless)
        print(json.dumps(result, ensure_ascii=False))
    except Exception as exc:
        error_text = str(exc)
        error_type = "unknown"
        if "USER_NOT_FOUND" in error_text:
            error_type = "user_not_found"
        print(
            json.dumps(
                {
                    "success": False,
                    "error": error_text,
                    "error_type": error_type,
                },
                ensure_ascii=False,
            )
        )
        raise SystemExit(2)


if __name__ == "__main__":
    main()
