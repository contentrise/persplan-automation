import argparse
import csv
import time
from datetime import datetime
from pathlib import Path
from typing import Union
from urllib.parse import urljoin

from playwright.sync_api import Frame, Page, TimeoutError, sync_playwright

from src import config
from src.login import do_login


FILTER_PATH = "user.php?filter_anfangsbuchstabe=*&filter_aktive_mitarbeiter=1"
TABLE_WRAPPER_SELECTOR = "#scn_datatable_outer_table_user_tbl"
ROW_SELECTORS = [
    f"{TABLE_WRAPPER_SELECTOR} table#user_tbl tbody tr",
    f"{TABLE_WRAPPER_SELECTOR} tbody tr",
    "table#user_tbl tbody tr",
    "#user_tbl tbody tr",
]
EXPORT_FIELDS = [
    "captured_at",
    "name",
    "phone",
    "email",
    "profile_url",
    "absage_datum",
    "absage_text",
    "absage_eingetragen_von",
]
EXPORT_TEMPLATE = "absagen_{timestamp}.csv"
DEFAULT_LIMIT = 20


def _wait_for_inhalt_frame(page: Page, timeout_seconds: int = 5) -> Frame | None:
    """Wartet kurz auf den optionalen Frame 'inhalt'."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        frame = page.frame(name="inhalt")
        if frame:
            return frame
        time.sleep(0.2)
    return None


def _open_user_table(page: Page) -> Union[Frame, Page]:
    """Lädt user.php mit Filtern und liefert Page bzw. Frame für weitere Interaktionen."""
    target_url = urljoin(config.BASE_URL, FILTER_PATH)
    print(f"[INFO] Öffne Benutzerliste mit Filtern: {target_url}")
    page.goto(target_url, wait_until="domcontentloaded", timeout=30000)

    frame = _wait_for_inhalt_frame(page)
    target: Union[Frame, Page] = frame if frame else page

    if frame:
        # Falls Frames aktiv sind, sicher über den Frame laden.
        frame.goto(target_url, wait_until="domcontentloaded", timeout=30000)

    target.wait_for_selector(TABLE_WRAPPER_SELECTOR, timeout=20000)
    target.wait_for_selector(f"{TABLE_WRAPPER_SELECTOR} tr", timeout=20000)
    print("[INFO] Tabelle geladen – prüfe Zeilen …")
    return target


def _locate_rows(target: Union[Frame, Page]):
    """Versucht mehrere Selektoren, um Tabellenzeilen zu finden."""
    for selector in ROW_SELECTORS:
        locator = target.locator(selector)
        if locator.count() > 0:
            return locator
    return target.locator(f"{TABLE_WRAPPER_SELECTOR} tr")


def _collect_employee_entry(row) -> dict[str, str] | None:
    """Liest Name, Telefon, E-Mail und Link aus der Tabellenzeile."""
    link = row.locator("a.ma_akte_link_text, a.ma_akte_link_img, a").first
    if link.count() == 0:
        return None

    entry = {"name": "", "phone": "", "email": "", "profile_url": ""}
    try:
        tds = row.locator("td")
        first_name = ""
        if tds.count() > 2:
            first_name = tds.nth(2).inner_text().strip()
        last_name_loc = row.locator("a.ma_akte_link_text").first
        last_name = last_name_loc.inner_text().strip() if last_name_loc.count() > 0 else ""
        parts = [p for p in (first_name, last_name) if p]
        entry["name"] = " ".join(parts) if parts else (last_name or first_name)
    except Exception:
        pass

    phone_link = row.locator("a[href^='tel:']").first
    if phone_link.count() > 0:
        try:
            entry["phone"] = phone_link.inner_text().strip() or (phone_link.get_attribute("href") or "")
        except Exception:
            entry["phone"] = phone_link.get_attribute("href") or ""

    email_link = row.locator("a[href^='mailto:']").first
    if email_link.count() > 0:
        try:
            entry["email"] = email_link.inner_text().strip() or (email_link.get_attribute("href") or "")
        except Exception:
            entry["email"] = email_link.get_attribute("href") or ""

    href = link.get_attribute("href") or ""
    entry["profile_url"] = urljoin(config.BASE_URL, href) if href else ""
    return entry


def _build_export_rows(entry: dict[str, str], absagen: list[dict[str, str]], captured_at: str) -> list[dict[str, str]]:
    if not absagen:
        absagen = [{"absage_datum": "", "absage_text": "Keine Absagen", "absage_eingetragen_von": ""}]
    rows = []
    for absage in absagen:
        rows.append(
            {
                "captured_at": captured_at,
                **entry,
                "absage_datum": absage.get("absage_datum", ""),
                "absage_text": absage.get("absage_text", ""),
                "absage_eingetragen_von": absage.get("absage_eingetragen_von", ""),
            }
        )
    return rows


def _write_export(rows: list[dict[str, str]], timestamp_file: str):
    """Schreibt die Daten in eine CSV im exports-Ordner."""
    export_dir = Path(config.EXPORT_DIR)
    export_dir.mkdir(parents=True, exist_ok=True)

    path = export_dir / EXPORT_TEMPLATE.format(timestamp=timestamp_file)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EXPORT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"[OK] Export gespeichert: {path}")


def _collect_employee_entries(target: Union[Frame, Page], limit: int | None) -> list[dict[str, str]]:
    """Liest die ersten n Mitarbeiterzeilen aus und liefert Basisdaten samt Profil-Link."""
    rows = _locate_rows(target)
    total = rows.count()
    if total == 0:
        raise RuntimeError("Keine Mitarbeiterreihen gefunden.")

    if limit is None or limit < 0:
        effective_limit = total
    else:
        effective_limit = min(limit, total)

    print(f"[INFO] Bearbeite {effective_limit} von {total} Reihen …")
    entries: list[dict[str, str]] = []
    for i in range(effective_limit):
        row = rows.nth(i)
        entry = _collect_employee_entry(row)
        if not entry or not entry.get("profile_url"):
            print(f"[WARNUNG] Überspringe Reihe {i+1}, kein gültiger Link gefunden.")
            continue
        entries.append(entry)
    return entries


def _navigate_to_mitarbeiterinformationen(page: Page) -> Page:
    """Öffnet den Tab 'Mitarbeiterinformationen'."""
    frame = _wait_for_inhalt_frame(page, timeout_seconds=2)
    target: Union[Frame, Page] = frame if frame else page

    link = target.locator("#tableOfSubmenue a", has_text="Mitarbeiterinformationen").first
    if link.count() == 0:
        raise RuntimeError("Link 'Mitarbeiterinformationen' nicht gefunden.")

    href = link.get_attribute("href") or ""
    print("[AKTION] Öffne Tab 'Mitarbeiterinformationen' …")
    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
            link.click()
        return page
    except TimeoutError:
        print("[INFO] Navigation hat keinen Seitenwechsel ausgelöst – prüfe Inhalt …")
    except Exception as exc:
        print(f"[WARNUNG] Klick auf 'Mitarbeiterinformationen' fehlgeschlagen: {exc}")

    if href:
        target_url = urljoin(config.BASE_URL, href)
        print(f"[INFO] Fallback: direktes Laden {target_url}")
        target.goto(target_url, wait_until="domcontentloaded", timeout=15000)
    return page


def _extract_absagen(page: Page) -> list[dict[str, str]]:
    """Liest die Absagen-Tabelle aus."""
    frame = _wait_for_inhalt_frame(page, timeout_seconds=2)
    target: Union[Frame, Page] = frame if frame else page
    try:
        target.wait_for_selector("#absagen_datatable", timeout=15000)
    except Exception:
        print("[WARNUNG] Tabelle 'Absagen' nicht gefunden – markiere als keine Absagen.")
        return [{"absage_datum": "", "absage_text": "Keine Absagen", "absage_eingetragen_von": ""}]

    rows = target.locator("#absagen_datatable tbody tr")
    count = rows.count()
    entries: list[dict[str, str]] = []
    for i in range(count):
        row = rows.nth(i)
        tds = row.locator("td")
        td_count = tds.count()
        if td_count == 0:
            text = row.inner_text().strip()
            if text:
                entries.append({"absage_datum": "", "absage_text": text, "absage_eingetragen_von": ""})
            continue
        if td_count < 3:
            text = row.inner_text().strip() or "Keine Absagen"
            entries.append({"absage_datum": "", "absage_text": text, "absage_eingetragen_von": ""})
            continue
        entries.append(
            {
                "absage_datum": tds.nth(0).inner_text().strip(),
                "absage_text": tds.nth(1).inner_text().strip(),
                "absage_eingetragen_von": tds.nth(2).inner_text().strip(),
            }
        )
    if not entries:
        entries.append({"absage_datum": "", "absage_text": "Keine Absagen", "absage_eingetragen_von": ""})
    return entries


def _process_employee(entry: dict[str, str], context, timeout: int = 30000) -> list[dict[str, str]]:
    """Öffnet die Mitarbeiterakte, wechselt zum Tab und liest Absagen aus."""
    profile_url = entry.get("profile_url")
    if not profile_url:
        print("[WARNUNG] Kein Profil-Link vorhanden – überspringe.")
        return [{"absage_datum": "", "absage_text": "Keine Absagen", "absage_eingetragen_von": ""}]

    page = context.new_page()
    print(f"[INFO] Öffne Mitarbeiterakte: {profile_url}")
    page.goto(profile_url, wait_until="domcontentloaded", timeout=timeout)
    info_page = _navigate_to_mitarbeiterinformationen(page)
    absagen = _extract_absagen(info_page)
    page.close()
    return absagen


def run_absagen(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    hold_seconds: float = 5.0,
    max_rows: int | None = DEFAULT_LIMIT,
):
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms
    state_path = Path(config.STATE_PATH)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms or 0)
        context = None
        page = None

        if state_path.exists():
            try:
                print(f"[INFO] Verwende gespeicherten Login-State aus {state_path}")
                context = browser.new_context(storage_state=str(state_path))
                page = context.new_page()
                target = _open_user_table(page)
            except Exception as exc:
                print(f"[WARNUNG] Gespeicherter State ungültig ({exc}) – führe Login erneut durch.")
                if context:
                    context.close()
                context = None
                page = None
                target = None
        else:
            target = None

        if page is None or target is None:
            print("[INFO] Starte manuellen Login …")
            page = browser.new_page()
            do_login(page)
            target = _open_user_table(page)
            context = page.context
        else:
            context = page.context

        limit_desc = "alle" if (max_rows is None or max_rows < 0) else str(max_rows)
        print(f"[INFO] Angeforderte Anzahl Reihen: {limit_desc}")

        entries = _collect_employee_entries(target, max_rows)
        if not entries:
            print("[INFO] Keine gültigen Einträge gefunden – nichts zu exportieren.")
            browser.close()
            return

        captured_ts = datetime.now()
        captured_at = captured_ts.strftime("%Y-%m-%d %H:%M:%S")
        ts_file = captured_ts.strftime("%Y-%m-%d_%H-%M-%S")
        export_rows: list[dict[str, str]] = []

        for idx, entry in enumerate(entries, start=1):
            print(f"\n--- Mitarbeiter {idx}/{len(entries)}: {entry.get('name') or entry.get('profile_url')}")
            absagen_entries = _process_employee(entry, context)
            export_rows.extend(_build_export_rows(entry, absagen_entries, captured_at))

        _write_export(export_rows, ts_file)
        print(f"[INFO] Halte Seite {hold_seconds} Sekunden offen …")
        time.sleep(max(0.0, hold_seconds))
        print("[OK] Fertig – Browser schließen.")
        browser.close()


def main():
    parser = argparse.ArgumentParser(description="Öffnet user.php und protokolliert Absagen für mehrere Mitarbeitende.")
    parser.add_argument("--headless", choices=["true", "false"], default=None, help="Playwright headless-Modus überschreiben.")
    parser.add_argument("--slowmo", type=int, default=None, help="Playwright slow_mo in Millisekunden.")
    parser.add_argument("--hold", type=float, default=5.0, help="Pause nach dem Klick (Sekunden).")
    parser.add_argument(
        "--limit",
        type=int,
        default=-1,
        help="Anzahl Mitarbeitender (Standard: 20, -1 oder leer = alle).",
    )
    args = parser.parse_args()

    headless = None
    if args.headless is not None:
        headless = args.headless.lower() == "true"

    run_absagen(
        headless=headless,
        slowmo_ms=args.slowmo,
        hold_seconds=args.hold,
        max_rows=args.limit,
    )


if __name__ == "__main__":
    main()
