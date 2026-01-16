import argparse
import calendar
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Union
from urllib.parse import urljoin

from playwright.sync_api import Frame, Page, TimeoutError, sync_playwright

from src import config
from src.login import do_login


def _wait_for_inhalt_frame(page: Page, timeout_seconds: int = 5) -> Frame | None:
    """
    Sucht kurz nach dem Frame 'inhalt'. Kurzer Timeout, weil user.php meist ohne Frames lädt.
    """
    existing = page.frame(name="inhalt")
    if existing:
        return existing

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        frame = page.frame(name="inhalt")
        if frame:
            return frame
        time.sleep(0.2)
    return None


def _load_flow(flow_path: Path) -> dict:
    """Liest die Flow-Datei ein."""
    if not flow_path.exists():
        raise FileNotFoundError(f"[FEHLER] Flow-Datei nicht gefunden: {flow_path}")

    with flow_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("[FEHLER] Flow-JSON hat unerwartetes Format.")
    return data


def _load_contact(flow_data: dict) -> dict:
    """Liest die Kontakt-Infos (firstName, lastName, phone, email) aus der Flow-Datenstruktur."""
    contact = flow_data.get("contact", {}) if isinstance(flow_data, dict) else {}
    if not contact:
        raise ValueError("[FEHLER] Kein contact-Block in der Flow-Datei gefunden.")
    return contact


def _format_amount(amount: float) -> str:
    """Formatiert eine Zahl im deutschen Komma-Format mit zwei Nachkommastellen."""
    return f"{amount:0.2f}".replace(".", ",")


def _normalize_negative(amount: float) -> float:
    """Stellt sicher, dass der Ansatz als negativer Wert eingetragen wird."""
    return -abs(float(amount))


def _navigate_to_zulagen(page: Page) -> None:
    """Klickt im geöffneten Mitarbeiter-Profil auf 'Zulagen'."""
    # Falls doch Frames genutzt werden
    frame = _wait_for_inhalt_frame(page, timeout_seconds=2)
    target: Union[Frame, Page] = frame if frame else page

    link = target.locator("a", has_text="Zulagen").first
    if link.count() == 0:
        # Fallback auf ID/Tabellenstruktur
        link = target.locator("#tableOfSubmenue a", has_text="Zulagen").first
    if link.count() == 0:
        print("[WARNUNG] 'Zulagen'-Link nicht gefunden.")
        return

    href = link.get_attribute("href") or ""
    print("[AKTION] Öffne Tab 'Zulagen' …")
    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=12000):
            link.click()
        return
    except TimeoutError:
        pass
    except Exception as exc:
        print(f"[WARNUNG] Klick auf 'Zulagen' fehlgeschlagen: {exc}")

    if href:
        target_url = urljoin(config.BASE_URL, href)
        print(f"[INFO] Fallback: öffne direkt {target_url}")
        page.goto(target_url, wait_until="domcontentloaded", timeout=15000)

    try:
        page.wait_for_selector("table", timeout=7000)
    except Exception:
        pass


def _fill_zulage_form(
    target: Union[Frame, Page],
    bezeichnung: str,
    bemerkung: str,
    ansatz: float,
    lohnart: str,
    last_day_str: str,
) -> None:
    """Füllt das Zulage-Formular mit den gegebenen Daten."""
    target.fill("#bezeichnung", bezeichnung)
    target.fill("#bemerkung", bemerkung)
    target.fill("#ansatz", _format_amount(ansatz))
    target.select_option("#rhythmus", "einmalig")
    target.select_option("#lohnart", lohnart)
    target.fill("#gueltig_bis", last_day_str)


def _click_zulage_hinzufuegen(
    page: Page,
    bezeichnung: str,
    bemerkung: str,
    ansatz: float,
    lohnart: str,
    last_day_str: str,
) -> None:
    """Klickt auf 'Zulage hinzufügen', füllt das Formular und stoppt kurz."""
    frame = _wait_for_inhalt_frame(page, timeout_seconds=2)
    target: Union[Frame, Page] = frame if frame else page

    btn = target.locator("button.pointer:has-text('Zulage hinzufügen'), #mitarbeiter_zulagen_wrapper button.pointer").first
    if btn.count() == 0:
        print("[WARNUNG] Button 'Zulage hinzufügen' nicht gefunden.")
        return

    print("[AKTION] Klicke 'Zulage hinzufügen' …")
    try:
        btn.click()
    except Exception as exc:
        print(f"[WARNUNG] Klick auf 'Zulage hinzufügen' fehlgeschlagen: {exc}")
        return

    try:
        target.wait_for_selector("#mitarbeiter_zulage_form", timeout=8000)
    except Exception:
        print("[WARNUNG] Formular 'mitarbeiter_zulage_form' nicht gefunden.")
        return

    try:
        _fill_zulage_form(target, bezeichnung, bemerkung, ansatz, lohnart, last_day_str)
        print(
            "[OK] Formular gefüllt: Bezeichnung="
            f"{bezeichnung}, Ansatz={ansatz}, Rhythmus=einmalig, Lohnart={lohnart}, gültig bis {last_day_str}"
        )
    except Exception as exc:
        print(f"[WARNUNG] Konnte Formularfelder nicht befüllen: {exc}")

    # Speichern klicken
    save_btn = target.locator("#mitarbeiter_zulage_form button.pointer:has-text('Hinzufügen'), #mitarbeiter_zulage_form button.pointer img.save").first
    try:
        print("[AKTION] Klicke auf 'Hinzufügen' …")
        save_btn.click()
    except Exception as exc:
        print(f"[WARNUNG] Klick auf 'Hinzufügen' fehlgeschlagen: {exc}")

    # Sichtbar lassen
    time.sleep(5)


def _build_queries(contact: dict) -> list[str]:
    """Erzeugt Suchbegriffe aus den verfügbaren Kontaktfeldern (priorisiert)."""
    queries: list[str] = []
    first = (contact.get("firstName") or "").strip()
    last = (contact.get("lastName") or "").strip()
    phone = (contact.get("phone") or "").strip()
    email = (contact.get("email") or "").strip()
    personalnummer = (contact.get("personalnummer") or contact.get("personnelNumber") or "").strip()

    full_name = " ".join(part for part in [first, last] if part).strip()
    phone_clean = "".join(ch for ch in phone if ch.isdigit() or ch == "+")

    # Priorität: Personalnummer (eindeutig) -> Phone -> Email -> Name-Varianten
    candidates = [
        personalnummer,
        phone,
        phone_clean,
        email,
        full_name,
        last,
        first,
    ]

    for candidate in candidates:
        if candidate and candidate not in queries:
            queries.append(candidate)
    return queries


def _open_user_overview(page: Page) -> Union[Frame, Page]:
    """Lädt user.php direkt (ohne Startseite) und liefert das Ziel (Frame oder Page)."""
    target_url = urljoin(config.BASE_URL, "user.php")
    print(f"[INFO] Öffne Benutzerübersicht: {target_url}")
    page.goto(target_url, wait_until="domcontentloaded", timeout=30000)

    frame = _wait_for_inhalt_frame(page)
    target: Union[Frame, Page] = frame if frame else page

    # Falls ein Frame existiert, sicherheitshalber direkt darin laden
    if frame:
        frame.goto(target_url, wait_until="domcontentloaded", timeout=30000)
        target = frame

    # Robust warten, bis die Suche und Tabelle sichtbar sind (etwas länger, Server kann träge sein)
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
    """Findet das Suchfeld mit mehreren Fallbacks."""
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
    return target.locator("input").first  # letzter Fallback


def _search_and_click(
    target: Union[Frame, Page], queries: list[str], delay: float, deadline: float | None = None
) -> Page | None:
    """
    Füllt das Suchfeld mit den gegebenen Queries.
    Bei genau einem Treffer wird geklickt. Liefert die Page, auf der nach dem Klick weitergearbeitet wird.
    """
    if deadline is None:
        deadline = time.time() + 30  # Hard-Timeout für die Suche

    search_input = _locate_search_input(target)
    if search_input.count() == 0:
        raise RuntimeError("[FEHLER] Suchfeld in user.php nicht gefunden.")

    rows = target.locator("table#user_tbl tbody tr")
    parent_page = target.page if isinstance(target, Frame) else target

    for query in queries:
        if time.time() > deadline:
            print("[INFO] Suche abgebrochen (Timeout nach 30s).")
            break
        search_input.fill(query)
        time.sleep(max(0.05, delay))
        match_count = rows.count()
        print(f"[INFO] Suche '{query}' → {match_count} Treffer")

        if match_count == 1:
            row = rows.first
            link = row.locator("a.ma_akte_link_text, a.ma_akte_link_img").first
            if link.count() == 0:
                link = row.locator("a").first
            if link.count() == 0:
                print("[WARNUNG] Kein klickbarer Link in der Trefferzeile gefunden.")
                continue

            print(f"[AKTION] Eindeutiger Treffer für '{query}' – klicke Eintrag …")
            href = link.get_attribute("href") or ""

            # 1) Popups (neuer Tab) abfangen
            try:
                with parent_page.context.expect_page(timeout=5000) as new_page_event:
                    link.click()
                new_page = new_page_event.value
                new_page.wait_for_load_state("domcontentloaded", timeout=15000)
                return new_page
            except TimeoutError:
                pass

            # 2) Navigation im selben Tab
            try:
                with parent_page.expect_navigation(wait_until="domcontentloaded", timeout=12000):
                    link.click()
                return parent_page
            except TimeoutError:
                # 3) Fallback: direkter GET
                if href:
                    try:
                        parent_page.goto(urljoin(config.BASE_URL, href), wait_until="domcontentloaded", timeout=15000)
                        return parent_page
                    except Exception:
                        pass
                # 4) Letzter Versuch: Klick ohne Wait
                link.click()
                return parent_page

    search_input.fill("")
    return None


def _sum_items(flow_data: dict, item_type: str) -> float:
    """Summiert Preise für Items eines bestimmten Typs (z. B. 'sale', 'deposit')."""
    items = flow_data.get("items") if isinstance(flow_data, dict) else None
    if not isinstance(items, list):
        return 0.0
    total = 0.0
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("type") != item_type:
            continue
        try:
            total += float(str(item.get("price", 0)).replace(",", "."))
        except Exception:
            continue
    return total


def run_user_search(
    flow_file: str = "flow (2).json",
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    delay: float = 0.2,
):
    """Öffnet user.php, sucht nach den Flow-Kontaktdaten und klickt bei genau einem Treffer."""
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms
    delay = max(0.05, delay)  # minimale Wartezeit, damit Filter greifen

    state_path = Path(config.STATE_PATH)
    if not state_path.exists():
        raise RuntimeError(f"[FEHLER] Kein gespeicherter Login-State unter {state_path}. Bitte zuerst 'login' ausführen.")

    flow_data = _load_flow(Path(flow_file))
    contact = _load_contact(flow_data)
    deposit_total = flow_data.get("depositTotal", 0) or 0
    if not isinstance(deposit_total, (int, float)):
        try:
            deposit_total = float(str(deposit_total).replace(",", "."))
        except Exception:
            deposit_total = 0
    if not deposit_total:
        deposit_total = _sum_items(flow_data, "deposit")

    sale_total = flow_data.get("saleTotal", 0) or 0
    if not isinstance(sale_total, (int, float)):
        try:
            sale_total = float(str(sale_total).replace(",", "."))
        except Exception:
            sale_total = 0
    if not sale_total:
        sale_total = _sum_items(flow_data, "sale")

    today = datetime.now()
    today_str = today.strftime("%d.%m.%Y")
    last_day = calendar.monthrange(today.year, today.month)[1]
    last_day_str = f"{last_day:02d}.{today.month:02d}.{today.year}"

    queries = _build_queries(contact)
    if not queries:
        raise RuntimeError("[FEHLER] Keine gültigen Suchbegriffe gefunden.")

    print(f"[INFO] Verwende Suchbegriffe: {queries}")
    search_deadline = time.time() + 30  # Gesamttimeout für die Suche

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        context = browser.new_context(storage_state=str(state_path))
        page = context.new_page()

        print("[INFO] Lade Startseite mit gespeicherter Session …")
        page.goto(config.BASE_URL, wait_until="domcontentloaded")

        try:
            # erster Versuch mit gespeichertem State
            frame = _open_user_overview(page)
        except Exception as exc:
            print(f"[WARNUNG] Übersicht nicht geladen (Session evtl. abgelaufen): {exc} – versuche Login …")
            page = browser.new_page()
            do_login(page)
            frame = _open_user_overview(page)

        try:
            result_page = _search_and_click(frame, queries, delay, deadline=search_deadline)
            if result_page:
                print("[OK] Treffer geklickt – navigiere zu 'Zulagen' …")
                _navigate_to_zulagen(result_page)
                print("[OK] Zulagen geöffnet – klicke auf 'Zulage hinzufügen' und befülle Formular …")
                bemerkung = f"Ausgabe am {today_str}"
                if sale_total:
                    _click_zulage_hinzufuegen(
                        result_page,
                        "Verkauf Schuhe",
                        bemerkung,
                        _normalize_negative(sale_total),
                        "91",
                        last_day_str,
                    )
                if deposit_total:
                    _click_zulage_hinzufuegen(
                        result_page,
                        "Servicekleidung",
                        bemerkung,
                        _normalize_negative(deposit_total),
                        "90",
                        last_day_str,
                    )
            else:
                print("[INFO] Kein eindeutiger Treffer – nichts geklickt.")
        finally:
            browser.close()


def main():
    parser = argparse.ArgumentParser(description="Suche user.php mit Flow-Daten")
    parser.add_argument("--flow-file", default="flow (2).json", help="Pfad zur Flow-JSON (enthält contact-Block)")
    parser.add_argument("--headless", choices=["true", "false"], default=None)
    parser.add_argument("--slowmo", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.2, help="Wartezeit nach Setzen des Suchbegriffs (Sekunden)")
    args = parser.parse_args()

    headless = None if args.headless is None else args.headless.lower() == "true"
    run_user_search(
        flow_file=args.flow_file,
        headless=headless,
        slowmo_ms=args.slowmo,
        delay=args.delay,
    )


if __name__ == "__main__":
    main()
