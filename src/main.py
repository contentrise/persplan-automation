# main.py
import argparse
import sys
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright

import src.config as config
from src.login import do_login
from src.schichten import open_schichtplan
from src.mitarbeiter_loop import loop_all_mitarbeiter
from src.anfragen_parser import extract_anfragen
from src.mitarbeiteranlage import open_mitarbeiteranlage  # ✅ neu
from src.schicht_bestaetigen import run_schicht_bestaetigen
from src.kleidungsrueckgabe import run_kleidungsrueckgabe
from src.tagesplan_vortag import run_tagesplan_vortag
from src.planung_zeitraum import run_planung_zeitraum
from src.kunden_scraper import run_kunden_scraper


def run_login(save_state: str | None, headless: bool | None, slowmo_ms: int | None):
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    config.assert_env_ready()
    state_path = save_state or config.STATE_PATH
    Path(state_path).parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        except Exception as e:
            print(f"[FEHLER] Browser konnte nicht gestartet werden: {e}")
            print("💡 Versuch: playwright install chromium")
            sys.exit(1)

        context = browser.new_context()
        page = context.new_page()

        try:
            do_login(page)
            context.storage_state(path=state_path)
            print(f"[OK] Login erfolgreich. Session-State gespeichert unter: {state_path}")
        except Exception as e:
            print(f"[FEHLER] {e}", file=sys.stderr)
            raise
        finally:
            browser.close()


def run_planung(headless: bool | None, slowmo_ms: int | None):
    """
    Startet Browser mit gespeicherter Session, öffnet den Schichtplan
    und iteriert über alle Mitarbeiter auf der Staffing-Seite.
    """
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    state_path = config.STATE_PATH
    if not Path(state_path).exists():
        print(f"[FEHLER] Kein gespeicherter Login-State unter {state_path}. Bitte zuerst 'login' ausführen.")
        sys.exit(1)

    export_dir = Path("exports")
    export_dir.mkdir(parents=True, exist_ok=True)

    csv_path = export_dir / f"anfragen_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        context = browser.new_context(storage_state=state_path)
        page = context.new_page()

        print("[INFO] Lade Startseite mit gespeicherter Session …")
        page.goto(config.BASE_URL, wait_until="load")

        try:
            open_schichtplan(page)
            print("[INFO] Starte Verarbeitung aller Mitarbeiter …")
            loop_all_mitarbeiter(page, str(csv_path))
            print(f"[OK] Alle Mitarbeiter verarbeitet. Ergebnisse gespeichert unter: {csv_path}")

        except Exception as e:
            print(f"[FEHLER] {e}")
        finally:
            print("[INFO] Browser wird geschlossen …")
            browser.close()


def run_mitarbeiteranlage(headless: bool | None, slowmo_ms: int | None):
    """
    Öffnet den Administrationsbereich und wechselt in die Mitarbeiteransicht.
    """
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    state_path = config.STATE_PATH
    if not Path(state_path).exists():
        print(f"[FEHLER] Kein gespeicherter Login-State unter {state_path}. Bitte zuerst 'login' ausführen.")
        sys.exit(1)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        context = browser.new_context(storage_state=state_path)
        page = context.new_page()

        print("[INFO] Lade Startseite mit gespeicherter Session …")
        page.goto(config.BASE_URL, wait_until="load")

        try:
            open_mitarbeiteranlage(page)
            print("[OK] Mitarbeiteranlage geöffnet.")
        except Exception as e:
            print(f"[FEHLER] {e}")
        finally:
            print("[INFO] Browser wird geschlossen …")
            browser.close()


def run_kunden(headless: bool | None, slowmo_ms: int | None, max_customers: int):
    """
    Öffnet kunden.php und klickt den Firmennamen in den gewünschten Zeilen.
    """
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    export_dir = Path(config.EXPORT_DIR)
    export_dir.mkdir(parents=True, exist_ok=True)
    csv_path = export_dir / "kunden_details.csv"

    state_path = config.STATE_PATH
    if not Path(state_path).exists():
        print(f"[FEHLER] Kein gespeicherter Login-State unter {state_path}. Bitte zuerst 'login' ausführen.")
        sys.exit(1)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        context = browser.new_context(storage_state=state_path)
        page = context.new_page()

        print("[INFO] Lade Startseite mit gespeicherter Session …")
        page.goto(config.BASE_URL, wait_until="load")

        try:
            csv_file = run_kunden_scraper(
                page,
                max_customers=max_customers,
                csv_path=csv_path,
                timestamp=run_timestamp,
            )
            print(f"[OK] Kunden-Scraper abgeschlossen. Ergebnis: {csv_file}")
        except Exception as e:
            print(f"[FEHLER] {e}")
        finally:
            print("[INFO] Browser wird geschlossen …")
            browser.close()


def main():
    parser = argparse.ArgumentParser(description="Persplan Automatisierung")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # --- login ---
    p_login = sub.add_parser("login", help="Nur einloggen und Session-State speichern")
    p_login.add_argument("--save-state", default=None)
    p_login.add_argument("--headless", choices=["true", "false"], default=None)
    p_login.add_argument("--slowmo", type=int, default=None)

    # --- planung ---
    p_plan = sub.add_parser(
        "planung",
        help="Öffnet den Schichtplan, iteriert über alle Mitarbeiter und exportiert Anfragen in CSV"
    )
    p_plan.add_argument("--headless", choices=["true", "false"], default=None)
    p_plan.add_argument("--slowmo", type=int, default=None)

    p_kunden = sub.add_parser(
        "kunden",
        help="Öffnet kunden.php und klickt auf den Firmennamen in der Tabelle"
    )
    p_kunden.add_argument("--headless", choices=["true", "false"], default=None)
    p_kunden.add_argument("--slowmo", type=int, default=None)
    p_kunden.add_argument(
        "--max-customers",
        type=int,
        default=0,
        help="Wie viele Kundenzeilen verarbeitet werden (0 = alle)",
    )

    # --- mitarbeiteranlage ---
    p_admin = sub.add_parser(
        "mitarbeiteranlage",
        help="Öffnet den Administrationsbereich und die Mitarbeiteransicht"
    )
    p_admin.add_argument("--headless", choices=["true", "false"], default=None)
    p_admin.add_argument("--slowmo", type=int, default=None)

    p_sb = sub.add_parser(
        "schicht-bestaetigen",
        help="Öffnet die Startseite und klickt auf 'Tagesplan (alt)'"
    )
    p_sb.add_argument("--headless", choices=["true", "false"], default=None)
    p_sb.add_argument("--slowmo", type=int, default=None)

    p_kleid = sub.add_parser(
        "kleidungsrueckgabe",
        help="Öffnet user.php, filtert auf Ausgeschiedene und sammelt Kontaktinfos"
    )
    p_kleid.add_argument("--headless", choices=["true", "false"], default=None)
    p_kleid.add_argument("--slowmo", type=int, default=None)

    p_tagesplan = sub.add_parser(
        "tagesplan-vortag",
        help="Lädt den Tagesplan (alt), setzt beide Datumsfelder auf gestern und zeigt das Ergebnis an",
    )
    p_tagesplan.add_argument("--headless", choices=["true", "false"], default=None)
    p_tagesplan.add_argument("--slowmo", type=int, default=None)
    p_tagesplan.add_argument("--wait-seconds", type=int, default=5, help="Pause nach Export (Sek.)")
    p_tagesplan.add_argument(
        "--days-back",
        type=int,
        default=1,
        help="Wieviele Tage zurückgesetzt werden sollen (1 = gestern)",
    )
    p_tagesplan.add_argument(
        "--date",
        type=str,
        default=None,
        help="Optionales fixes Datum im Format TT.MM.JJJJ (überschreibt days-back)",
    )

    p_planung_zeitraum = sub.add_parser(
        "planung-zeitraum",
        help="Öffnet planung.php, setzt den Zeitraum auf heute bis in N Tagen und zeigt die Ansicht an",
    )
    p_planung_zeitraum.add_argument("--headless", choices=["true", "false"], default=None)
    p_planung_zeitraum.add_argument("--slowmo", type=int, default=None)
    p_planung_zeitraum.add_argument(
        "--days-forward",
        type=int,
        default=21,
        help="Wie viele Tage ab heute als 'bis'-Datum gesetzt werden sollen (Default: 21)",
    )
    p_planung_zeitraum.add_argument(
        "--wait-seconds",
        type=int,
        default=5,
        help="Wie lange nach dem Anwenden des Filters gewartet werden soll",
    )

    args = parser.parse_args()

    headless = None if args.headless is None else (args.headless.lower() == "true")

    if args.cmd == "login":
        run_login(save_state=args.save_state, headless=headless, slowmo_ms=args.slowmo)

    elif args.cmd == "planung":
        run_planung(headless=headless, slowmo_ms=args.slowmo)

    elif args.cmd == "kunden":
        run_kunden(headless=headless, slowmo_ms=args.slowmo, max_customers=args.max_customers)

    elif args.cmd == "mitarbeiteranlage":
        run_mitarbeiteranlage(headless=headless, slowmo_ms=args.slowmo)

    elif args.cmd == "schicht-bestaetigen":
        run_schicht_bestaetigen(headless=headless, slowmo_ms=args.slowmo)
    elif args.cmd == "kleidungsrueckgabe":
        run_kleidungsrueckgabe(headless=headless, slowmo_ms=args.slowmo)
    elif args.cmd == "tagesplan-vortag":
        run_tagesplan_vortag(
            headless=headless,
            slowmo_ms=args.slowmo,
            hold_seconds=args.wait_seconds,
            days_back=args.days_back,
            explicit_date=args.date,
        )
    elif args.cmd == "planung-zeitraum":
        run_planung_zeitraum(
            headless=headless,
            slowmo_ms=args.slowmo,
            days_forward=args.days_forward,
            hold_seconds=args.wait_seconds,
        )


if __name__ == "__main__":
    main()
