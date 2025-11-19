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

    args = parser.parse_args()

    headless = None if args.headless is None else (args.headless.lower() == "true")

    if args.cmd == "login":
        run_login(save_state=args.save_state, headless=headless, slowmo_ms=args.slowmo)

    elif args.cmd == "planung":
        run_planung(headless=headless, slowmo_ms=args.slowmo)

    elif args.cmd == "mitarbeiteranlage":
        run_mitarbeiteranlage(headless=headless, slowmo_ms=args.slowmo)

    elif args.cmd == "schicht-bestaetigen":
        run_schicht_bestaetigen(headless=headless, slowmo_ms=args.slowmo)
    elif args.cmd == "kleidungsrueckgabe":
        run_kleidungsrueckgabe(headless=headless, slowmo_ms=args.slowmo)


if __name__ == "__main__":
    main()
