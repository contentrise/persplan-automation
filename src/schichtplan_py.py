"""Dienstplan-Scraper: gleicher Flow wie der Anfragen-Phraser, aber im Tab "Dienstpläne"."""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

from src import config
from src.schichten import open_schichtplan
from src.mitarbeiter_loop import loop_all_mitarbeiter


def run_schichtplan(headless: bool | None = None, slowmo_ms: int | None = None):
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    state_path = config.STATE_PATH
    if not Path(state_path).exists():
        print(f"[FEHLER] Kein gespeicherter Login-State unter {state_path}. Bitte zuerst 'login' ausführen.")
        sys.exit(1)

    export_dir = Path(config.EXPORT_DIR or "exports")
    export_dir.mkdir(parents=True, exist_ok=True)
    csv_path = export_dir / f"dienstplaene_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
        context = browser.new_context(storage_state=state_path)
        page = context.new_page()

        print("[INFO] Lade Startseite mit gespeicherter Session …")
        page.goto(config.BASE_URL, wait_until="load")

        try:
            open_schichtplan(page)
            print("[INFO] Starte Dienstplan-Scraper …")
            loop_all_mitarbeiter(page, str(csv_path), view="dienstplan")
            print(f"[OK] Alle Mitarbeiter verarbeitet. Ergebnisse gespeichert unter: {csv_path}")
        except Exception as e:
            print(f"[FEHLER] {e}")
        finally:
            print("[INFO] Browser wird geschlossen …")
            browser.close()


def main():
    parser = argparse.ArgumentParser(description="Dienstplan-Scraper (schichtplan_py)")
    parser.add_argument("--headless", choices=["true", "false"], default=None)
    parser.add_argument("--slowmo", type=int, default=None)
    args = parser.parse_args()

    headless = None if args.headless is None else (args.headless.lower() == "true")
    run_schichtplan(headless=headless, slowmo_ms=args.slowmo)


if __name__ == "__main__":
    main()

