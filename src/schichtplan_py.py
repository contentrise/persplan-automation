"""Dienstplan-Scraper: gleicher Flow wie der Anfragen-Phraser, aber im Tab "Dienstpläne"."""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import boto3
from playwright.sync_api import sync_playwright

from src import config
from src.schichten import open_schichtplan
from src.mitarbeiter_loop import loop_all_mitarbeiter

S3_BUCKET = os.getenv("S3_BUCKET", "greatstaff-data-storage")
S3_PREFIX = os.getenv("DIENSTPLAN_S3_PREFIX", "staffing/dienstplan")


def _resolve_month_year(
    month_override: int | None, year_override: int | None, month_offset: int
) -> tuple[int, int]:
    """Ermittelt den Zielmonat und das Zieljahr für den Lauf."""
    now = datetime.now()
    if month_override is not None and not (1 <= month_override <= 12):
        raise ValueError("--month muss zwischen 1 und 12 liegen.")

    if month_override is not None:
        target_month = month_override
        target_year = year_override if year_override is not None else now.year
    elif year_override is not None:
        target_month = now.month
        target_year = year_override
    else:
        target_month = now.month
        target_year = now.year
        if month_offset:
            target_month += month_offset
            while target_month > 12:
                target_month -= 12
                target_year += 1
            while target_month < 1:
                target_month += 12
                target_year -= 1

    return target_month, target_year


def _override_config_month_year(month: int, year: int) -> None:
    """Schreibt Monat/Jahr in das globale config-Modul, damit alle Parser sie nutzen."""
    config.MONTH = int(month)
    config.YEAR = str(year)


def upload_dienstplan_to_s3(path: Path | None, month: int, year: int) -> None:
    """Lädt die CSV optional nach S3 in einen Monatsordner unterhalb des Prefix."""
    if path is None or not path.exists():
        return

    if not S3_BUCKET:
        print("[INFO] Kein S3_BUCKET konfiguriert – Upload übersprungen.")
        return

    prefix = S3_PREFIX.strip().strip("/")
    month_folder = f"{year}-{int(month):02d}"
    key_parts = [part for part in [prefix, month_folder, path.name] if part]
    key = "/".join(key_parts)

    s3 = boto3.client("s3")
    try:
        s3.upload_file(str(path), S3_BUCKET, key)
        print(f"[OK] CSV nach S3 hochgeladen: s3://{S3_BUCKET}/{key}")
    except Exception as exc:
        print(f"[WARNUNG] Upload nach S3 fehlgeschlagen: {exc}")


def run_schichtplan(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    target_month: int | None = None,
    target_year: int | None = None,
    month_offset: int = 0,
):
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    month, year = _resolve_month_year(target_month, target_year, month_offset)
    print(f"[INFO] Nutze Ziel-Monat/-Jahr: {month:02d}/{year}")
    _override_config_month_year(month, year)

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
            upload_dienstplan_to_s3(csv_path, month, year)
        except Exception as e:
            print(f"[FEHLER] {e}")
        finally:
            print("[INFO] Browser wird geschlossen …")
            browser.close()


def main():
    parser = argparse.ArgumentParser(description="Dienstplan-Scraper (schichtplan_py)")
    parser.add_argument("--headless", choices=["true", "false"], default=None)
    parser.add_argument("--slowmo", type=int, default=None)
    parser.add_argument("--month", type=int, help="Monat (1-12), überschreibt automatische Berechnung.")
    parser.add_argument("--year", type=int, help="Jahr (z. B. 2024), überschreibt automatische Berechnung.")
    parser.add_argument(
        "--month-offset",
        type=int,
        default=0,
        help="Monats-Offset relativ zum aktuellen Monat (0 = aktueller Monat, 1 = nächster Monat).",
    )
    args = parser.parse_args()

    headless = None if args.headless is None else (args.headless.lower() == "true")
    run_schichtplan(
        headless=headless,
        slowmo_ms=args.slowmo,
        target_month=args.month,
        target_year=args.year,
        month_offset=args.month_offset,
    )


if __name__ == "__main__":
    main()
