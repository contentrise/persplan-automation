import csv
from pathlib import Path
from datetime import datetime


EXPORT_DIR = Path("exports")
OUTPUT_TEMPLATE = "abgleich_jobundanfrage_{timestamp}.csv"
FIELDNAMES = [
    "typ",
    "datum",
    "veranstaltung",
    "uhrzeit",
    "beschreibung",
    "eingeplant",
    "mitarbeiter",
    "personalnummer",
    "telefon",
    "kommentar",
]


def _find_latest(prefix: str) -> Path:
    candidates = sorted(EXPORT_DIR.glob(f"{prefix}*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"Keine Datei mit Präfix '{prefix}' in {EXPORT_DIR} gefunden.")
    return candidates[0]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def _categorize(rows: list[dict[str, str]], active_types: set[str]) -> tuple[set[str], dict[str, dict[str, str]]]:
    active = set()
    fallback = {}
    for row in rows:
        name = (row.get("mitarbeiter") or "").strip()
        if not name:
            continue
        typ = (row.get("typ") or "").strip().lower()
        if typ in active_types:
            active.add(name)
        fallback.setdefault(name, row)
    return active, fallback


def main():
    EXPORT_DIR.mkdir(exist_ok=True)

    anfragen_path = _find_latest("anfragen_")
    dienst_path = _find_latest("dienstplaene_")

    anfragen_rows = _read_csv(anfragen_path)
    dienst_rows = _read_csv(dienst_path)

    anfrage_active, anfrage_any = _categorize(
        anfragen_rows,
        active_types={"anfrage", "urlaub", "schicht"},
    )
    dienst_active, dienst_any = _categorize(
        dienst_rows,
        active_types={"dienst"},
    )

    kandidaten = []
    all_names = set(anfrage_any.keys()) | set(dienst_any.keys())
    for name in sorted(all_names):
        if name in anfrage_active or name in dienst_active:
            continue

        base_row = anfrage_any.get(name) or dienst_any.get(name) or {}
        row = {field: base_row.get(field, "") for field in FIELDNAMES}
        if not row["typ"]:
            row["typ"] = "Keine Anfragen"
        if not row["beschreibung"]:
            row["beschreibung"] = "Keine Anfragen oder Dienste gefunden"
        row["mitarbeiter"] = name
        kandidaten.append(row)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = EXPORT_DIR / OUTPUT_TEMPLATE.format(timestamp=timestamp)

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(kandidaten)

    print(f"[OK] Verglichen: {anfragen_path.name} vs. {dienst_path.name}")
    print(f"[OK] {len(kandidaten)} Mitarbeitende ohne Anfragen & Dienste → {output_path}")


if __name__ == "__main__":
    main()

