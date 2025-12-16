"""
Erstellt eine CSV mit Metadaten zu allen PDFs im Ordner `import_vertragsanpassung`.

Gespeichert werden pdf_nummer, Vorname, Nachname sowie der am Dokumentende
platzierte Zahlencode. Die Ausgabe landet standardmäßig unter
`export_vertragsanpassung`.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from PyPDF2 import PdfReader


FIELDNAMES = ["pdf_nummer", "vorname", "nachname", "dokument_code"]
NAME_HINT_PATTERN = re.compile(r"^(herrn?|frau)\b", re.IGNORECASE)
NAME_FALLBACK_PATTERN = re.compile(
    r"([A-Za-zÄÖÜäöüß' -]{2,}),\s*([A-Za-zÄÖÜäöüß' -]{2,})"
)
LONG_NUMBER_PATTERN = re.compile(r"(?<!\d)(?:\d[\s-]?){11,}(?!\d)")


@dataclass
class ExtractionResult:
    pdf_nummer: str
    vorname: str
    nachname: str
    dokument_code: str
    pdf_name: str


def extract_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    parts: list[str] = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    return "\n".join(parts)


def sanitize_token(value: str) -> str:
    token = value.replace("\u200b", "").strip()
    token = re.sub(r"\s{2,}", " ", token)
    return token.strip(",;:. ")


def extract_name(text: str) -> tuple[str, str]:
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]

    for idx, line in enumerate(lines):
        if not NAME_HINT_PATTERN.match(line):
            continue
        for candidate in lines[idx + 1 : idx + 5]:
            if "," not in candidate:
                continue
            last, first = [sanitize_token(part) for part in candidate.split(",", 1)]
            if last and first:
                return first, last

    for match in NAME_FALLBACK_PATTERN.finditer(text):
        last = sanitize_token(match.group(1))
        first = sanitize_token(match.group(2))
        if last and first:
            return first, last

    return "", ""


def extract_long_code(text: str) -> str:
    matches = LONG_NUMBER_PATTERN.findall(text)
    if not matches:
        return ""

    code = re.sub(r"[\s-]", "", matches[-1])
    return code


def extract_pdf_number(path: Path) -> str:
    digits = re.findall(r"\d+", path.stem)
    if digits:
        return digits[-1]
    return path.stem


def process_pdf(pdf_path: Path) -> ExtractionResult:
    text = extract_text(pdf_path)
    vorname, nachname = extract_name(text)
    return ExtractionResult(
        pdf_nummer=extract_pdf_number(pdf_path),
        vorname=vorname,
        nachname=nachname,
        dokument_code=extract_long_code(text),
        pdf_name=pdf_path.name,
    )


def ensure_plus_prefix(code: str) -> str:
    if not code:
        return ""
    normalized = code.replace(" ", "")
    if normalized.startswith("+"):
        return normalized
    return f"+{normalized.lstrip('+')}"


def write_csv(rows: Iterable[ExtractionResult], output_path: Path) -> None:
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "pdf_nummer": row.pdf_nummer,
                    "vorname": row.vorname,
                    "nachname": row.nachname,
                    "dokument_code": f"{ensure_plus_prefix(row.dokument_code)} {row.pdf_name}".strip(),
                }
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Liest PDFs aus import_vertragsanpassung und erzeugt eine CSV "
            "mit Name und Dokumentcode."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("import_vertragsanpassung"),
        help="Ordner mit den Vertragsanpassungs-PDFs.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("export_vertragsanpassung"),
        help="Zielordner für die CSV-Datei.",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default="",
        help="Optionale Datei (inkl. .csv). Standard ist ein Timestamp-basierter Name.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    input_dir: Path = args.input_dir
    output_dir: Path = args.output_dir
    output_file: str = args.output_file

    if not input_dir.exists():
        parser.error(f"Eingabeordner {input_dir} existiert nicht.")

    pdf_files = sorted(input_dir.glob("*.pdf"))
    if not pdf_files:
        parser.error(f"Keine PDFs im Ordner {input_dir} gefunden.")

    results: list[ExtractionResult] = []
    for pdf in pdf_files:
        try:
            results.append(process_pdf(pdf))
        except Exception as exc:
            print(f"Fehler beim Lesen von {pdf.name}: {exc}", file=sys.stderr)

    if not results:
        parser.error("Es konnten keine Metadaten extrahiert werden.")

    output_dir.mkdir(parents=True, exist_ok=True)
    if output_file:
        output_path = output_dir / output_file
    else:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_path = output_dir / f"vertragsanpassungen_meta_{timestamp}.csv"

    write_csv(results, output_path)
    print(f"{len(results)} Einträge nach {output_path} geschrieben.")


if __name__ == "__main__":
    main()
