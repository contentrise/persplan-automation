"""
Split PDFs from an input folder into single-page PDFs and place them into
import_vertragsanpassung with a consistent naming scheme.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from PyPDF2 import PdfReader, PdfWriter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Splits PDFs into single pages and stores them as "
            "'Vertragsanpassungen Stunden MM-YYYY-###.pdf'."
        )
    )
    parser.add_argument("--month", required=True, help="Monat (z. B. 12)")
    parser.add_argument("--year", required=True, help="Jahr (z. B. 2025)")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("va-splitter"),
        help="Quelle für die PDFs, die gesplittet werden sollen.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("import_vertragsanpassung"),
        help="Zielordner für die gesplitteten PDFs.",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=1,
        help="Startnummer für die Dateinamen (Standard: 1).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Vorhandene Dateien überschreiben.",
    )
    return parser


def normalize_month(month: str) -> str:
    cleaned = month.strip()
    if cleaned.isdigit():
        return f"{int(cleaned):02d}"
    return cleaned.zfill(2)


def split_pdfs(
    input_dir: Path,
    output_dir: Path,
    month: str,
    year: str,
    start_index: int,
    overwrite: bool,
) -> int:
    if not input_dir.exists():
        raise FileNotFoundError(f"Eingabeordner existiert nicht: {input_dir}")

    pdf_files = sorted(input_dir.glob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError(f"Keine PDFs im Ordner gefunden: {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    index = start_index
    written = 0

    for pdf_path in pdf_files:
        reader = PdfReader(str(pdf_path))
        for page in reader.pages:
            filename = f"Vertragsanpassungen Stunden {month}-{year}-{index:03d}.pdf"
            target_path = output_dir / filename
            if target_path.exists() and not overwrite:
                raise FileExistsError(f"Datei existiert bereits: {target_path}")

            writer = PdfWriter()
            writer.add_page(page)
            with target_path.open("wb") as handle:
                writer.write(handle)

            index += 1
            written += 1

    return written


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    month = normalize_month(args.month)
    year = args.year.strip()

    count = split_pdfs(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        month=month,
        year=year,
        start_index=args.start_index,
        overwrite=args.overwrite,
    )

    print(f"{count} Seiten nach {args.output_dir} geschrieben.")


if __name__ == "__main__":
    main()
