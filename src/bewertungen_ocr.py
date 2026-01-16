import argparse
import csv
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import certifi
import easyocr
import numpy as np
import pytesseract
from pdf2image import convert_from_path
from PIL import ImageFilter, ImageOps


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _normalize(text: str) -> str:
    return re.sub(r"[^a-z]", "", text.lower())


def _bbox_from_quad(points: Sequence[Sequence[int]]) -> Tuple[int, int, int, int]:
    xs = [int(p[0]) for p in points]
    ys = [int(p[1]) for p in points]
    return min(xs), min(ys), max(xs), max(ys)


def _read_page(reader: easyocr.Reader, image, min_conf: float) -> List[Dict]:
    np_image = np.array(image)
    words = []
    for box, text, conf in reader.readtext(np_image):
        if conf < min_conf:
            continue
        x0, y0, x1, y1 = _bbox_from_quad(box)
        words.append(
            {
                "text": text.strip(),
                "conf": conf,
                "x0": x0,
                "y0": y0,
                "x1": x1,
                "y1": y1,
                "cx": (x0 + x1) / 2,
                "cy": (y0 + y1) / 2,
            }
        )
    return words


def _detect_headers(words: Sequence[Dict], height: int) -> Dict[str, Dict]:
    header_tokens = {
        "nr": "nr",
        "name": "name",
        "first_name": "vor",
        "function": "funk",
        "remarks": "bemerk",
        "signature": "untersch",
    }
    headers: Dict[str, Dict] = {}
    band_low = 0
    band_high = int(height * 0.5)
    margin_y = int(height * 0.02)
    name_candidates = [
        word for word in words if "name" in _normalize(word["text"]) and word["y0"] < band_high
    ]
    if name_candidates:
        best_name = max(name_candidates, key=lambda w: w["conf"])
        headers["name"] = best_name
        band_low = max(0, best_name["y0"] - margin_y)
        band_high = min(height, best_name["y1"] + margin_y)

    for word in words:
        if not (band_low <= word["y0"] <= band_high):
            continue
        normalized = _normalize(word["text"])
        for key, token in header_tokens.items():
            if token in normalized:
                stored = headers.get(key)
                if stored is None or word["conf"] > stored["conf"]:
                    headers[key] = word
    return headers


def _column_windows(width: int, headers: Dict[str, Dict]) -> Dict[str, Tuple[int, int]]:
    margin = int(width * 0.015)

    def from_header(key: str, fallback: Tuple[float, float]) -> Tuple[int, int]:
        if key not in headers:
            return int(width * fallback[0]), int(width * fallback[1])
        x0 = max(0, headers[key]["x0"] - 2 * margin)
        x1 = min(width, headers[key]["x1"] + margin)
        return x0, x1

    windows: Dict[str, Tuple[int, int]] = {}
    default_ranges = {
        "name": (0.10, 0.32),
        "first_name": (0.32, 0.48),
        "remarks": (0.64, 0.95),
        "row_number": (0.02, 0.09),
    }
    if "nr" in headers:
        windows["row_number"] = (
            max(0, headers["nr"]["x0"] - margin),
            headers["nr"]["x1"] + 2 * margin,
        )
    else:
        windows["row_number"] = (
            int(width * default_ranges["row_number"][0]),
            int(width * default_ranges["row_number"][1]),
        )

    windows["name"] = from_header("name", default_ranges["name"])
    nxt = headers.get("first_name")
    if nxt:
        windows["name"] = (windows["name"][0], max(windows["name"][0] + margin, nxt["x0"] - margin))

    fn_window = from_header("first_name", default_ranges["first_name"])
    nxt = headers.get("function")
    if nxt:
        fn_window = (fn_window[0], max(fn_window[0] + margin, nxt["x0"] - margin))
    left_guard = windows["name"][1] + int(margin * 0.4)
    if fn_window[0] < left_guard:
        fn_window = (left_guard, fn_window[1])
    if fn_window[1] <= fn_window[0]:
        fn_window = (fn_window[0], fn_window[0] + int(width * 0.08))
    windows["first_name"] = fn_window

    remarks_window = from_header("remarks", default_ranges["remarks"])
    nxt = headers.get("signature")
    if nxt:
        remarks_window = (remarks_window[0], max(remarks_window[0] + margin, nxt["x0"] - margin))
    right_guard = width - int(width * 0.03)
    remarks_window = (max(remarks_window[0], windows["first_name"][1] + margin), min(remarks_window[1], right_guard))
    if remarks_window[1] <= remarks_window[0]:
        remarks_window = (remarks_window[0], remarks_window[0] + int(width * 0.15))
    windows["remarks"] = remarks_window
    return windows


def _find_section_word(words: Sequence[Dict], tokens: Sequence[str]) -> Dict | None:
    best = None
    for word in words:
        normalized = _normalize(word["text"])
        if all(token in normalized for token in tokens):
            if best is None or word["conf"] > best["conf"]:
                best = word
    return best


def _words_in_window(
    words: Sequence[Dict],
    window: Tuple[int, int],
    y_limits: Tuple[int, int],
    allow_digits: bool = True,
) -> List[Dict]:
    left, right = window
    top, bottom = y_limits
    return [
        word
        for word in words
        if top <= word["cy"] <= bottom
        and left <= word["cx"] <= right
        and word["text"].strip()
        and (allow_digits or not word["text"].strip().isdigit())
    ]


def _combine_text(words: Sequence[Dict]) -> str:
    ordered = sorted(words, key=lambda w: w["cx"])
    return _clean_text(" ".join(word["text"] for word in ordered))


def _ocr_remark_image(image, window: Tuple[int, int], y_limits: Tuple[int, int]) -> str:
    left, right = window
    top, bottom = y_limits
    if right - left < 10 or bottom - top < 10:
        return ""
    pad_x = int(image.width * 0.01)
    pad_y = int(image.height * 0.005)
    crop_box = (
        max(0, left - pad_x),
        max(0, top - pad_y),
        min(image.width, right + pad_x),
        min(image.height, bottom + pad_y),
    )
    if crop_box[2] - crop_box[0] < 10 or crop_box[3] - crop_box[1] < 10:
        return ""
    crop = image.crop(crop_box)
    gray = ImageOps.grayscale(crop)
    enhanced = ImageOps.autocontrast(gray)
    denoised = enhanced.filter(ImageFilter.MedianFilter(size=3))
    text = pytesseract.image_to_string(denoised, lang="deu", config="--psm 6")
    return _clean_text(text)


def _extract_rows(words: Sequence[Dict], image, page: int) -> List[Dict]:
    width, height = image.size
    headers = _detect_headers(words, height)
    windows = _column_windows(width, headers)
    row_pad = int(height * 0.018)
    table_top = headers.get("name", {"y1": height * 0.3})["y1"] + row_pad
    bemerkungen = _find_section_word(words, ["bemerkungen"])
    table_bottom = (
        min(height, bemerkungen["y0"] - row_pad) if bemerkungen else int(height * 0.78)
    )
    row_candidates = [
        word
        for word in words
        if word["text"].strip().isdigit()
        and windows["row_number"][0] <= word["cx"] <= windows["row_number"][1]
        and table_top <= word["cy"] <= table_bottom
    ]
    row_candidates.sort(key=lambda w: w["cy"])
    rows: List[Dict] = []
    dedup_threshold = row_pad * 0.6
    filtered_candidates: List[Dict] = []
    for cand in row_candidates:
        if filtered_candidates and abs(cand["cy"] - filtered_candidates[-1]["cy"]) < dedup_threshold:
            continue
        filtered_candidates.append(cand)

    row_candidates = filtered_candidates

    for row_word in row_candidates:
        top = max(0, row_word["y0"] - row_pad)
        bottom = min(height, row_word["y1"] + row_pad)
        window_span = (top, bottom)
        name_words = _words_in_window(words, windows["name"], window_span, allow_digits=False)
        first_name_words = _words_in_window(words, windows["first_name"], window_span, allow_digits=False)
        remark_words = _words_in_window(words, windows["remarks"], window_span)
        name = _combine_text(name_words)
        first_name = _combine_text(first_name_words)
        remark = _combine_text(remark_words)
        if len(remark) < 2:
            remark = _ocr_remark_image(image, windows["remarks"], window_span)
        if not (name or first_name or remark):
            continue
        rows.append(
            {
                "page": page,
                "row_number": row_word["text"].strip(),
                "name": name,
                "vorname": first_name,
                "bemerkung": remark,
            }
        )
    return rows


def extract_reviews(pdf_path: Path, dpi: int, min_conf: float, model_dir: Path) -> List[Dict]:
    os.environ.setdefault("SSL_CERT_FILE", certifi.where())
    os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
    model_dir.mkdir(parents=True, exist_ok=True)
    reader = easyocr.Reader(
        ["de", "en"],
        gpu=False,
        model_storage_directory=str(model_dir),
        user_network_directory=str(model_dir),
    )
    images = convert_from_path(str(pdf_path), dpi=dpi)
    all_rows: List[Dict] = []
    for page_num, image in enumerate(images, start=1):
        words = _read_page(reader, image, min_conf=min_conf)
        rows = _extract_rows(words, image, page_num)
        all_rows.extend(rows)
    return all_rows


def _write_output(rows: List[Dict], output_path: Path) -> None:
    if output_path.suffix.lower() == ".csv":
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["page", "row_number", "name", "vorname", "bemerkung"])
            writer.writeheader()
            writer.writerows(rows)
    else:
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(rows, handle, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="OCR helper for Bewertungsdokumente.")
    parser.add_argument(
        "--pdf",
        type=Path,
        required=True,
        help="Pfad zum Bewertungs-PDF (z. B. eingang-bewertungen/...pdf).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=False,
        help="Ausgabedatei (.json oder .csv). Standard: Ausgabe als JSON auf STDOUT.",
    )
    parser.add_argument("--dpi", type=int, default=300, help="Auflösung für die PDF-Konvertierung.")
    parser.add_argument("--min-conf", type=float, default=0.2, help="Confidence-Schwelle für OCR-Wörter.")
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=Path("easyocr_models"),
        help="Verzeichnis zum Cachen der EasyOCR-Modelle.",
    )
    args = parser.parse_args()

    rows = extract_reviews(args.pdf.expanduser(), dpi=args.dpi, min_conf=args.min_conf, model_dir=args.model_dir)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        _write_output(rows, args.out)
    else:
        print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
