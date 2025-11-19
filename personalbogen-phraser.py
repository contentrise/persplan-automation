# personalbogen_phraser.py
import os
import re
import csv
import json
import math
from datetime import datetime
from pathlib import Path

import fitz  # PyMuPDF
from PIL import Image
import numpy as np

# Optional, but improves OCR on light/scan PDFs
try:
    import cv2
except Exception:
    cv2 = None

import pytesseract


# ------------------------------
# Helpers
# ------------------------------

def load_latest_pdf(input_dir: Path) -> Path:
    pdfs = sorted(input_dir.glob("*.pdf"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not pdfs:
        raise FileNotFoundError("Keine PDF-Datei im Ordner 'mitarbeiteranlage-input' gefunden.")
    return pdfs[0]


def pix_to_image(pix: fitz.Pixmap) -> Image.Image:
    if pix.alpha:
        pix = fitz.Pixmap(pix, 0)  # remove alpha
    mode = "RGB" if pix.n < 4 else "RGBA"
    img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)
    if mode == "RGBA":
        img = img.convert("RGB")
    return img


def render_pdf_to_images(pdf_path: str, dpi: int = 300) -> list[Image.Image]:
    images = []
    doc = fitz.open(pdf_path)
    zoom = dpi / 72.0
    mat = fitz.Matrix(zoom, zoom)
    for page in doc:
        pix = page.get_pixmap(matrix=mat, annots=False)
        images.append(pix_to_image(pix))
    doc.close()
    return images


def enhance_for_ocr(img: Image.Image) -> Image.Image:
    if cv2 is None:
        return img
    arr = np.array(img)
    gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
    # CLAHE + adaptive threshold is robust for scans
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
    g2 = clahe.apply(gray)
    thr = cv2.adaptiveThreshold(g2, 255, cv2.ADAPTIVE_THRESH_MEAN_C,
                                cv2.THRESH_BINARY, 31, 10)
    return Image.fromarray(thr)


def ocr_page(img: Image.Image) -> list[dict]:
    """
    Returns list of tokens: {text, left, top, width, height, conf, line_num, block_num, par_num}
    """
    data = pytesseract.image_to_data(img, lang="deu+eng", output_type=pytesseract.Output.DICT)
    tokens = []
    n = len(data["text"])
    for i in range(n):
        txt = data["text"][i]
        if txt is None:
            txt = ""
        txt = txt.strip()
        if txt == "":
            continue
        token = {
            "text": txt,
            "left": data["left"][i],
            "top": data["top"][i],
            "width": data["width"][i],
            "height": data["height"][i],
            "conf": float(data["conf"][i]) if data["conf"][i] not in ["-1", "", None] else -1.0,
            "block": data.get("block_num", [0]*n)[i],
            "par": data.get("par_num", [0]*n)[i],
            "line": data.get("line_num", [0]*n)[i],
            "word": data.get("word_num", [0]*n)[i],
        }
        tokens.append(token)
    return tokens


def find_nearest_right_text(tokens: list[dict], anchor_regex: str, max_dx: int = 900, max_dy: int = 90) -> str:
    """
    Robust text finder:
    - sucht Text rechts oder leicht unterhalb des Labels
    - berücksichtigt auch, wenn das Feld auf der nächsten Zeile beginnt
    """
    pattern = re.compile(anchor_regex, re.IGNORECASE)
    anchors = [t for t in tokens if pattern.search(t["text"])]
    if not anchors:
        return ""

    anchors.sort(key=lambda t: (t["top"], t["left"]))
    a = anchors[0]
    ay = a["top"]

    # Kandidaten rechts oder leicht darunter
    candidates = [
        t for t in tokens
        if (
            (t["left"] > a["left"] - 30)  # etwas Toleranz
            and (0 < (t["left"] - a["left"]) < max_dx)
            and (0 <= (t["top"] - ay) < max_dy)
        )
    ]

    if not candidates:
        # Fallback: nächster Absatz
        candidates = [
            t for t in tokens
            if (
                abs(t["left"] - a["left"]) < max_dx / 2
                and 0 < (t["top"] - ay) < 2 * max_dy
            )
        ]

    if not candidates:
        return ""

    # Alle Tokens auf einer Zeile (ähnliche y-Koordinaten)
    candidates.sort(key=lambda t: (t["top"], t["left"]))
    line_top = candidates[0]["top"]
    same_line = [t for t in candidates if abs(t["top"] - line_top) < max_dy]
    value = " ".join([t["text"] for t in same_line])

    # Säubern
    value = re.sub(r"[_]+", "", value)
    value = re.sub(r"\s{2,}", " ", value).strip(" :.-")
    return value.strip()



def normalize_numeric(val: str) -> str:
    val = val.strip()
    val = val.replace(",", ".")
    # keep numbers and separators
    if re.search(r"\d", val):
        return re.sub(r"[^0-9./-]", "", val).strip(".-/ ")
    return ""


def line_text(tokens: list[dict]) -> list[list[dict]]:
    lines = {}
    for t in tokens:
        key = (t["block"], t["par"], t["line"])
        lines.setdefault(key, []).append(t)
    seq = []
    for k in sorted(lines.keys()):
        seq.append(sorted(lines[k], key=lambda x: x["left"]))
    return seq


def text_of_line(line_tokens: list[dict]) -> str:
    return " ".join(t["text"] for t in line_tokens)


def bbox_of_tokens(line_tokens: list[dict]) -> tuple[int,int,int,int]:
    xs = [t["left"] for t in line_tokens]
    ys = [t["top"] for t in line_tokens]
    ws = [t["width"] for t in line_tokens]
    hs = [t["height"] for t in line_tokens]
    x1 = min(xs)
    y1 = min(ys)
    x2 = max(xs[i] + ws[i] for i in range(len(xs)))
    y2 = max(ys[i] + hs[i] for i in range(len(ys)))
    return x1, y1, x2, y2


def detect_check_mark_near(img: Image.Image, center: tuple[int,int], box_size: int = 26, fill_thresh: float = 0.08) -> bool:
    """
    Crop a small square around 'center' and decide if it contains a mark (X, ✓, ☒).
    Uses simple density of ink after binarization.
    """
    if cv2 is None:
        return False
    arr = np.array(img)
    h, w = arr.shape[:2]
    cx, cy = center
    half = box_size // 2
    x1 = max(0, cx - half)
    y1 = max(0, cy - half)
    x2 = min(w, cx + half)
    y2 = min(h, cy + half)
    crop = arr[y1:y2, x1:x2]

    # 🔧 FIX: nur in RGB → Grau konvertieren, sonst direkt weiter
    if len(crop.shape) == 3 and crop.shape[2] == 3:
        gray = cv2.cvtColor(crop, cv2.COLOR_RGB2GRAY)
    else:
        gray = crop

    thr = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    density = float(np.count_nonzero(thr)) / float(thr.size)
    return density >= fill_thresh



def checkbox_from_label(tokens: list[dict], img: Image.Image, label_regex: str, search_radius: int = 220) -> str:
    """
    Determine X/Off for a checkbox by:
    1) Finding label token(s)
    2) Searching around the left side of the label for an 'X' OCR token or ink density in a small square
    """
    pattern = re.compile(label_regex, re.IGNORECASE)
    label_candidates = [t for t in tokens if pattern.search(t["text"])]
    if not label_candidates:
        # try match on line level to catch multi-token labels
        for line in line_text(tokens):
            txt = text_of_line(line)
            if re.search(label_regex, txt, re.IGNORECASE):
                label_candidates = line
                break
        if not label_candidates:
            return "Off"

    if isinstance(label_candidates, list) and isinstance(label_candidates[0], dict):
        lbl = label_candidates[0]
        cx = lbl["left"] - 30  # a bit left
        cy = lbl["top"] + lbl["height"] // 2
    else:
        return "Off"

    # 1) OCR token for X/✓/☒ near
    marks = {"x", "X", "✓", "✔", "☑", "☒"}
    for t in tokens:
        if t["text"] in marks:
            dx = t["left"] - cx
            dy = (t["top"] + t["height"] // 2) - cy
            if abs(dx) < search_radius and abs(dy) < 26:
                return "X"

    # 2) Ink density near checkbox
    if detect_check_mark_near(img, (cx, cy)):
        return "X"

    # 3) Sometimes the checkbox is slightly right of the label (rare)
    if detect_check_mark_near(img, (cx + 50, cy)):
        return "X"

    return "Off"


def extract_all(tokens: list[dict], img: Image.Image) -> dict:
    data = {}

    # --- Textual fields ---
    data["Körpergröße"] = normalize_numeric(find_nearest_right_text(tokens, r"K[öo]rpergr[öo]ße"))
    data["Konfektionsgröße"] = find_nearest_right_text(tokens, r"Konfektionsgr[öo]ße")
    data["Schuhgröße"] = find_nearest_right_text(tokens, r"Schuhgr[öo]ße")

    # Notfallkontakt
    data["Notfallname"] = find_nearest_right_text(tokens, r"Name(?!.*&.*Datum)")
    data["Verwandtschaftsgrad"] = find_nearest_right_text(tokens, r"Verwandtschaftsgrad")
    data["Notfalltelefon"] = re.sub(r"[^\d/+ ]", "", find_nearest_right_text(tokens, r"Tel"))

    # Beruflicher Status – Firmenname / Anschrift
    data["Firmenname"] = find_nearest_right_text(tokens, r"Firmenname")
    data["Anschrift"] = find_nearest_right_text(tokens, r"Anschrift")

    # Wie/wer aufmerksam geworden + Fremdsprachen
    data["Wie oder durch wen bist Du auf uns aufmerksam geworden"] = find_nearest_right_text(
        tokens, r"Wie.*auf.*aufmerksam geworden"
    )
    data["Fremdsprachen"] = find_nearest_right_text(tokens, r"Fremdsprachen.*sprechen.*\??")

    # Ort & Datum (es gibt meist 2 Stellen – wir nehmen die erste sinnvoll erkannte)
    ort_datum = find_nearest_right_text(tokens, r"Ort.*Datum")
    data["Ort & Datum"] = ort_datum

    # Von–Bis (erste Stelle)
    data["Von – Bis"] = find_nearest_right_text(tokens, r"Von\s*[–-]\s*Bis")

    # --- Checkboxes / Options ---
    # Urlaub erhalten
    data["bezahlten Urlaub erhalten"] = checkbox_from_label(tokens, img, r"bezahlten Urlaub erhalten")
    data["unbezahlten Urlaub erhalten"] = checkbox_from_label(tokens, img, r"unbezahlten Urlaub erhalten")

    # Beschäftigungsverhältnis (nicht weiteres)
    data["Ich stehe nicht in einem Beschäftigungsverhältnis zu einem weiteren Arbeitgeber sondern"] = checkbox_from_label(
        tokens, img, r"Ich stehe nicht in einem Beschäftigungsverh[äa]ltnis.*weiteren Arbeitgeber.*sondern"
    )

    # Varianten der Zeile (bin Student/in, bin Schüler/in, selbstständig, arbeitslos, lebe von ...)
    data["bin Student/in"] = checkbox_from_label(tokens, img, r"bin\s+Student[\/in]*|bin\s+Studentin|Student/in")
    data["bin Schülerin/in"] = checkbox_from_label(tokens, img, r"bin\s+Sch[uü]ler[\/in]*|Sch[uü]lerin|Schüler/in")
    data["selbstständig"] = checkbox_from_label(tokens, img, r"selbstst[äa]ndig")
    data["arbeitslos gemeldet"] = checkbox_from_label(tokens, img, r"arbeitslos gemeldet")
    data["lebe von dem Unterhalt meiner Eltern und beabsichtige ein Studium"] = checkbox_from_label(
        tokens, img, r"lebe.*Unterhalt.*Eltern.*Studium"
    )

    # Minijob in diesem Kalenderjahr?
    data["Waren Sie in diesem Kalenderjahr in einem anderen Unternehmen geringfügig (Minijob) beschäftigt? – Nein"] = checkbox_from_label(
        tokens, img, r"geringf[üu]gig.*\(Minijob\).*Nein"
    )
    data["Waren Sie in diesem Kalenderjahr in einem anderen Unternehmen geringfügig (Minijob) beschäftigt? – Ja"] = checkbox_from_label(
        tokens, img, r"geringf[üu]gig.*\(Minijob\).*Ja"
    )

    # Kurzfristig (70 Tage)?
    data["Waren Sie in diesem Kalenderjahr in einem anderen Unternehmen kurzfristig (70 Tage) beschäftigt? – Nein"] = checkbox_from_label(
        tokens, img, r"kurzfristig.*70.*Tage.*Nein"
    )
    data["Waren Sie in diesem Kalenderjahr in einem anderen Unternehmen kurzfristig (70 Tage) beschäftigt? – Ja"] = checkbox_from_label(
        tokens, img, r"kurzfristig.*70.*Tage.*Ja"
    )

    # Schon einmal bei uns beschäftigt?
    data["Waren Sie schon einmal bei uns beschäftigt? – Nein"] = checkbox_from_label(tokens, img, r"schon einmal.*besch[äa]ftigt.*Nein")
    data["Waren Sie schon einmal bei uns beschäftigt? – Ja"] = checkbox_from_label(tokens, img, r"schon einmal.*besch[äa]ftigt.*Ja")

    # Aufenthaltsgenehmigung/Arbeitsgenehmigung (Nicht-EU)
    data["Aufenthaltsgenehmigung – Nein"] = checkbox_from_label(tokens, img, r"Aufenthaltsgenehmigung.*Nein")
    data["Aufenthaltsgenehmigung – Ja"] = checkbox_from_label(tokens, img, r"Aufenthaltsgenehmigung.*Ja")
    data["Arbeitsgenehmigung – Nein"] = checkbox_from_label(tokens, img, r"Arbeitsgenehmigung.*Nein")
    data["Arbeitsgenehmigung – Ja"] = checkbox_from_label(tokens, img, r"Arbeitsgenehmigung.*Ja")

    # Ermittlungs-/Strafverfahren
    data["Schwebt Ermittlungs-/Strafverfahren vor? – Ja"] = checkbox_from_label(tokens, img, r"Ermittlungs.*Strafverfahren.*Ja")
    data["Schwebt Ermittlungs-/Strafverfahren vor? – Nein"] = checkbox_from_label(tokens, img, r"Ermittlungs.*Strafverfahren.*Nein")

    # Vorbestraft
    data["Sind Sie vorbestraft? – Ja"] = checkbox_from_label(tokens, img, r"vorbestraft.*Ja")
    data["Sind Sie vorbestraft? – Nein"] = checkbox_from_label(tokens, img, r"vorbestraft.*Nein")

    # Schwerbehindert
    data["Sind Sie schwerbehindert oder gleichgestellt? – Ja"] = checkbox_from_label(tokens, img, r"schwerbehindert.*gleichgestellt.*Ja")
    data["Sind Sie schwerbehindert oder gleichgestellt? – Nein"] = checkbox_from_label(tokens, img, r"schwerbehindert.*gleichgestellt.*Nein")

    # Ersthelfer/Sanitäter/Krankenschwester/Wasserwacht
    data["Ersthelfer/Sanitäter/Krankenschwester/Wasserwacht – Ja"] = checkbox_from_label(tokens, img, r"Ersthelfer|Sanit[aä]ter|Krankenschwester|Wasserwacht.*Ja")
    data["Ersthelfer/Sanitäter/Krankenschwester/Wasserwacht – Nein"] = checkbox_from_label(tokens, img, r"Ersthelfer|Sanit[aä]ter|Krankenschwester|Wasserwacht.*Nein")

    # Führerschein
    data["Führerschein – Ja"] = checkbox_from_label(tokens, img, r"F[üu]hrerschein.*Ja")
    data["Führerschein – Nein"] = checkbox_from_label(tokens, img, r"F[üu]hrerschein.*Nein")

    # Jobmails/WhatsApp/E-Mail-Gruppe Einverständnis (Text wird meist nur unterschrieben – Checkbox optional)
    data["WhatsApp/E-Mail-Gruppe Einverständnis"] = checkbox_from_label(tokens, img, r"WhatsApp.*E-?Mail.*einverstanden")

    return data


def merge_pages_dicts(dicts: list[dict]) -> dict:
    out = {}
    for d in dicts:
        for k, v in d.items():
            if k not in out or (isinstance(v, str) and v and out.get(k, "") == ""):
                out[k] = v
            elif isinstance(v, str) and v and out.get(k, "") and v != out[k]:
                # Keep the first; but if it is a checkbox pair "Ja/Nein", prefer "X"
                if v == "X":
                    out[k] = "X"
    return out


def write_csv(data: dict, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"parsed_personalbogen_{ts}.csv"

    # Deterministic column order (group important ones first)
    preferred_order = [
        "Körpergröße", "Konfektionsgröße", "Schuhgröße",
        "Notfallname", "Verwandtschaftsgrad", "Notfalltelefon",
        "Firmenname", "Anschrift",
        "Wie oder durch wen bist Du auf uns aufmerksam geworden", "Fremdsprachen",
        "Ort & Datum", "Von – Bis",
        "bezahlten Urlaub erhalten", "unbezahlten Urlaub erhalten",
        "Ich stehe nicht in einem Beschäftigungsverhältnis zu einem weiteren Arbeitgeber sondern",
        "bin Student/in", "bin Schülerin/in", "selbstständig", "arbeitslos gemeldet",
        "lebe von dem Unterhalt meiner Eltern und beabsichtige ein Studium",
        "Waren Sie in diesem Kalenderjahr in einem anderen Unternehmen geringfügig (Minijob) beschäftigt? – Nein",
        "Waren Sie in diesem Kalenderjahr in einem anderen Unternehmen geringfügig (Minijob) beschäftigt? – Ja",
        "Waren Sie in diesem Kalenderjahr in einem anderen Unternehmen kurzfristig (70 Tage) beschäftigt? – Nein",
        "Waren Sie in diesem Kalenderjahr in einem anderen Unternehmen kurzfristig (70 Tage) beschäftigt? – Ja",
        "Waren Sie schon einmal bei uns beschäftigt? – Nein",
        "Waren Sie schon einmal bei uns beschäftigt? – Ja",
        "Aufenthaltsgenehmigung – Nein", "Aufenthaltsgenehmigung – Ja",
        "Arbeitsgenehmigung – Nein", "Arbeitsgenehmigung – Ja",
        "Schwebt Ermittlungs-/Strafverfahren vor? – Ja", "Schwebt Ermittlungs-/Strafverfahren vor? – Nein",
        "Sind Sie vorbestraft? – Ja", "Sind Sie vorbestraft? – Nein",
        "Sind Sie schwerbehindert oder gleichgestellt? – Ja", "Sind Sie schwerbehindert oder gleichgestellt? – Nein",
        "Ersthelfer/Sanitäter/Krankenschwester/Wasserwacht – Ja", "Ersthelfer/Sanitäter/Krankenschwester/Wasserwacht – Nein",
        "Führerschein – Ja", "Führerschein – Nein",
        "WhatsApp/E-Mail-Gruppe Einverständnis"
    ]
    # include any extra keys
    for k in data.keys():
        if k not in preferred_order:
            preferred_order.append(k)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=preferred_order)
        writer.writeheader()
        writer.writerow({k: data.get(k, "") for k in preferred_order})

    print(f"[OK] CSV erzeugt → {out_path}")


def main():
    input_dir = Path("mitarbeiteranlage-input")
    output_dir = Path("mitarbeiteranlage-output")

    pdf_path = load_latest_pdf(input_dir)
    print(f"[INFO] Analysiere PDF: {pdf_path.name}")

    images = render_pdf_to_images(str(pdf_path), dpi=300)
    all_pages_data = []

    for idx, img in enumerate(images, start=1):
        print(f"[INFO] Seite {idx}/{len(images)} OCR …")
        proc = enhance_for_ocr(img)
        tokens = ocr_page(proc)
        page_data = extract_all(tokens, proc)
        all_pages_data.append(page_data)

    data = merge_pages_dicts(all_pages_data)

    # Normalize X/Off to simple "X"/"Off"
    for k, v in list(data.items()):
        if isinstance(v, str):
            if v.strip().lower() in {"x", "ja", "yes", "true", "1"}:
                data[k] = "X"
            elif v.strip() == "":
                # leave empty unless it is an explicit checkbox slot we set "Off"
                if "– Ja" in k or "– Nein" in k or "bin " in k or "Urlaub" in k or "Beschäftigungsverhältnis" in k:
                    data[k] = "Off"

    write_csv(data, output_dir)


if __name__ == "__main__":
    main()
