import time
import tempfile
import requests
import base64
import re
import json
import os
import sys
import io
from pathlib import Path
from datetime import datetime

from playwright.sync_api import sync_playwright

from src import config
from src.login import do_login
from src.mitarbeiter_vervollstaendigen import (
    _click_lastname_link,
    _load_personalbogen_json,
    _locate_search_input,
    _open_mitarbeiterinformationen,
    _open_user_overview,
)


class _Tee:
    def __init__(self, primary, buffer):
        self.primary = primary
        self.buffer = buffer

    def write(self, data):
        try:
            self.primary.write(data)
        except Exception:
            pass
        try:
            self.buffer.write(data)
        except Exception:
            pass
        return len(data)

    def flush(self):
        try:
            self.primary.flush()
        except Exception:
            pass
        try:
            self.buffer.flush()
        except Exception:
            pass


def _has_retry_warning(log_text: str) -> bool:
    if not log_text:
        return False
    patterns = [
        r"\[WARNUNG\].*(nicht gesetzt|nicht gefunden|fehlgeschlagen)",
        r"\[WARNUNG\].*nicht sichtbar",
        r"\[WARNUNG\].*nicht geöffnet",
    ]
    return any(re.search(pat, log_text, re.IGNORECASE) for pat in patterns)


class FieldTracker:
    def __init__(self, attempt: int, max_retries: int):
        self.attempt = attempt
        self.max_retries = max_retries
        self.entries: list[dict] = []

    def _add(self, section: str, field_id: str, expected: str, actual: str, status: str) -> None:
        self.entries.append(
            {
                "section": section,
                "field_id": field_id,
                "expected": expected,
                "actual": actual,
                "status": status,
                "attempt": self.attempt,
            }
        )

    def ok(self, section: str, field_id: str, expected: str, actual: str) -> None:
        self._add(section, field_id, expected, actual, "ok")

    def skip(self, section: str, field_id: str, expected: str, actual: str) -> None:
        self._add(section, field_id, expected, actual, "skipped")

    def missing(self, section: str, field_id: str, expected: str, actual: str) -> None:
        self._add(section, field_id, expected, actual, "missing")

    def error(self, section: str, field_id: str, actual: str) -> None:
        self._add(section, field_id, "", actual, "error")

    def missing_fields(self) -> list[dict]:
        return [entry for entry in self.entries if entry.get("status") in {"missing", "error"}]

    def log_summary(self) -> None:
        missing = self.missing_fields()
        print(
            "=== MISSING_FIELDS ===\n"
            + json.dumps(
                {"attempt": self.attempt, "max_retries": self.max_retries, "missing": missing},
                ensure_ascii=False,
                indent=2,
            )
        )

UPLOAD_LABELS = {
    "sicherheitsbelehrung": "Sicherheitsbelehrung",
    "immatrikulation": "Imma/ Schulbescheinigung",
    "infektionsschutz": "Infektionsschutzbelehrung",
    "aufenthaltserlaubnis": "Arbeitsaufenthaltserlaubnis",
    "arbeitserlaubnis": "Arbeitsaufenthaltserlaubnis",
    "inventionsschutz": "Inventionsschutzbelehrung",
    "rentenbefreiung": "Rentenbefreiung",
    "profilbild": "Profilbild",
}

PERSONAL_FORM_VARIANTS = {
    "kb": {
        "aliases": {"default", "standard", "kb"},
        "upload_fields": [
            ("immatrikulation", True),
            ("infektionsschutz", True),
            ("profilbild", False),
            ("aufenthaltserlaubnis", False),
        ],
    },
    "geringfuegig": {
        "aliases": {"geringfügig", "geringfuegig", "minijob", "mini", "gmj", "gb"},
        "upload_fields": [
            ("infektionsschutz", True),
            ("profilbild", False),
            ("rentenbefreiung", False),
            ("aufenthaltserlaubnis", False),
        ],
    },
    "teilzeit": {
        "aliases": {"tz", "teilzeit", "pt"},
        "upload_fields": [
            ("infektionsschutz", True),
            ("profilbild", False),
            ("aufenthaltserlaubnis", False),
        ],
    },
}


def _resolve_form_variant(payload: dict) -> str:
    raw_value = (
        payload.get("form_variant")
        or payload.get("formVariant")
        or payload.get("variant")
        or payload.get("contract_type")
        or payload.get("vertragstyp")
        or (payload.get("vertrag") or {}).get("contract_type")
        or ""
    )
    normalized = str(raw_value).strip().lower()
    if not normalized:
        return "kb"
    for key, variant in PERSONAL_FORM_VARIANTS.items():
        if normalized == key:
            return key
        if normalized in variant.get("aliases", set()):
            return key
    return "kb"


def _should_require_immatrikulation(payload: dict) -> bool:
    employment_mode = payload.get("beschaeftigung_modus")
    if employment_mode != "kein":
        return False
    status = str(payload.get("kein_beschaeftigungsverhaeltnis") or "").strip().lower()
    return status in {"studentin", "schuelerin"}


def _build_required_upload_keys(payload: dict) -> list[str]:
    variant_key = _resolve_form_variant(payload)
    variant = PERSONAL_FORM_VARIANTS.get(variant_key, PERSONAL_FORM_VARIANTS["kb"])
    required = []
    for key, required_flag in variant.get("upload_fields", []):
        if not required_flag:
            continue
        if key == "immatrikulation" and not _should_require_immatrikulation(payload):
            continue
        required.append(key)
    return required


def _iso_to_de_date(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"none", "null", "undefined", "nan"}:
        return ""
    try:
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            dt = datetime.strptime(text[:10], "%Y-%m-%d")
            if dt.year < 2005:
                return ""
            return dt.strftime("%d.%m.%Y")
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.year < 2005:
            return ""
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return ""


def _sanitize_valid_until(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"none", "null", "undefined", "nan", "-", "—"}:
        return ""
    match = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", text)
    if match and int(match.group(1)) < 2005:
        return ""
    match = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", text)
    if match and int(match.group(3)) < 2005:
        return ""
    return text


def _normalize_uploads(uploads: dict) -> dict:
    if not isinstance(uploads, dict):
        return {}
    normalized = dict(uploads)
    aliases = {
        "arbeitsaufenthaltserlaubnis": "aufenthaltserlaubnis",
        "arbeits-aufenthaltserlaubnis": "aufenthaltserlaubnis",
        "aufenthaltserlaubnisarbeit": "aufenthaltserlaubnis",
        "inventionsschutzbelehrung": "inventionsschutz",
        "inventionsschutz": "inventionsschutz",
        "immatrikulationsbescheinigung": "immatrikulation",
    }
    for raw_key, target_key in aliases.items():
        if raw_key in normalized and target_key not in normalized:
            normalized[target_key] = normalized[raw_key]
    return normalized


# Unterlagen, die niemals als "Einzureichende Unterlage" eingetragen werden sollen.
NON_EINZUREICHENDE_UNTERLAGEN = {"rentenbefreiung"}
NON_EINZUREICHENDE_LABELS = {"rentenbefreiung"}


def _build_unterlagen_from_payload(payload: dict) -> list[dict]:
    uploads = payload.get("uploads") if isinstance(payload, dict) else {}
    uploads = _normalize_uploads(uploads)
    if not isinstance(uploads, dict):
        return []

    required_keys = _build_required_upload_keys(payload)
    required_set = set(required_keys)
    skip_keys = {
        "personalbogen",
        "vertrag",
        "arbeitsvertrag",
        "zusatzvereinbarung",
        "sicherheitsbelehrung",
    }
    skip_keys.update(NON_EINZUREICHENDE_UNTERLAGEN)

    # Feste Reihenfolge, damit die Einträge in der Akte reproduzierbar sind.
    preferred_order = [
        "sicherheitsbelehrung",
        "inventionsschutz",
        "immatrikulation",
        "infektionsschutz",
        "aufenthaltserlaubnis",
        "arbeitserlaubnis",
    ]
    preferred_order = [key for key in preferred_order if key not in NON_EINZUREICHENDE_UNTERLAGEN]
    ordered_keys = [key for key in preferred_order if key in required_set or key in uploads]
    ordered_keys.extend([key for key in required_keys if key not in ordered_keys])
    ordered_keys.extend([key for key in uploads.keys() if key not in ordered_keys])
    ordered_keys = [key for key in ordered_keys if key not in NON_EINZUREICHENDE_UNTERLAGEN]

    unterlagen = []
    for key in ordered_keys:
        if key in skip_keys:
            continue
        if key == "profilbild":
            continue
        meta = uploads.get(key)
        has_source = False
        if isinstance(meta, dict):
            has_source = bool(
                (meta.get("key") or "").strip()
                or (meta.get("url") or "").strip()
                or (meta.get("name") or "").strip()
            )
        if not has_source and key not in required_set:
            continue
        label = UPLOAD_LABELS.get(key, key)
        valid_until = ""
        if has_source and isinstance(meta, dict):
            valid_until = _iso_to_de_date(meta.get("validUntil"))
        if key not in {
            "infektionsschutz",
            "aufenthaltserlaubnis",
            "arbeitserlaubnis",
            "immatrikulation",
            "inventionsschutz",
        }:
            valid_until = ""
        vorhanden = has_source or key == "sicherheitsbelehrung"
        unterlagen.append(
            {
                "key": key,
                "bezeichnung": label,
                "gueltig_bis": valid_until,
                "vorhanden": vorhanden,
            }
        )
    return unterlagen


def _clear_einzureichende_unterlagen(page, skip: bool = False) -> None:
    def _remove_disallowed() -> int:
        labels = {label.strip().lower() for label in NON_EINZUREICHENDE_LABELS if label.strip()}
        if not labels:
            return 0

        candidates = [page]
        inhalt = page.frame(name="inhalt")
        if inhalt:
            candidates.append(inhalt)
        candidates.extend(page.frames)

        removed = 0
        for target in candidates:
            try:
                if target.locator("#einzureichendes").count() == 0:
                    continue
            except Exception:
                continue
            rows = target.locator("#einzureichendes tbody tr")
            try:
                row_count = rows.count()
            except Exception:
                row_count = 0
            for idx in range(row_count):
                row = rows.nth(idx)
                try:
                    cells = row.locator("td")
                    if cells.count() < 2:
                        continue
                    label_text = (cells.nth(1).inner_text() or "").strip().lower()
                    if label_text not in labels:
                        continue
                except Exception:
                    continue
                btn = row.locator(
                    "button[onclick*='maEinzureichendesLoeschen'], "
                    "button[title*='deaktivieren'], "
                    "img.sprite_16x16.inaktiv"
                ).first
                try:
                    if btn.count() == 0:
                        continue
                    btn.click()
                    removed += 1
                    time.sleep(0.4)
                except Exception:
                    try:
                        btn.evaluate("el => el.click()")
                        removed += 1
                        time.sleep(0.4)
                    except Exception:
                        continue
        if removed:
            print(f"[OK] Einzureichende Unterlagen entfernt (verboten): {removed}")
        return removed

    _remove_disallowed()
    if skip:
        print("[INFO] Einzureichende Unterlagen: Skip (Retry).")
        return
    def _find_target():
        candidates = [page]
        inhalt = page.frame(name="inhalt")
        if inhalt:
            candidates.append(inhalt)
        candidates.extend(page.frames)
        for candidate in candidates:
            try:
                if candidate.locator("#einzureichendes").count() > 0:
                    return candidate
            except Exception:
                continue
        return None

    def _row_count(target) -> int:
        try:
            return target.locator("#einzureichendes tbody tr").count()
        except Exception:
            return 0

    def _log_state(target, prefix: str) -> None:
        try:
            info = target.evaluate(
                """() => {
                    const rows = Array.from(document.querySelectorAll('#einzureichendes tbody tr'));
                    const sample = rows.slice(0, 3).map((row) => {
                        const cells = row.querySelectorAll('td');
                        const label = cells[1]?.textContent?.trim() || '';
                        const id = row.getAttribute('id') || '';
                        return `${id}:${label}`;
                    });
                    return { count: rows.length, sample };
                }"""
            )
            print(f"[DEBUG] {prefix} Einzureichende: count={info.get('count')}, sample={info.get('sample')}")
        except Exception as exc:
            print(f"[DEBUG] {prefix} Einzureichende: Konnte Zustand nicht lesen: {exc}")

    def _find_target_with_retry(timeout_seconds: float = 6.0):
        deadline = time.time() + timeout_seconds
        last_error = None
        while time.time() < deadline:
            try:
                target = _find_target()
                if target is not None:
                    return target
            except Exception as exc:
                last_error = exc
            time.sleep(0.3)
        if last_error:
            print(f"[WARNUNG] Tabelle 'Einzureichende Unterlagen' nicht gefunden (Retry-Fehler: {last_error})")
        return None

    target = _find_target_with_retry()
    if target is None:
        print("[WARNUNG] Tabelle 'Einzureichende Unterlagen' nicht gefunden.")
        return

    try:
        filter_active = target.locator("#aktiveUnterlagen").first
        if filter_active.count() > 0 and not filter_active.is_checked():
            filter_active.click()
            time.sleep(0.8)
    except Exception:
        pass

    try:
        target.wait_for_selector("#einzureichendes tbody tr", timeout=8000)
    except Exception:
        pass

    removed = 0
    _log_state(target, "Vor dem Löschen")
    max_loops = 100
    loops = 0
    no_change_rounds = 0
    while loops < max_loops:
        loops += 1
        target = _find_target_with_retry()
        if target is None:
            print("[WARNUNG] Tabelle 'Einzureichende Unterlagen' nicht gefunden (nach Refresh).")
            break

        rows = target.locator("#einzureichendes tbody tr")
        try:
            row_count = rows.count()
        except Exception:
            row_count = 0
        if row_count == 0:
            break

        row = rows.first
        buttons = row.locator(
            "button[onclick*='maEinzureichendesLoeschen'], "
            "button[title*='deaktivieren'], "
            "img.sprite_16x16.inaktiv"
        )
        try:
            count = buttons.count()
        except Exception:
            count = 0
        if count == 0:
            break

        before_count = _row_count(target)
        try:
            info = row.evaluate(
                """(btn) => {
                    const row = btn;
                    const id = row?.getAttribute('id') || '';
                    const cells = row?.querySelectorAll('td') || [];
                    const label = cells[1]?.textContent?.trim() || '';
                    return { id, label };
                }"""
            )
            print(f"[DEBUG] Lösche Unterlage: {info.get('id')} | {info.get('label')}")
        except Exception as exc:
            print(f"[DEBUG] Lösche Unterlage: Konnte Row-Info nicht lesen: {exc}")

        try:
            page.once("dialog", lambda dialog: dialog.accept())
        except Exception as exc:
            print(f"[DEBUG] Dialog-Handler konnte nicht gesetzt werden: {exc}")
        try:
            clicked = target.evaluate(
                """() => {
                    const btn =
                        document.querySelector("#einzureichendes tbody tr button[onclick*='maEinzureichendesLoeschen']") ||
                        document.querySelector("#einzureichendes tbody tr button[title*='deaktivieren']");
                    if (btn) { btn.click(); return true; }
                    const img = document.querySelector("#einzureichendes tbody tr img.sprite_16x16.inaktiv");
                    if (img && img.closest('button')) { img.closest('button').click(); return true; }
                    return false;
                }"""
            )
            if not clicked:
                buttons.first.scroll_into_view_if_needed()
                buttons.first.click(force=True)
        except Exception as exc:
            print(f"[WARNUNG] Unterlage konnte nicht gelöscht/deaktiviert werden: {exc}")
            break

        try:
            page.wait_for_load_state("domcontentloaded", timeout=5000)
        except Exception:
            pass
        time.sleep(0.4)

        target = _find_target_with_retry()
        if info and info.get("id"):
            try:
                if target:
                    target.wait_for_selector(f"#{info.get('id')}", state="detached", timeout=8000)
            except Exception:
                pass
        after_count = _row_count(target) if target else 0
        if after_count < before_count:
            removed += 1
            no_change_rounds = 0
        else:
            no_change_rounds += 1
            print(f"[DEBUG] Nach Löschung keine Zeilenänderung erkannt (before={before_count}, after={after_count}).")
            if no_change_rounds >= 3:
                print("[WARNUNG] Löschen scheint nicht zu greifen – breche ab.")
                break

    target = _find_target()
    if target:
        _log_state(target, "Nach dem Löschen")
    print(f"[INFO] Einzureichende Unterlagen entfernt/deaktiviert: {removed}")
    try:
        target.wait_for_selector("#einzureichendes", timeout=6000)
    except Exception:
        pass


def _normalize_doc_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    return re.sub(r"[^a-z0-9]+", "", text)


def _extract_unterlagen_rows(page) -> list[dict]:
    candidates = [page]
    try:
        inhalt = page.frame(name="inhalt")
    except Exception:
        inhalt = None
    if inhalt:
        candidates.append(inhalt)

    target = None
    for candidate in candidates:
        try:
            if candidate.locator("#einzureichendes").count() > 0:
                target = candidate
                break
        except Exception:
            continue
    if target is None:
        return []

    rows = target.locator("#einzureichendes tbody tr")
    entries = []
    for idx in range(rows.count()):
        row = rows.nth(idx)
        try:
            cells = row.locator("td")
            if cells.count() < 4:
                continue
            label = cells.nth(1).inner_text().strip()
            valid = cells.nth(2).inner_text().strip()
            vorhanden = cells.nth(3).inner_text().strip()
            entries.append(
                {
                    "label": label,
                    "valid_until": valid,
                    "vorhanden": vorhanden,
                }
            )
        except Exception:
            continue
    return entries


def _unterlage_exists(page, label: str, valid_until: str = "") -> bool:
    if not label:
        return False
    normalized_label = _normalize_doc_text(label)
    rows = _extract_unterlagen_rows(page)
    for row in rows:
        row_label = _normalize_doc_text(row.get("label", ""))
        if normalized_label and normalized_label not in row_label:
            continue
        if valid_until:
            if valid_until.strip() != str(row.get("valid_until") or "").strip():
                continue
        return True
    return False


def _has_profile_image(page) -> bool:
    candidates = [page]
    try:
        inhalt = page.frame(name="inhalt")
    except Exception:
        inhalt = None
    if inhalt:
        candidates.append(inhalt)
    for target in candidates:
        try:
            found = target.evaluate(
                """() => {
                    const imgs = Array.from(document.querySelectorAll('img'))
                        .filter(img => img.src && !img.src.includes('transparent.gif'));
                    return imgs.some(img => img.naturalWidth > 10 && img.naturalHeight > 10);
                }"""
            )
            if found:
                return True
        except Exception:
            continue
    return False


def _extract_documents_table(page) -> list[dict]:
    candidates = [page]
    try:
        inhalt = page.frame(name="inhalt")
    except Exception:
        inhalt = None
    if inhalt:
        candidates.append(inhalt)

    target = None
    for candidate in candidates:
        try:
            if candidate.locator("#dokumenten_tabelle").count() > 0:
                target = candidate
                break
        except Exception:
            continue

    if target is None:
        return []

    rows = target.locator("#dokumenten_tabelle tbody tr")
    entries = []
    for idx in range(rows.count()):
        row = rows.nth(idx)
        try:
            cells = row.locator("td")
            if cells.count() < 5:
                continue
            file_text = cells.nth(1).inner_text().strip()
            desc_text = cells.nth(2).inner_text().strip()
            valid_text = cells.nth(4).inner_text().strip()
            if not file_text and not desc_text:
                continue
            entries.append(
                {
                    "file": file_text,
                    "description": desc_text,
                    "valid_until": valid_text,
                }
            )
        except Exception:
            continue
    return entries


def _enrich_unterlagen_from_documents(unterlagen: list[dict], dokumente: list[dict]) -> list[dict]:
    if not dokumente:
        return unterlagen

    keyword_map = {
        "sicherheitsbelehrung": ["sicherheitsbelehrung"],
        "immatrikulation": ["immatrikulation", "schulbescheinigung", "imma"],
        "infektionsschutz": ["infektionsschutz"],
        "aufenthaltserlaubnis": ["aufenthaltserlaubnis", "arbeitsaufenthaltserlaubnis", "arbeitserlaubnis"],
        "arbeitserlaubnis": ["aufenthaltserlaubnis", "arbeitsaufenthaltserlaubnis", "arbeitserlaubnis"],
        "inventionsschutz": ["inventionsschutz", "inventionsschutzbelehrung"],
        "rentenbefreiung": ["rentenbefreiung"],
    }

    normalized_docs = []
    for entry in dokumente:
        normalized_docs.append(
            {
                "file": _normalize_doc_text(entry.get("file", "")),
                "description": _normalize_doc_text(entry.get("description", "")),
                "valid_until": _sanitize_valid_until(entry.get("valid_until")),
            }
        )

    for unterlage in unterlagen:
        key = str(unterlage.get("key") or "").strip().lower()
        label = str(unterlage.get("bezeichnung") or "").strip()
        if not key:
            continue
        keywords = keyword_map.get(key)
        if not keywords and label:
            keywords = [_normalize_doc_text(label)]
        if not keywords:
            continue

        found = None
        for doc in normalized_docs:
            if any(k in doc["description"] or k in doc["file"] for k in keywords):
                found = doc
                break
        if found:
            unterlage["vorhanden"] = True
            if not unterlage.get("gueltig_bis") and found.get("valid_until"):
                valid_text = _sanitize_valid_until(found["valid_until"])
                if key in {
                    "infektionsschutz",
                    "aufenthaltserlaubnis",
                    "arbeitserlaubnis",
                    "immatrikulation",
                    "inventionsschutz",
                }:
                    unterlage["gueltig_bis"] = valid_text
    return unterlagen


def _resolve_profile_image(payload: dict, temp_dir: Path) -> Path | None:
    uploads = payload.get("uploads") if isinstance(payload, dict) else {}
    if not isinstance(uploads, dict):
        return None
    profile_meta = uploads.get("profilbild")
    if not isinstance(profile_meta, dict):
        return None

    data_url = str(profile_meta.get("dataUrl") or "").strip()
    if data_url.startswith("data:") and ";base64," in data_url:
        header, b64_data = data_url.split(";base64,", 1)
        ext = ".jpg"
        if "png" in header:
            ext = ".png"
        elif "webp" in header:
            ext = ".webp"
        elif "heic" in header or "heif" in header:
            ext = ".heic"
        target_path = temp_dir / f"profilbild{ext}"
        try:
            raw = base64.b64decode(b64_data)
            if len(raw) < 16:
                print("[WARNUNG] Profilbild dataUrl zu klein (vermutlich leer/kaputt).")
                return None
            target_path.write_bytes(raw)
            return target_path
        except Exception as exc:
            print(f"[WARNUNG] Konnte Profilbild aus dataUrl nicht dekodieren: {exc}")

    image_url = str(profile_meta.get("url") or "").strip()
    if not image_url:
        return None
    try:
        response = requests.get(image_url, timeout=60)
        response.raise_for_status()
    except Exception as exc:
        print(f"[WARNUNG] Profilbild konnte nicht geladen werden: {exc}")
        return None

    content_type = (response.headers.get("Content-Type") or "").lower()
    content_len = response.headers.get("Content-Length") or ""
    print(f"[INFO] Profilbild HTTP: status={response.status_code}, type={content_type or '—'}, len={content_len or '—'}")
    if not content_type.startswith("image/"):
        snippet = response.content[:200]
        if snippet.strip().startswith(b"<"):
            print("[WARNUNG] Profilbild-URL lieferte HTML/XML (vermutlich abgelaufen/denied).")
            return None
    ext = ".jpg"
    if "png" in content_type:
        ext = ".png"
    elif "jpeg" in content_type or "jpg" in content_type:
        ext = ".jpg"
    elif "webp" in content_type:
        ext = ".webp"
    elif "heic" in content_type or "heif" in content_type:
        ext = ".heic"
    else:
        url_ext = Path(image_url.split("?", 1)[0]).suffix.lower()
        if url_ext in {".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif"}:
            ext = url_ext
    if len(response.content) < 16:
        print("[WARNUNG] Profilbild-Download zu klein (vermutlich leer/kaputt).")
        return None

    header = response.content[:16]
    looks_like_image = (
        header.startswith(b"\xFF\xD8\xFF")  # jpeg
        or header.startswith(b"\x89PNG\r\n\x1a\n")  # png
        or (header.startswith(b"RIFF") and b"WEBP" in header)  # webp
        or b"ftypheic" in header
        or b"ftypheif" in header
    )
    if not looks_like_image and content_type.startswith("image/"):
        print("[WARNUNG] Profilbild-Header wirkt nicht wie Bild (evtl. Proxy/Fehlerseite).")
        return None

    target_path = temp_dir / f"profilbild{ext}"
    target_path.write_bytes(response.content)
    return target_path


def _normalize_profile_image(image_path: Path) -> Path | None:
    try:
        size = image_path.stat().st_size
    except Exception as exc:
        print(f"[WARNUNG] Profilbild nicht lesbar: {exc}")
        return None
    if size <= 0:
        print("[WARNUNG] Profilbild-Datei ist leer.")
        return None

    ext = image_path.suffix.lower()
    if ext in {".jpg", ".jpeg"}:
        return image_path

    try:
        from PIL import Image
    except Exception as exc:
        print(f"[WARNUNG] PIL nicht verfügbar – Profilbild bleibt unverändert: {exc}")
        return image_path

    try:
        with Image.open(image_path) as img:
            try:
                w, h = img.size
                mode = img.mode
                print(f"[INFO] Profilbild geladen: {image_path.name} ({w}x{h}, mode={mode})")
            except Exception:
                pass
            if img.mode in {"RGBA", "LA"} or (img.mode == "P" and "transparency" in img.info):
                rgba = img.convert("RGBA")
                try:
                    alpha = rgba.split()[-1]
                    bbox = alpha.getbbox()
                    if bbox:
                        rgba = rgba.crop(bbox)
                except Exception:
                    pass
                base = Image.new("RGB", rgba.size, (255, 255, 255))
                base.paste(rgba, mask=rgba.split()[-1])
                rgb = base
            else:
                rgb = img.convert("RGB")
            try:
                # Detect near-black images (likely upload/convert issues).
                small = rgb.resize((64, 64))
                pixels = list(small.getdata())
                dark = sum(1 for r, g, b in pixels if r <= 8 and g <= 8 and b <= 8)
                ratio = dark / max(1, len(pixels))
                if ratio >= 0.9:
                    print(f"[WARNUNG] Profilbild wirkt schwarz (dark_ratio={ratio:.2f}).")
            except Exception:
                pass
            target_path = image_path.with_suffix(".jpg")
            rgb.save(target_path, format="JPEG", quality=92)
            print(f"[INFO] Profilbild konvertiert: {image_path.suffix} -> .jpg")
            return target_path
    except Exception as exc:
        print(f"[WARNUNG] Profilbild-Konvertierung fehlgeschlagen: {exc}")
        return image_path


def _wait_for_profile_preview(target, timeout_seconds: float = 6.0) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            ready = target.evaluate(
                """() => {
                    const input = document.querySelector('#fileupload');
                    if (!input) return false;
                    const root = input.closest('.ui-dialog, form, body') || document.body;
                    const imgs = Array.from(root.querySelectorAll('img'))
                        .filter(img => img.src && !img.src.includes('transparent.gif'));
                    return imgs.some(img => img.naturalWidth > 10 && img.naturalHeight > 10);
                }"""
            )
            if ready:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    print("[WARNUNG] Kein Bild-Preview erkannt (Upload evtl. noch nicht verarbeitet).")
    return False


def _click_unterlage_hinzufuegen(page) -> bool:
    selectors = [
        "button:has-text('Unterlage hinzufügen')",
        "button:has-text('Unterlage hinzufuegen')",
        "button[onclick*='openUiWindowReloaded'][title*='Unterlage']",
        "button[onclick*='einzureichendes_editor']",
    ]

    deadline = time.time() + 10
    last_error = None
    while time.time() < deadline:
        candidates = [page]
        try:
            inhalt = page.frame(name="inhalt")
        except Exception:
            inhalt = None
        if inhalt:
            candidates.append(inhalt)
        candidates.extend(page.frames)

        for target in candidates:
            for selector in selectors:
                button = target.locator(selector).first
                try:
                    if button.count() == 0:
                        continue
                except Exception:
                    continue
                try:
                    button.scroll_into_view_if_needed()
                except Exception:
                    pass
                try:
                    button.click()
                    print("[OK] 'Unterlage hinzufügen' geklickt.")
                    try:
                        page.wait_for_selector("#bezeichnung", timeout=6000)
                    except Exception:
                        pass
                    return True
                except Exception as exc:
                    last_error = exc
                    continue
            try:
                clicked = target.evaluate(
                    """() => {
                        const buttons = Array.from(document.querySelectorAll('button'));
                        const match = buttons.find((btn) =>
                            (btn.textContent || '').toLowerCase().includes('unterlage hinzufügen') ||
                            (btn.textContent || '').toLowerCase().includes('unterlage hinzufuegen') ||
                            (btn.getAttribute('onclick') || '').includes('einzureichendes_editor') ||
                            (btn.getAttribute('onclick') || '').includes('openUiWindowReloaded')
                        );
                        if (!match) return false;
                        match.scrollIntoView({ block: 'center' });
                        match.click();
                        return true;
                    }"""
                )
                if clicked:
                    print("[OK] 'Unterlage hinzufügen' geklickt (JS fallback).")
                    try:
                        page.wait_for_selector("#bezeichnung", timeout=6000)
                    except Exception:
                        pass
                    return True
            except Exception as exc:
                last_error = exc
        time.sleep(0.4)

    if last_error:
        print(f"[WARNUNG] Button 'Unterlage hinzufügen' nicht gefunden (letzter Fehler: {last_error}).")
    else:
        print("[WARNUNG] Button 'Unterlage hinzufügen' nicht gefunden.")
    return False


def _fill_unterlage_modal_and_save(page, entry: dict) -> bool:
    bezeichnung_text = str(entry.get("bezeichnung") or "Unterlage").strip()
    if bezeichnung_text.strip().lower() in NON_EINZUREICHENDE_LABELS:
        print(f"[INFO] Unterlage übersprungen (nicht eintragen): {bezeichnung_text}")
        return True
    gueltig_bis = str(entry.get("gueltig_bis") or "").strip()
    vorhanden = bool(entry.get("vorhanden"))

    candidates = [page]
    try:
        inhalt = page.frame(name="inhalt")
    except Exception:
        inhalt = None
    if inhalt:
        candidates.append(inhalt)

    target = None
    for candidate in candidates:
        bezeichnung_input = candidate.locator("#bezeichnung").first
        try:
            if bezeichnung_input.count() > 0:
                target = candidate
                break
        except Exception:
            continue

    if target is None:
        print("[WARNUNG] Modal für 'Einzureichende Unterlage' nicht gefunden.")
        return False

    try:
        target.locator("#bezeichnung").first.fill(bezeichnung_text)
        if gueltig_bis:
            target.locator("#gueltigBis").first.evaluate(
                """(el, val) => {
                    el.value = val;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    el.dispatchEvent(new Event('blur', { bubbles: true }));
                }""",
                gueltig_bis,
            )
        else:
            target.locator("#gueltigBis").first.fill("")
        # Datepicker-Overlay schließen, damit es keine Klicks blockiert.
        target.evaluate(
            """() => {
                const dp = document.querySelector('#ui-datepicker-div');
                if (dp) dp.style.display = 'none';
                if (document.activeElement) document.activeElement.blur();
            }"""
        )
        target.locator("#vorhanden").first.evaluate(
            """(el, checked) => {
                el.checked = Boolean(checked);
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            }""",
            vorhanden,
        )
        save_button = target.locator("button:has-text('Speichern')").first
        try:
            save_button.click()
        except Exception:
            target.evaluate(
                """() => {
                    const btn = Array.from(document.querySelectorAll('button'))
                        .find(b => (b.textContent || '').toLowerCase().includes('speichern'));
                    if (btn) btn.click();
                }"""
            )
        print(
            f"[OK] Modal gespeichert: bezeichnung={bezeichnung_text}, "
            f"gueltigBis={gueltig_bis or '—'}, vorhanden={'Ja' if vorhanden else 'Nein'}"
        )
        return True
    except Exception as exc:
        print(f"[WARNUNG] Modal konnte nicht gespeichert werden: {exc}")
        return False


def _click_bild_aendern(page) -> bool:
    selectors = [
        "button:has-text('Bild ändern')",
        "button:has-text('Bild aendern')",
    ]
    deadline = time.time() + 12
    while time.time() < deadline:
        candidates = [page]
        inhalt = page.frame(name="inhalt")
        if inhalt:
            candidates.append(inhalt)
        candidates.extend(page.frames)

        for target in candidates:
            for selector in selectors:
                try:
                    button = target.locator(selector).first
                    if button.count() == 0:
                        continue
                    try:
                        button.scroll_into_view_if_needed()
                    except Exception:
                        pass
                    button.click()
                    print("[OK] 'Bild ändern' geklickt.")
                    return True
                except Exception:
                    # Frame kann während Reload/Submit detached sein; dann frisch versuchen.
                    continue
        time.sleep(0.25)
    print("[WARNUNG] Button 'Bild ändern' nicht gefunden.")
    return False


def _upload_image(page, image_path: Path) -> bool:
    if not image_path.exists():
        print(f"[WARNUNG] Bilddatei nicht gefunden: {image_path}")
        return False

    deadline = time.time() + 12
    while time.time() < deadline:
        candidates = [page]
        inhalt = page.frame(name="inhalt")
        if inhalt:
            candidates.append(inhalt)
        candidates.extend(page.frames)

        for target in candidates:
            try:
                file_input = target.locator("#fileupload").first
                if file_input.count() == 0:
                    continue
                file_input.set_input_files(str(image_path))
                try:
                    file_input.evaluate("el => el.files && el.files.length")
                except Exception:
                    pass
                _wait_for_profile_preview(target, timeout_seconds=6.0)
                time.sleep(0.4)
                print(f"[OK] Bild hochgeladen: {image_path}")
                return True
            except Exception:
                continue
        time.sleep(0.25)

    print("[WARNUNG] Upload-Feld '#fileupload' nicht gefunden.")
    return False


def _save_uploaded_image(page) -> bool:
    deadline = time.time() + 20
    while time.time() < deadline:
        candidates = [page]
        inhalt = page.frame(name="inhalt")
        if inhalt:
            candidates.append(inhalt)
        candidates.extend(page.frames)

        modal_still_open = False
        for target in candidates:
            try:
                if target.locator("#fileupload").count() > 0:
                    modal_still_open = True
                button = target.locator("button[onclick*='xajax_speicher_bild']").first
                if button.count() == 0:
                    button = target.locator("button:has-text('Speichern')").first
                if button.count() == 0:
                    continue
                try:
                    button.click(force=True)
                except Exception:
                    target.evaluate(
                        """() => {
                            const direct = document.querySelector("button[onclick*='xajax_speicher_bild']");
                            if (direct) {
                                direct.click();
                                return;
                            }
                            const fallback = Array.from(document.querySelectorAll('button'))
                                .find(b => (b.textContent || '').trim().toLowerCase() === 'speichern');
                            if (fallback) fallback.click();
                        }"""
                    )
                print("[INFO] Klick auf Bild-Dialog 'Speichern' ausgeführt.")
            except Exception:
                continue

        if not modal_still_open:
            print("[OK] Bild-Dialog geschlossen.")
            return True

        time.sleep(0.6)

    print("[WARNUNG] Bild-Dialog blieb offen (Timeout beim wiederholten Speichern).")
    return False


def run_mitarbeiterinformationen(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    wait_seconds: int = 45,
):
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    state_path = Path(config.STATE_PATH)
    if not state_path.exists():
        raise RuntimeError(f"[FEHLER] Kein gespeicherter Login-State unter {state_path}. Bitte zuerst 'login' ausführen.")

    payload = _load_personalbogen_json()
    email = str(payload.get("email", "")).strip()
    if not email:
        raise RuntimeError("[FEHLER] Keine E-Mail im personalbogen-JSON gefunden.")
    unterlagen = _build_unterlagen_from_payload(payload)
    if unterlagen:
        before = len(unterlagen)
        unterlagen = [
            u
            for u in unterlagen
            if str(u.get("key") or "").strip().lower() not in NON_EINZUREICHENDE_UNTERLAGEN
            and str(u.get("bezeichnung") or "").strip().lower() not in NON_EINZUREICHENDE_LABELS
        ]
        removed = before - len(unterlagen)
        if removed:
            print(f"[INFO] Unterlagen-Filter: {removed} Einträge ausgeschlossen (nicht eintragen).")

    max_retries = int(os.environ.get("PERSONAL_SCRAPER_MAX_RETRIES", "2"))
    if max_retries > 2:
        max_retries = 2
    attempts = max_retries + 1

    for attempt in range(1, attempts + 1):
        tracker = FieldTracker(attempt=attempt, max_retries=max_retries)
        print(f"[INFO] Versuch {attempt}/{attempts} gestartet.")
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        prev_stdout = sys.stdout
        prev_stderr = sys.stderr
        sys.stdout = _Tee(prev_stdout, stdout_buffer)
        sys.stderr = _Tee(prev_stderr, stderr_buffer)
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=headless, slow_mo=slowmo_ms)
                context = browser.new_context(storage_state=str(state_path))
                context.add_init_script(
                    """() => {
                        const deny = async () => {
                            const error = new Error('Permission denied');
                            error.name = 'NotAllowedError';
                            throw error;
                        };
                        if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
                            navigator.mediaDevices.getUserMedia = deny;
                        }
                        if (navigator.permissions && navigator.permissions.query) {
                            const originalQuery = navigator.permissions.query.bind(navigator.permissions);
                            navigator.permissions.query = (params) => {
                                if (params && (params.name === 'camera' || params.name === 'microphone')) {
                                    return Promise.resolve({
                                        state: 'denied',
                                        onchange: null,
                                        addEventListener: () => {},
                                        removeEventListener: () => {},
                                        dispatchEvent: () => false,
                                    });
                                }
                                return originalQuery(params);
                            };
                        }
                    }"""
                )
                page = context.new_page()

                print("[INFO] Lade Startseite mit gespeicherter Session …")
                page.goto(config.BASE_URL, wait_until="domcontentloaded")

                try:
                    target = _open_user_overview(page)
                except Exception as exc:
                    print(f"[WARNUNG] Übersicht nicht geladen (Session evtl. abgelaufen): {exc} – versuche Login …")
                    page = context.new_page()
                    do_login(page)
                    target = _open_user_overview(page)

                search_input = _locate_search_input(target)
                if search_input.count() == 0:
                    raise RuntimeError("[FEHLER] Suchfeld in user.php nicht gefunden.")

                search_input.fill(email)
                time.sleep(0.2)
                print(f"[INFO] Suche nach E-Mail: {email}")

                target_page = _click_lastname_link(target, email)
                if not target_page:
                    print("[INFO] Kein Treffer geklickt – keine Pause.")
                    browser.close()
                    return

                if _open_mitarbeiterinformationen(target_page):
                    print("[OK] Mitarbeiterinformationen geöffnet.")
                    _clear_einzureichende_unterlagen(target_page, skip=attempt > 1)
                    dokumente = _extract_documents_table(target_page)
                    unterlagen = _enrich_unterlagen_from_documents(unterlagen, dokumente)
                    for unterlage in unterlagen:
                        label = str(unterlage.get("bezeichnung") or "").strip()
                        key = str(unterlage.get("key") or "").strip().lower()
                        if key in NON_EINZUREICHENDE_UNTERLAGEN or label.lower() in NON_EINZUREICHENDE_LABELS:
                            print(f"[INFO] Unterlage übersprungen (nicht eintragen): {label or key}")
                            tracker.skip("unterlagen", label or key, "nicht eintragen", "übersprungen")
                            continue
                        valid_until = str(unterlage.get("gueltig_bis") or "").strip()
                        if _unterlage_exists(target_page, label, valid_until=valid_until):
                            print(f"[INFO] Unterlage bereits vorhanden – überspringe: {label}")
                            tracker.skip("unterlagen", label or unterlage.get("key", ""), "vorhanden", "vorhanden")
                            continue
                        if _click_unterlage_hinzufuegen(target_page):
                            time.sleep(0.4)
                            _fill_unterlage_modal_and_save(target_page, unterlage)
                            time.sleep(0.2)
                        else:
                            print(f"[WARNUNG] Unterlage konnte nicht angelegt werden: {unterlage.get('bezeichnung')}")
                        if _unterlage_exists(target_page, label, valid_until=valid_until):
                            tracker.ok("unterlagen", label or unterlage.get("key", ""), "vorhanden", "vorhanden")
                        else:
                            tracker.missing("unterlagen", label or unterlage.get("key", ""), "vorhanden", "fehlend")
                    # Always replace profile image, even if one seems present.
                    if _click_bild_aendern(target_page):
                        with tempfile.TemporaryDirectory(prefix="perso-profilbild-") as tmp:
                            image_path = _resolve_profile_image(payload, Path(tmp))
                            if image_path:
                                image_path = _normalize_profile_image(image_path) or image_path
                                time.sleep(0.5)
                                if _upload_image(target_page, image_path):
                                    time.sleep(0.3)
                                    _save_uploaded_image(target_page)
                            else:
                                print("[WARNUNG] Kein Profilbild im Personalbogen gefunden.")
                        if _has_profile_image(target_page):
                            tracker.ok("profilbild", "profilbild", "vorhanden", "vorhanden")
                        else:
                            tracker.missing("profilbild", "profilbild", "vorhanden", "fehlend")
                    else:
                        print("[WARNUNG] Button 'Bild ändern' nicht verfügbar.")
                        tracker.missing("profilbild", "profilbild", "vorhanden", "fehlend")
                    print(f"[INFO] Pause für manuelle Schritte ({wait_seconds}s) …")
                    time.sleep(max(1, wait_seconds))
                else:
                    print("[WARNUNG] Mitarbeiterinformationen konnten nicht geöffnet werden.")
                    tracker.missing("run", "mitarbeiterinformationen", "geöffnet", "fehlgeschlagen")

                browser.close()

            sys.stdout = prev_stdout
            sys.stderr = prev_stderr
            combined_log = stdout_buffer.getvalue() + "\n" + stderr_buffer.getvalue()
            if _has_retry_warning(combined_log):
                tracker.missing("run", "warnung", "keine warnung", "warnung erkannt")
            tracker.log_summary()
            missing = tracker.missing_fields()
            if not missing:
                print("[INFO] Alle Felder gesetzt – Erfolg.")
                return
            if attempt <= max_retries:
                print("[WARNUNG] Fehlende Felder gefunden – starte Retry …")
                continue
            raise RuntimeError(f"Fehlende Felder nach {attempts} Versuchen: {missing}")
        except Exception as exc:
            sys.stdout = prev_stdout
            sys.stderr = prev_stderr
            tracker.error("run", "exception", str(exc))
            tracker.log_summary()
            if attempt <= max_retries:
                print(f"[WARNUNG] Fehler in Versuch {attempt}: {exc} – retry …")
                continue
            raise
