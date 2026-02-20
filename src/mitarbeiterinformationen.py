import time
import tempfile
import requests
import base64
import re
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


def _build_unterlagen_from_payload(payload: dict) -> list[dict]:
    uploads = payload.get("uploads") if isinstance(payload, dict) else {}
    uploads = _normalize_uploads(uploads)
    if not isinstance(uploads, dict):
        return []

    required_keys = _build_required_upload_keys(payload)
    required_set = set(required_keys)
    skip_keys = {"personalbogen", "vertrag", "arbeitsvertrag", "zusatzvereinbarung", "sicherheitsbelehrung"}

    # Feste Reihenfolge, damit die Einträge in der Akte reproduzierbar sind.
    preferred_order = [
        "sicherheitsbelehrung",
        "inventionsschutz",
        "immatrikulation",
        "infektionsschutz",
        "aufenthaltserlaubnis",
        "arbeitserlaubnis",
        "rentenbefreiung",
    ]
    ordered_keys = [key for key in preferred_order if key in required_set or key in uploads]
    ordered_keys.extend([key for key in required_keys if key not in ordered_keys])
    ordered_keys.extend([key for key in uploads.keys() if key not in ordered_keys])

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


def _clear_einzureichende_unterlagen(page) -> None:
    candidates = [page]
    inhalt = page.frame(name="inhalt")
    if inhalt:
        candidates.append(inhalt)
    candidates.extend(page.frames)

    target = None
    for candidate in candidates:
        if candidate.locator("#einzureichendes").count() > 0:
            target = candidate
            break

    if target is None:
        print("[WARNUNG] Tabelle 'Einzureichende Unterlagen' nicht gefunden.")
        return

    try:
        filter_all = target.locator("#alleUnterlagen").first
        if filter_all.count() > 0 and not filter_all.is_checked():
            filter_all.click()
            time.sleep(0.8)
    except Exception:
        pass

    try:
        target.wait_for_selector("#einzureichendes tbody tr", timeout=8000)
    except Exception:
        pass

    def _row_count() -> int:
        try:
            return target.locator("#einzureichendes tbody tr").count()
        except Exception:
            return 0

    def _log_state(prefix: str) -> None:
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

    removed = 0
    _log_state("Vor dem Löschen")
    max_loops = 200
    loops = 0
    while loops < max_loops:
        loops += 1
        buttons = target.locator("button[onclick*='maEinzureichendesLoeschen'], img.sprite_16x16.inaktiv")
        try:
            count = buttons.count()
        except Exception:
            count = 0
        if count == 0:
            break

        before_count = _row_count()
        try:
            info = buttons.first.evaluate(
                """(btn) => {
                    const row = btn.closest('tr');
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
            buttons.first.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            buttons.first.click()
            removed += 1
        except Exception as exc:
            print(f"[WARNUNG] Unterlage konnte nicht gelöscht/deaktiviert werden: {exc}")
            break

        try:
            target.wait_for_function(
                """(prev) => {
                    const rows = document.querySelectorAll('#einzureichendes tbody tr');
                    return rows.length < prev;
                }""",
                before_count,
                timeout=3000,
            )
        except Exception as exc:
            print(f"[DEBUG] Nach Löschung keine Zeilenänderung erkannt: {exc}")
            try:
                clicked = target.evaluate(
                    """() => {
                        const btn = document.querySelector("button[onclick*='maEinzureichendesLoeschen']");
                        if (btn) { btn.click(); return true; }
                        const img = document.querySelector("img.sprite_16x16.inaktiv");
                        if (img && img.closest('button')) { img.closest('button').click(); return true; }
                        return false;
                    }"""
                )
                if clicked:
                    print("[DEBUG] Lösch-Klick per JS-Fallback ausgeführt.")
            except Exception as js_exc:
                print(f"[DEBUG] JS-Fallback Lösch-Klick fehlgeschlagen: {js_exc}")
        time.sleep(0.2)

    _log_state("Nach dem Löschen")
    print(f"[INFO] Einzureichende Unterlagen entfernt/deaktiviert: {removed}")
    try:
        target.wait_for_selector("#einzureichendes", timeout=6000)
    except Exception:
        pass


def _normalize_doc_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    return re.sub(r"[^a-z0-9]+", "", text)


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
            target_path.write_bytes(base64.b64decode(b64_data))
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
    target_path = temp_dir / f"profilbild{ext}"
    target_path.write_bytes(response.content)
    return target_path


def _click_unterlage_hinzufuegen(page) -> bool:
    selectors = [
        "button:has-text('Unterlage hinzufügen')",
        "button:has-text('Unterlage hinzufuegen')",
        "button[onclick*='openUiWindowReloaded'][title*='Unterlage']",
        "button[onclick*='einzureichendes_editor']",
    ]

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
                print(f"[WARNUNG] Klick auf 'Unterlage hinzufügen' fehlgeschlagen: {exc}")
                return False
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
        except Exception:
            pass
    print("[WARNUNG] Button 'Unterlage hinzufügen' nicht gefunden.")
    return False


def _fill_unterlage_modal_and_save(page, entry: dict) -> bool:
    bezeichnung_text = str(entry.get("bezeichnung") or "Unterlage").strip()
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
            _clear_einzureichende_unterlagen(target_page)
            dokumente = _extract_documents_table(target_page)
            unterlagen = _enrich_unterlagen_from_documents(unterlagen, dokumente)
            for unterlage in unterlagen:
                if _click_unterlage_hinzufuegen(target_page):
                    time.sleep(0.4)
                    _fill_unterlage_modal_and_save(target_page, unterlage)
                    time.sleep(0.2)
                else:
                    print(f"[WARNUNG] Unterlage konnte nicht angelegt werden: {unterlage.get('bezeichnung')}")
            if _click_bild_aendern(target_page):
                with tempfile.TemporaryDirectory(prefix="perso-profilbild-") as tmp:
                    image_path = _resolve_profile_image(payload, Path(tmp))
                    if image_path:
                        time.sleep(0.5)
                        if _upload_image(target_page, image_path):
                            time.sleep(0.3)
                            _save_uploaded_image(target_page)
                    else:
                        print("[WARNUNG] Kein Profilbild im Personalbogen gefunden.")
            print(f"[INFO] Pause für manuelle Schritte ({wait_seconds}s) …")
            time.sleep(max(1, wait_seconds))
        else:
            print("[WARNUNG] Mitarbeiterinformationen konnten nicht geöffnet werden.")

        browser.close()
