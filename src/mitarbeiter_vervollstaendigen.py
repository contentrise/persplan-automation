import json
import os
import re
import time
import tempfile
import shutil
import sys
import io
from pathlib import Path
from typing import Union
from urllib.parse import parse_qs, urljoin, urlparse

from playwright.sync_api import Frame, Locator, Page, TimeoutError, sync_playwright
import requests

from src import config
from src.login import do_login


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

def _extract_bn(value: str) -> str:
    if not value:
        return ""
    trimmed = value.strip()
    if re.fullmatch(r"[A-Za-z0-9_]+", trimmed) and len(trimmed) >= 5:
        return trimmed
    match = re.search(r"\[Bn:\s*([^\]]+)\]", value)
    if match:
        return match.group(1).strip()
    match = re.search(r"\b(BN|Bn)\s*[:\-]?\s*([A-Za-z0-9_]+)\b", value)
    if match:
        return match.group(2).strip()
    return ""


def _normalize_kasse_name(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = (
        text.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
        .replace("&", "und")
    )
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


_KRANKENKASSE_OPTIONS = [
    "SVLFG, Landwirtschaftliche Krankenkasse Geschäftsstelle Kiel [Bn: 13199426]",
    "IKK - Die Innovationskasse Rechtskreis West und Ost [Bn: 14228571]",
    "Techniker Krankenkasse -Rechtskreis West und Ost- [Bn: 15027365]",
    "HEK Hanseatische Krankenkasse [Bn: 15031806]",
    "Mobil Krankenkasse [Bn: 15517302]",
    "SECURVITA BKK [Bn: 15517482]",
    "pronova BKK [Bn: 15872672]",
    "AOK Bremen/Bremerhaven [Bn: 20012084]",
    "hkk Handelskrankenkasse [Bn: 20013461]",
    "BKK Salzgitter [Bn: 21203214]",
    "KKH Kaufmännische Krankenkasse [Bn: 29137937]",
    "SVLFG, Landwirtschaftliche Krankenkasse Geschäftsstelle Hannover [Bn: 29147110]",
    "energie-BKK Hauptverwaltung [Bn: 29717581]",
    "AOK Niedersachsen. Die Gesundheitskasse. [Bn: 29720865]",
    "Heimat Krankenkasse [Bn: 31209131]",
    "Bertelsmann BKK [Bn: 31323584]",
    "BKK Diakonie [Bn: 31323686]",
    "BKK DürkoppAdler [Bn: 31323799]",
    "AOK NordWest [Bn: 33526082]",
    "Continentale Betriebskrankenkasse [Bn: 33865367]",
    "Augenoptiker Ausgleichskasse VVaG [Bn: 33868451]",
    "AOK Rheinland/Hamburg Die Gesundheitskasse [Bn: 34364249]",
    "BKK Deutsche Bank AG [Bn: 34401277]",
    "NOVITAS Betriebskrankenkasse [Bn: 35134022]",
    "bkk melitta hmr [Bn: 36916935]",
    "SVLFG, Landwirtschaftliche Krankenkasse Geschäftsstelle Münster [Bn: 39873587]",
    "VIACTIV Krankenkasse [Bn: 40180080]",
    "BERGISCHE KRANKENKASSE [Bn: 42039708]",
    "BARMER (vormals BARMER GEK) [Bn: 42938966]",
    "BKK Werra-Meissner [Bn: 44037562]",
    "Salus BKK [Bn: 44953697]",
    "AOK Hessen Direktion [Bn: 45118687]",
    "EY Betriebskrankenkasse [Bn: 46939789]",
    "BKK Wirtschaft & Finanzen [Bn: 46967693]",
    "BKK Herkules vorher BKK Wegmann bis 31.12.2000 [Bn: 47034953]",
    "BKK B. Braun Aesculap [Bn: 47034975]",
    "SVLFG, Landwirtschaftliche Krankenkasse Geschäftsstelle Darmstadt [Bn: 47068420]",
    "SVLFG, Landwirtschaftliche Krankenkasse Geschäftsstelle Kassel [Bn: 47069693]",
    "Betriebskrankenkasse PricewaterhouseCoopers [Bn: 47307817]",
    "KARL MAYER Betriebskrankenkasse [Bn: 48063096]",
    "DAK-Gesundheit [Bn: 48698890]",
    "R+V Betriebskrankenkasse [Bn: 48944809]",
    "BAHN-BKK [Bn: 49003443]",
    "BKK PFAFF [Bn: 51588416]",
    "AOK Rheinland-Pfalz/Saarland [Bn: 51605725]",
    "Betriebskrankenkasse der Energieversorgung Mittelrhein [Bn: 51980490]",
    "Debeka BKK [Bn: 52156763]",
    "BKK Pfalz [Bn: 52598579]",
    "Betriebskrankenkasse Groz-Beckert [Bn: 60393261]",
    "mhplus Betriebskrankenkasse West [Bn: 63494759]",
    "vivida bkk [Bn: 66458477]",
    "BKK Schwarzwald-Baar-Heuberg [Bn: 66614249]",
    "BKK Rieker.RICOSTA.Weisser [Bn: 66626976]",
    "AOK Baden-Württemberg Hauptverwaltung [Bn: 67450665]",
    "MAHLE BKK [Bn: 67572537]",
    "SVLFG, Landwirtschaftliche Krankenkasse Geschäftsstelle Stuttgart [Bn: 67574619]",
    "BKK Akzo Nobel Bayern [Bn: 71579930]",
    "SVLFG, Landwirtschaftliche Krankenkasse Geschäftsstelle Bayreuth [Bn: 72360029]",
    "Koenig & Bauer BKK [Bn: 75925585]",
    "Audi BKK [Bn: 82889062]",
    "BKK Faber-Castell & Partner [Bn: 86772584]",
    "SVLFG, Landwirtschaftliche Krankenkasse Geschäftsstelle Landshut [Bn: 87119868]",
    "BMW BKK Zentrale [Bn: 87271125]",
    "AOK Bayern Die Gesundheitskasse [Bn: 87880235]",
    "BKK ProVita [Bn: 88571250]",
    "AOK Nordost - Die Gesundheitskasse [Bn: 90235319]",
    "BKK mkk - meine krankenkasse [Bn: 92644250]",
    "Knappschaft Hauptverwaltung [Bn: 98000006]",
    "Knappschaft Hauptverwaltung [Bn: 98094032]",
    "AOK PLUS Die Gesundheitskasse [Bn: 05174740]",
    "AOK Sachsen-Anhalt [Bn: 01029141]",
    "IKK Brandenburg und Berlin [Bn: 01020803]",
    "IKK classic -Rechtskreis Ost und West- [Bn: 01049203]",
    "IKK gesund plus (Ost) Hauptverwaltung [Bn: 01000455]",
    "SVLFG, Landwirtschaftliche Krankenkasse Geschäftsstelle Kassel (ehemals Gartenbau) [Bn: 01000650]",
    "SVLFG, Landwirtschaftliche Krankenkasse Geschäftsstelle Hoppegarten [Bn: 01000308]",
    "Allianz Private Krankenversicherung [Bn: PRIVAT_1]",
    "Alte Oldenburger Krankenversicherung [Bn: PRIVAT_2]",
    "ARAG Krankenversicherung [Bn: PRIVAT_3]",
    "AXA Krankenkversicherung [Bn: PRIVAT_4]",
    "Barmenia Krankenversicherung [Bn: PRIVAT_5]",
    "Versicherungskammer Bayern [Bn: PRIVAT_6]",
    "Concordia Krankenversicherung [Bn: PRIVAT_7]",
    "Continentale Krankenversicherung [Bn: PRIVAT_8]",
    "DBV Deutsche Beamtenversicherung [Bn: PRIVAT_9]",
    "Debeka Krankenversicherungsverein [Bn: PRIVAT_10]",
    "Deutscher Ring Krankenversicherungsverein [Bn: PRIVAT_11]",
    "DEVK Krankenversicherung [Bn: PRIVAT_12]",
    "die Bayerische [Bn: PRIVAT_13]",
    "DKV - Deutsche Krankenversicherung [Bn: PRIVAT_14]",
    "Envivas Krankenversicherung [Bn: PRIVAT_15]",
    "ERGO Krankenversicherung [Bn: PRIVAT_16]",
    "Generali Krankenversicherung [Bn: PRIVAT_17]",
    "Gothaer Krankenversicherung [Bn: PRIVAT_18]",
    "HALLESCHE Krankenversicherung [Bn: PRIVAT_19]",
    "HanseMerkur Krankenversicherung [Bn: PRIVAT_20]",
    "HUK-Coburg Krankenversicherung [Bn: PRIVAT_21]",
    "Inter Krankenversicherung [Bn: PRIVAT_22]",
    "LKH Landeskrankenhilfe [Bn: PRIVAT_23]",
    "LVM Krankenversicherung [Bn: PRIVAT_24]",
    "Mecklenburgische Krankenversicherung [Bn: PRIVAT_25]",
    "Münchener Verein Krankenversicherung [Bn: PRIVAT_26]",
    "Nürnberger Krankenversicherung [Bn: PRIVAT_27]",
    "ottonova Krankenversicherung [Bn: PRIVAT_28]",
    "VGH Krankenversicherung [Bn: PRIVAT_29]",
    "R+V Krankenversicherung [Bn: PRIVAT_30]",
    "Signal Iduna Krankenversicherung [Bn: PRIVAT_31]",
    "Süddeutsche Krankenversicherung [Bn: PRIVAT_32]",
    "UKV Union Krankenversicherung [Bn: PRIVAT_33]",
    "Universa Krankenversicherung [Bn: PRIVAT_34]",
    "vigo Krankenversicherung [Bn: PRIVAT_35]",
    "VRK Krankenversicherung AG [Bn: PRIVAT_36]",
    "Württembergische Krankenversicherung [Bn: PRIVAT_37]",
]

_KRANKENKASSE_BN_MAP = {}
_KRANKENKASSE_LABEL_BY_NORM = {}
_KRANKENKASSE_LABEL_BY_BN = {}
for _entry in _KRANKENKASSE_OPTIONS:
    _bn = _extract_bn(_entry)
    _name = _entry.split("[Bn:", 1)[0].strip()
    _norm = _normalize_kasse_name(_name)
    if _norm and _bn:
        _KRANKENKASSE_BN_MAP[_norm] = _bn
        _KRANKENKASSE_LABEL_BY_NORM[_norm] = _entry
        _KRANKENKASSE_LABEL_BY_BN[_bn] = _entry


def _resolve_bn_from_name(value: str) -> str:
    normalized = _normalize_kasse_name(value)
    if not normalized:
        return ""
    return _KRANKENKASSE_BN_MAP.get(normalized, "")


def _resolve_kasse_label(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "[Bn:" in text or "[BN:" in text:
        return text
    normalized = _normalize_kasse_name(text)
    if not normalized:
        return text
    return _KRANKENKASSE_LABEL_BY_NORM.get(normalized, text)


def _wait_for_inhalt_frame(page: Page, timeout_seconds: int = 5) -> Frame | None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        frame = page.frame(name="inhalt")
        if frame:
            return frame
        time.sleep(0.2)
    return None


def _get_user_id_from_url(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query or "")
        return str(qs.get("user_id", [""])[0] or "")
    except Exception:
        return ""


def _load_personalbogen_json() -> dict:
    input_dir = Path(os.environ.get("PERSO_INPUT_DIR", "perso-input"))
    candidates = list(input_dir.glob("*.json"))
    if not candidates:
        raise FileNotFoundError("[FEHLER] Keine JSON-Datei in 'perso-input' gefunden.")
    if len(candidates) > 1:
        raise FileNotFoundError("[FEHLER] Mehr als eine JSON-Datei in 'perso-input' gefunden.")
    json_path = candidates[0]
    print(f"[INFO] Verwende JSON-Datei: {json_path}")
    with json_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("[FEHLER] JSON-Datei muss ein Objekt sein.")
    if isinstance(payload.get("fragebogen"), dict):
        normalized = dict(payload["fragebogen"])
        if isinstance(payload.get("vertrag"), dict):
            normalized["vertrag"] = payload["vertrag"]
        return normalized
    return payload


def _pick_payload_value(payload: dict, keys: list[str]) -> str:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            for entry in value:
                text = str(entry).strip()
                if text:
                    return text
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _open_user_overview(page: Page) -> Union[Frame, Page]:
    target_url = urljoin(config.BASE_URL, "user.php")
    print(f"[INFO] Öffne Benutzerübersicht: {target_url}")
    page.goto(target_url, wait_until="domcontentloaded", timeout=30000)

    frame = _wait_for_inhalt_frame(page)
    target: Union[Frame, Page] = frame if frame else page
    if frame:
        frame.goto(target_url, wait_until="domcontentloaded", timeout=30000)
        target = frame

    target.wait_for_selector(
        ".scn_datatable_outer_table_user_tbl, div.dataTables_filter, #user_tbl_filter",
        timeout=20000,
    )
    target.wait_for_selector(
        ".scn_datatable_outer_table_user_tbl input[type='search'], input[aria-controls='user_tbl'], input[type='search']",
        timeout=20000,
    )
    target.wait_for_selector(
        ".scn_datatable_outer_table_user_tbl table#user_tbl tbody tr, table#user_tbl tbody tr",
        timeout=20000,
    )
    # ensure filter is set to "Alle"
    try:
        filter_all = target.locator("#filter_anzeige_0").first
        if filter_all.count() > 0 and not filter_all.is_checked():
            filter_all.click()
            print("[OK] Filter auf 'Alle' gesetzt.")
            # allow table to refresh
            time.sleep(0.8)
    except Exception:
        pass
    return target


def _locate_search_input(target: Union[Frame, Page]):
    selectors = [
        ".scn_datatable_outer_table_user_tbl div.dataTables_filter input[type='search']",
        ".scn_datatable_outer_table_user_tbl input[aria-controls='user_tbl']",
        ".scn_datatable_outer_table_user_tbl input[type='search']",
        "div.dataTables_filter input[type='search']",
        "#user_tbl_filter input[type='search']",
        "input[aria-controls='user_tbl']",
        "input[type='search']",
    ]
    candidates: list[Union[Frame, Page]] = [target]
    if isinstance(target, Frame):
        try:
            candidates.append(target.page)
            candidates.extend(target.page.frames)
        except Exception:
            pass
    else:
        try:
            candidates.extend(target.frames)
        except Exception:
            pass

    for candidate in candidates:
        for sel in selectors:
            locator = candidate.locator(sel).first
            if locator.count() > 0:
                return locator
    return target.locator("input[type='search']").first


def _click_lastname_link(target: Union[Frame, Page], email: str) -> Page | None:
    rows = target.locator("table#user_tbl tbody tr")
    parent_page = target.page if isinstance(target, Frame) else target

    email_link = target.locator(f"a[href^='mailto:'][href*='{email}']")
    email_rows = rows.filter(has=email_link)
    row = email_rows.first if email_rows.count() > 0 else rows.first

    if row.count() == 0:
        print("[WARNUNG] Keine Zeilen in user_tbl gefunden.")
        return None

    link = row.locator("a.ma_akte_link_text, a.ma_akte_link_img").first
    if link.count() == 0:
        link = row.locator("a").first
    if link.count() == 0:
        print("[WARNUNG] Kein klickbarer Link in der Trefferzeile gefunden.")
        return None

    href = link.get_attribute("href") or ""
    if href:
        print("[AKTION] Öffne Mitarbeiterakte per Direktlink …")
        try:
            parent_page.goto(urljoin(config.BASE_URL, href), wait_until="domcontentloaded", timeout=20000)
            return parent_page
        except Exception as exc:
            print(f"[WARNUNG] Direktlink fehlgeschlagen: {exc}")

    print("[AKTION] Klicke Nachname in Trefferzeile …")

    try:
        with parent_page.context.expect_page(timeout=3000) as new_page_event:
            link.click()
        new_page = new_page_event.value
        new_page.wait_for_load_state("domcontentloaded", timeout=15000)
        return new_page
    except TimeoutError:
        pass

    link.click()
    deadline = time.time() + 12
    while time.time() < deadline:
        if "mitarbeiter_akte.php" in parent_page.url:
            return parent_page
        for frame in parent_page.frames:
            if "mitarbeiter_akte.php" in (frame.url or ""):
                return parent_page
            try:
                if frame.locator("#administration_user_stammdaten_tabs").count() > 0:
                    return parent_page
            except Exception:
                continue
        time.sleep(0.2)
    print(f"[DEBUG] Aktuelle URL (Page): {parent_page.url}")
    for idx, frame in enumerate(parent_page.frames):
        try:
            tabs = frame.locator("#administration_user_stammdaten_tabs").count()
        except Exception:
            tabs = -1
        print(f"[DEBUG] Frame {idx}: name={frame.name!r} url={frame.url!r} tabs={tabs}")
    if href:
        try:
            parent_page.goto(urljoin(config.BASE_URL, href), wait_until="domcontentloaded", timeout=15000)
        except Exception:
            pass
    return parent_page


def _open_lohnabrechnung_and_edit(page: Page) -> bool:
    try:
        target, panel = _open_stammdaten_tab(page, "lohnabrechnung", "Lohnabrechnung")
        if not target or not panel:
            print("[WARNUNG] Tab 'Lohnabrechnung' nicht gefunden.")
            return False
        edit_icon = panel.locator("img[src*='b_edit.png'][onclick*='makeEdited']").first
        if edit_icon.count() == 0:
            edit_icon = panel.locator("img[title='Bearbeiten']").first
        if edit_icon.count() == 0:
            edit_icon = target.locator("img[src*='b_edit.png'][onclick*='makeEdited']").first
        if edit_icon.count() == 0:
            edit_icon = target.locator("img[title='Bearbeiten']").first
        if edit_icon.count() == 0:
            print("[WARNUNG] Edit-Stift nicht gefunden.")
            return False
        try:
            edit_icon.scroll_into_view_if_needed()
        except Exception:
            pass
        edit_icon.click(force=True)
        print("[OK] Lohnabrechnung geöffnet und Edit-Stift geklickt.")
        try:
            target.evaluate(
                """() => {
                    if (typeof makeEdited === 'function') {
                        try { makeEdited(); } catch (e) {}
                    }
                    const panel = document.querySelector('#administration_user_stammdaten_tabs_lohnabrechnung');
                    if (!panel) return;
                    panel.querySelectorAll('input, select, textarea').forEach((el) => {
                        el.removeAttribute('readonly');
                        el.removeAttribute('disabled');
                    });
                    const save = panel.querySelector("input.speichern, input[type='submit'][value*='Daten speichern']");
                    if (save) {
                        save.classList.remove('hideElement');
                        save.style.display = 'inline-block';
                        save.removeAttribute('disabled');
                    }
                }"""
            )
        except Exception:
            pass
        return True
    except Exception as exc:
        print(f"[WARNUNG] Lohnabrechnung/Edit fehlgeschlagen: {exc}")
        return False


def _open_stammdaten_tab(
    page: Page,
    tab_key: str,
    label: str,
) -> tuple[Union[Frame, Page] | None, Locator | None]:
    panel_id = f"administration_user_stammdaten_tabs_{tab_key}"
    panel_selector = f"#{panel_id}"

    candidates: list[Union[Frame, Page]] = [page]
    inhalt = page.frame(name="inhalt")
    if inhalt:
        candidates.append(inhalt)
    candidates.extend(page.frames)

    try:
        page.wait_for_load_state("domcontentloaded", timeout=6000)
    except Exception:
        pass

    def _debug_tab_state(candidate: Union[Frame, Page]) -> None:
        try:
            info = candidate.evaluate(
                """() => {
                    const tabs = Array.from(document.querySelectorAll('ul.ui-tabs-nav li[role="tab"]'))
                        .map((li) => {
                            const anchor = li.querySelector('a');
                            const text = (anchor?.textContent || '').trim();
                            const aria = li.getAttribute('aria-controls') || '';
                            const href = anchor?.getAttribute('href') || '';
                            return `${text}|${aria}|${href}`;
                        });
                    const navExists = document.querySelector('ul.ui-tabs-nav') !== null;
                    return { navExists, tabs, location: window.location.href };
                }"""
            )
            print(f"[DEBUG] {label} Tabs: nav={info.get('navExists')}, tabs={info.get('tabs')}")
            print(f"[DEBUG] {label} Tabs URL: {info.get('location')}")
        except Exception as exc:
            print(f"[DEBUG] {label} Tabs: JS-Check fehlgeschlagen: {exc}")

    def _find_panel() -> tuple[Union[Frame, Page] | None, Locator | None, bool]:
        for candidate in candidates:
            try:
                panel = candidate.locator(panel_selector).first
                if panel.count() == 0:
                    continue
                try:
                    visible = panel.is_visible()
                except Exception:
                    visible = False
                return candidate, panel, visible
            except Exception:
                continue
        return None, None, False

    def _click_tab(candidate: Union[Frame, Page]) -> bool:
        tab_selectors = [
            f"#administration_user_stammdaten_tabs a[href='#{panel_id}']",
            f"ul.ui-tabs-nav li[aria-controls='{panel_id}'] a",
            f"li[role='tab'][aria-controls='{panel_id}'] a",
            f"li[role='tab']:has-text('{label}') a",
            f"li[role='tab'] a:has-text('{label}')",
            f"ul.ui-tabs-nav a:has-text('{label}')",
            f"a.ui-tabs-anchor:has-text('{label}')",
            f"a:has-text('{label}')",
        ]
        for selector in tab_selectors:
            candidate_tab = candidate.locator(selector).first
            if candidate_tab.count() == 0:
                continue
            try:
                _dismiss_ui_overlay(page)
                try:
                    page.locator("#loaderContainer").first.wait_for(state="hidden", timeout=3000)
                except Exception:
                    pass
            except Exception:
                pass
            try:
                candidate_tab.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                before_url = candidate.url if hasattr(candidate, "url") else ""
                before_user_id = _get_user_id_from_url(before_url)
                try:
                    candidate_tab.evaluate("el => el.click()")
                except Exception:
                    pass
                candidate_tab.click(force=True, timeout=3000)
                after_url = candidate.url if hasattr(candidate, "url") else ""
                after_user_id = _get_user_id_from_url(after_url)
                if before_user_id and after_user_id and before_user_id != after_user_id:
                    print(
                        f"[WARNUNG] {label} Tab-Klick änderte user_id ({before_user_id} -> {after_user_id}); "
                        "stelle ursprüngliche URL wieder her."
                    )
                    try:
                        candidate.goto(before_url, wait_until="domcontentloaded", timeout=15000)
                    except Exception:
                        pass
                    return False
                return True
            except Exception as exc:
                print(f"[DEBUG] {label} Tab-Klick fehlgeschlagen ({selector}): {exc}")
                continue
        try:
            clicked = candidate.evaluate(
                """(args) => {
                    const { label, panelId } = args || {};
                    const selectors = [
                        'ul.ui-tabs-nav a',
                        '.ui-tabs-nav a',
                        'li[role="tab"] a',
                        'a.ui-tabs-anchor',
                        'a'
                    ];
                    const anchors = selectors.flatMap((sel) => Array.from(document.querySelectorAll(sel)));
                    const match = anchors.find((a) => {
                        const text = (a.textContent || '').trim();
                        const href = a.getAttribute('href') || '';
                        return (label && text.includes(label)) || (panelId && href.includes(`#${panelId}`));
                    });
                    if (!match) return false;
                    match.scrollIntoView({ block: 'center' });
                    match.click();
                    return true;
                }""",
                {"label": label, "panelId": panel_id},
            )
            return bool(clicked)
        except Exception as exc:
            print(f"[DEBUG] {label} Tab-Klick JS fehlgeschlagen: {exc}")
            return False

    target, panel, panel_visible = _find_panel()
    if panel_visible and target and panel:
        print(f"[DEBUG] {label} Panel bereits sichtbar – Tab-Klick übersprungen.")
        return target, panel

    for candidate in candidates:
        _debug_tab_state(candidate)
        if _click_tab(candidate):
            target = candidate
            break

    if not target:
        return None, None

    panel = target.locator(panel_selector).first
    try:
        panel.wait_for(state="visible", timeout=8000)
    except Exception:
        return target, panel if panel.count() > 0 else None
    return target, panel


def _open_sedcard(page: Page) -> bool:
    deadline = time.time() + 10
    last_frames: list[Frame] = []
    while time.time() < deadline:
        candidates: list[Union[Frame, Page]] = [page]
        inhalt = page.frame(name="inhalt")
        if inhalt:
            candidates.append(inhalt)
        last_frames = page.frames
        candidates.extend(last_frames)

        for target in candidates:
            link = target.locator("#tableOfSubmenue a:has-text('Sedcard')").first
            if link.count() == 0:
                link = target.locator("a:has-text('Sedcard')").first
            if link.count() == 0:
                continue
            try:
                link.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                _dismiss_ui_overlay(page)
                try:
                    page.locator("#loaderContainer").first.wait_for(state="hidden", timeout=3000)
                except Exception:
                    pass
                try:
                    link.click(force=True)
                except Exception:
                    link.evaluate("el => el.click()")
                print("[OK] Submenü 'Sedcard' geklickt.")
                time.sleep(0.5)
                return True
            except Exception as exc:
                try:
                    clicked = target.evaluate(
                        """() => {
                            const link = document.querySelector("#tableOfSubmenue a:contains('Sedcard')") ||
                                         Array.from(document.querySelectorAll("#tableOfSubmenue a"))
                                              .find(a => (a.textContent || '').trim() === 'Sedcard');
                            if (link) { link.click(); return true; }
                            return false;
                        }"""
                    )
                except Exception:
                    clicked = False
                if clicked:
                    print("[OK] Submenü 'Sedcard' geklickt (JS fallback).")
                    time.sleep(0.5)
                    return True
                print(f"[WARNUNG] Submenü 'Sedcard' Klick fehlgeschlagen: {exc}")
                return False
        time.sleep(0.25)

    print("[WARNUNG] Submenü-Link 'Sedcard' nicht gefunden.")
    for idx, frame in enumerate(last_frames):
        try:
            count = frame.locator("a:has-text('Sedcard')").count()
        except Exception:
            count = -1
        print(f"[DEBUG] Frame {idx}: name={frame.name!r} url={frame.url!r} sedcard_links={count}")
    return False


def _open_vertragsdaten(page: Page) -> bool:
    deadline = time.time() + 10
    last_frames: list[Frame] = []
    while time.time() < deadline:
        candidates: list[Union[Frame, Page]] = [page]
        inhalt = page.frame(name="inhalt")
        if inhalt:
            candidates.append(inhalt)
        last_frames = page.frames
        candidates.extend(last_frames)

        for target in candidates:
            link = target.locator("#tableOfSubmenue a:has-text('Vertragsdaten')").first
            if link.count() == 0:
                link = target.locator("a:has-text('Vertragsdaten')").first
            if link.count() == 0:
                continue
            try:
                link.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                _dismiss_ui_overlay(page)
                try:
                    page.locator("#loaderContainer").first.wait_for(state="hidden", timeout=3000)
                except Exception:
                    pass
                try:
                    link.click(force=True)
                except Exception:
                    link.evaluate("el => el.click()")
                print("[OK] Submenü 'Vertragsdaten' geklickt.")
                time.sleep(0.5)
                return True
            except Exception as exc:
                try:
                    clicked = target.evaluate(
                        """() => {
                            const link = document.querySelector("#tableOfSubmenue a:contains('Vertragsdaten')") ||
                                         Array.from(document.querySelectorAll("#tableOfSubmenue a"))
                                              .find(a => (a.textContent || '').trim() === 'Vertragsdaten');
                            if (link) { link.click(); return true; }
                            return false;
                        }"""
                    )
                except Exception:
                    clicked = False
                if clicked:
                    print("[OK] Submenü 'Vertragsdaten' geklickt (JS fallback).")
                    time.sleep(0.5)
                    return True
                print(f"[WARNUNG] Submenü 'Vertragsdaten' Klick fehlgeschlagen: {exc}")
                return False
        time.sleep(0.25)

    print("[WARNUNG] Submenü-Link 'Vertragsdaten' nicht gefunden.")
    for idx, frame in enumerate(last_frames):
        try:
            count = frame.locator("a:has-text('Vertragsdaten')").count()
        except Exception:
            count = -1
        print(f"[DEBUG] Frame {idx}: name={frame.name!r} url={frame.url!r} vertragsdaten_links={count}")
    return False


def _open_mitarbeiterinformationen(page: Page) -> bool:
    deadline = time.time() + 10
    last_frames: list[Frame] = []
    while time.time() < deadline:
        candidates: list[Union[Frame, Page]] = [page]
        inhalt = page.frame(name="inhalt")
        if inhalt:
            candidates.append(inhalt)
        last_frames = page.frames
        candidates.extend(last_frames)

        for target in candidates:
            link = target.locator("#tableOfSubmenue a:has-text('Mitarbeiterinformationen')").first
            if link.count() == 0:
                link = target.locator("a:has-text('Mitarbeiterinformationen')").first
            if link.count() == 0:
                continue
            try:
                link.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                _dismiss_ui_overlay(page)
                try:
                    link.click(force=True)
                except Exception:
                    link.evaluate("el => el.click()")
                print("[OK] Submenü 'Mitarbeiterinformationen' geklickt.")
                time.sleep(0.5)
                return True
            except Exception as exc:
                print(f"[WARNUNG] Submenü 'Mitarbeiterinformationen' Klick fehlgeschlagen: {exc}")
                return False
        time.sleep(0.25)

    print("[WARNUNG] Submenü-Link 'Mitarbeiterinformationen' nicht gefunden.")
    for idx, frame in enumerate(last_frames):
        try:
            count = frame.locator("a:has-text('Mitarbeiterinformationen')").count()
        except Exception:
            count = -1
        print(f"[DEBUG] Frame {idx}: name={frame.name!r} url={frame.url!r} mitarbeiterinformationen_links={count}")
    return False


def _enter_sedcard_edit_mode(page: Page) -> bool:
    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    edit_icon = target.locator("img.edit[onclick*='makeEdited'], img[title='Bearbeiten']").first
    if edit_icon.count() == 0:
        print("[WARNUNG] Sedcard-Edit-Stift nicht gefunden.")
        return False
    _log_locator_state(edit_icon, "sedcard edit icon (vor)")
    try:
        edit_icon.scroll_into_view_if_needed()
    except Exception:
        pass
    try:
        edit_icon.click(force=True, timeout=3000)
        print("[OK] Sedcard-Edit-Stift geklickt.")
    except Exception as exc:
        try:
            clicked = edit_icon.evaluate("el => { el.click(); return true; }")
        except Exception:
            clicked = False
        if clicked:
            print("[OK] Sedcard-Edit-Stift per JS geklickt.")
        else:
            print(f"[WARNUNG] Sedcard-Edit-Stift Klick fehlgeschlagen: {exc}")
            _log_locator_state(edit_icon, "sedcard edit icon (fehler)")
            return False

    try:
        target.evaluate(
            """() => {
                if (typeof makeEdited === 'function') {
                    try { makeEdited(); } catch (e) {}
                }
                document.querySelectorAll('input, select, textarea').forEach((el) => {
                    el.removeAttribute('readonly');
                    el.removeAttribute('disabled');
                });
            }"""
        )
    except Exception:
        pass

    probe = target.locator("#groesse, [name='groesse']").first
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            if probe.count() == 0:
                break
            disabled = probe.evaluate("el => el.disabled")
            if not disabled:
                break
        except Exception:
            pass
        time.sleep(0.2)
    return True


def _set_yes_no_select(locator, value: str) -> bool:
    if locator.count() == 0:
        return False
    normalized = str(value).strip().lower()
    if normalized in ["ja", "yes", "true", "1", "wahr"]:
        val = "1"
    elif normalized in ["nein", "no", "false", "0", "falsch"]:
        val = "0"
    else:
        return False
    try:
        locator.first.select_option(value=val)
        return True
    except Exception:
        return False


def _fill_sedcard_fields(page: Page, payload: dict, tracker: FieldTracker | None = None) -> None:
    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    if not _enter_sedcard_edit_mode(page):
        return

    input_mappings = {
        "groesse": _pick_payload_value(payload, ["koerpergroesse"]),
        "konfektion": _pick_payload_value(payload, ["konfektionsgroesse"]),
        "schuhgroesse": _pick_payload_value(payload, ["schuhgroesse"]),
        "schulausbildung": _pick_payload_value(payload, ["schulausbildung"]),
        "fuehrerscheinart": _pick_payload_value(payload, ["fuehrerscheinklasse"]),
    }

    for field, value in input_mappings.items():
        if not value:
            continue
        locator = target.locator(f"[name='{field}'], #{field}")
        current = _safe_input_value(locator)
        if current == str(value):
            print(f"[OK] sedcard {field} bereits korrekt → {value}")
            if tracker:
                tracker.skip("sedcard", field, str(value), current)
            continue
        if _set_input_value_force(locator, value):
            print(f"[OK] sedcard {field} → {value}")
        else:
            print(f"[WARNUNG] sedcard {field} nicht gesetzt.")
            if tracker:
                tracker.missing("sedcard", field, str(value), _safe_input_value(locator))
        if tracker:
            actual = _safe_input_value(locator)
            if actual == str(value):
                tracker.ok("sedcard", field, str(value), actual)
            else:
                tracker.missing("sedcard", field, str(value), actual)

    language_entries = _parse_language_entries(_pick_payload_value(payload, ["fremdsprachen"]))
    if language_entries:
        _fill_language_fields(target, language_entries)
        if tracker:
            pairs = [
                ("sprache01a", "sprache01b"),
                ("sprache02a", "sprache02b"),
                ("sprache03a", "sprache03b"),
                ("sprache04a", "sprache04b"),
            ]
            for idx, (lang_field, level_field) in enumerate(pairs):
                if idx >= len(language_entries):
                    break
                entry = language_entries[idx]
                expected_lang = entry.get("language", "")
                expected_level = entry.get("level", "")
                if expected_lang:
                    loc = target.locator(f"[name='{lang_field}'], #{lang_field}")
                    actual = _safe_input_value(loc)
                    if actual == expected_lang:
                        tracker.ok("sedcard", lang_field, expected_lang, actual)
                    else:
                        tracker.missing("sedcard", lang_field, expected_lang, actual)
                if expected_level:
                    loc = target.locator(f"[name='{level_field}'], #{level_field}")
                    actual = _safe_input_value(loc)
                    if actual == expected_level:
                        tracker.ok("sedcard", level_field, expected_level, actual)
                    else:
                        tracker.missing("sedcard", level_field, expected_level, actual)

    fuehrerschein_value = _pick_payload_value(payload, ["fuehrerschein"])
    if fuehrerschein_value:
        locator = target.locator("[name='fuehrerschein']")
        current = _safe_select_value(locator)
        expected = "1" if str(fuehrerschein_value).strip().lower() in ["ja", "yes", "true", "1", "wahr"] else "0"
        if current == expected:
            print(f"[OK] sedcard fuehrerschein bereits korrekt → {fuehrerschein_value}")
            if tracker:
                tracker.skip("sedcard", "fuehrerschein", expected, current)
        elif _set_yes_no_select(locator, fuehrerschein_value):
            print(f"[OK] sedcard fuehrerschein → {fuehrerschein_value}")
        else:
            print("[WARNUNG] sedcard fuehrerschein nicht gesetzt.")
            if tracker:
                tracker.missing("sedcard", "fuehrerschein", expected, _safe_select_value(locator))
        if tracker:
            actual = _safe_select_value(locator)
            if actual == expected:
                tracker.ok("sedcard", "fuehrerschein", expected, actual)
            else:
                tracker.missing("sedcard", "fuehrerschein", expected, actual)

    pkw_value = _pick_payload_value(payload, ["pkw"])
    if pkw_value:
        locator = target.locator("[name='pkw']")
        current = _safe_select_value(locator)
        expected = "1" if str(pkw_value).strip().lower() in ["ja", "yes", "true", "1", "wahr"] else "0"
        if current == expected:
            print(f"[OK] sedcard pkw bereits korrekt → {pkw_value}")
            if tracker:
                tracker.skip("sedcard", "pkw", expected, current)
        elif _set_yes_no_select(locator, pkw_value):
            print(f"[OK] sedcard pkw → {pkw_value}")
        else:
            print("[WARNUNG] sedcard pkw nicht gesetzt.")
            if tracker:
                tracker.missing("sedcard", "pkw", expected, _safe_select_value(locator))
        if tracker:
            actual = _safe_select_value(locator)
            if actual == expected:
                tracker.ok("sedcard", "pkw", expected, actual)
            else:
                tracker.missing("sedcard", "pkw", expected, actual)

    save_button = target.locator(
        "button.editSubcontractor, "
        "button:has-text('Daten speichern'), "
        "input[type='submit'][value*='Daten speichern'], "
        "input.speichern, button:has-text('Speichern')"
    ).first
    if save_button.count() > 0:
        try:
            save_button.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            try:
                save_button.wait_for(state="visible", timeout=1500)
            except Exception:
                pass
            save_button.click()
            print("[OK] Sedcard gespeichert (Daten speichern).")
            return
        except Exception:
            try:
                save_button.click(force=True)
                print("[OK] Sedcard gespeichert (force click).")
                return
            except Exception as exc:
                try:
                    clicked = target.evaluate(
                        """() => {
                            const candidates = [];
                            const byClass = document.querySelector("button.editSubcontractor");
                            if (byClass) candidates.push(byClass);
                            document.querySelectorAll("input[type='submit'], button").forEach((el) => {
                                const value = (el.getAttribute('value') || '').trim();
                                const text = (el.textContent || '').trim();
                                if (value.includes('Daten speichern') || text.includes('Daten speichern') || text === 'Speichern') {
                                    candidates.push(el);
                                }
                            });
                            for (const el of candidates) {
                                try {
                                    el.classList.remove('hideElement');
                                    el.classList.add('showElement');
                                    if (el.style) {
                                        el.style.display = 'inline-block';
                                        el.style.visibility = 'visible';
                                    }
                                    el.removeAttribute('disabled');
                                    el.removeAttribute('readonly');
                                    el.click();
                                    return true;
                                } catch (e) {}
                            }
                            return false;
                        }"""
                    )
                except Exception:
                    clicked = False
                if clicked:
                    print("[OK] Sedcard gespeichert (JS fallback).")
                else:
                    print(f"[WARNUNG] Sedcard speichern fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] Sedcard-Speichern-Button nicht gefunden.")


def _fill_grundlohn_history(page: Page) -> None:
    entries = [
        ("01.01.2026", "14,96"),
        ("01.09.2026", "15,33"),
        ("01.04.2027", "15,87"),
    ]

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    edit_icon = target.locator(
        "img.edit[onclick*=\"daten_historie\"][onclick*=\"'lohn'\"], "
        "img.edit[onclick*='daten_historie'][onclick*='lohn']"
    ).first
    if edit_icon.count() == 0:
        print("[WARNUNG] Grundlohn-Edit-Icon nicht gefunden.")
        return
    try:
        edit_icon.scroll_into_view_if_needed()
    except Exception:
        pass
    edit_icon.click(force=True)
    print("[OK] Grundlohn-Historie geöffnet.")

    dialog = page.locator("div.ui-dialog:has-text('Grundlohn-Historie')").first
    try:
        dialog.wait_for(state="visible", timeout=8000)
    except Exception:
        print("[WARNUNG] Grundlohn-Historie-Dialog nicht sichtbar.")
        return

    try:
        dialog_text = dialog.inner_text()
    except Exception:
        dialog_text = ""
    if all(date in dialog_text and amount in dialog_text for date, amount in entries):
        print("[INFO] Grundlohn-Historie bereits vorhanden – schließe Dialog.")
        close_button = dialog.locator("button:has-text('schließen'), button:has-text('Schließen')").first
        if close_button.count() > 0:
            try:
                close_button.click()
                print("[OK] Grundlohn-Dialog geschlossen.")
            except Exception as exc:
                print(f"[WARNUNG] Grundlohn-Dialog schließen fehlgeschlagen: {exc}")
        else:
            print("[WARNUNG] 'schließen' Button im Grundlohn-Dialog nicht gefunden.")
        return

    for date_value, amount_value in entries:
        value_input = dialog.locator("#daten_eintragen_wert").first
        date_input = dialog.locator("#daten_eintragen_gueltig_ab").first
        if value_input.count() == 0 or date_input.count() == 0:
            print("[WARNUNG] Eingabefelder im Grundlohn-Dialog nicht gefunden.")
            return
        value_input.fill(amount_value)
        date_input.fill(date_value)
        submit_button = dialog.locator("button:has-text('eintragen')").first
        if submit_button.count() == 0:
            print("[WARNUNG] 'eintragen'-Button im Grundlohn-Dialog nicht gefunden.")
            return
        try:
            submit_button.click()
            print(f"[OK] Grundlohn eingetragen → {date_value} = {amount_value}")
        except Exception as exc:
            try:
                dialog.evaluate(
                    """() => {
                        const btn = Array.from(document.querySelectorAll('button'))
                            .find(b => (b.textContent || '').trim().toLowerCase() === 'eintragen');
                        if (btn) { btn.click(); return true; }
                        return false;
                    }"""
                )
                print(f"[OK] Grundlohn eingetragen (JS-Fallback) → {date_value} = {amount_value}")
            except Exception as js_exc:
                print(f"[ERROR] Grundlohn 'eintragen' Klick fehlgeschlagen: {exc} / JS: {js_exc}")
                return
        time.sleep(0.5)

    close_button = dialog.locator("button:has-text('schließen'), button:has-text('Schließen')").first
    if close_button.count() > 0:
        try:
            close_button.click()
            print("[OK] Grundlohn-Dialog geschlossen.")
        except Exception as exc:
            print(f"[WARNUNG] Grundlohn-Dialog schließen fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] 'schließen' Button im Grundlohn-Dialog nicht gefunden.")


def _fill_vertrag_history(page: Page, payload: dict) -> None:
    vertrag = payload.get("vertrag") or {}
    if not isinstance(vertrag, dict):
        vertrag = {}
    contract_type = str(vertrag.get("contract_type", "")).strip().lower()
    hire_date = str(vertrag.get("hire_date", "")).strip()
    if not contract_type or not hire_date:
        print("[HINWEIS] Vertrag/Eintrittsdatum fehlt – überspringe Vertragshistorie.")
        return

    type_map = {
        "kb": "kurzf. Beschäftigte",
        "tz": "Teilzeit 80h",
        "gb": "GB - Minijob",
    }
    label = type_map.get(contract_type)
    if not label:
        print(f"[WARNUNG] Unbekannter contract_type: {contract_type!r}")
        return

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    edit_icon = target.locator(
        "img.edit[onclick*=\"daten_historie\"][onclick*=\"'vertrag_id'\"], "
        "img.edit[onclick*='daten_historie'][onclick*='vertrag_id']"
    ).first
    if edit_icon.count() == 0:
        print("[WARNUNG] Vertrag-Edit-Icon nicht gefunden.")
        return
    try:
        edit_icon.scroll_into_view_if_needed()
    except Exception:
        pass
    edit_icon.click(force=True)
    print("[OK] Vertragshistorie geöffnet.")

    dialog = page.locator("div.ui-dialog:has-text('Vertragshistorie')").first
    try:
        dialog.wait_for(state="visible", timeout=8000)
    except Exception:
        print("[WARNUNG] Vertragshistorie-Dialog nicht sichtbar.")
        return

    # Deactivate specifically the "Kein Vertrag" row so only the new contract remains active.
    try:
        kein_vertrag_link = dialog.locator(
            "tr:has(td:has-text('Kein Vertrag')) a[onclick*='daten_historie_change_status'][onclick*='vertrag_id']"
        ).first
        if kein_vertrag_link.count() > 0:
            print("[INFO] Deaktiviere 'Kein Vertrag' …")
            try:
                kein_vertrag_link.click()
            except Exception:
                try:
                    kein_vertrag_link.evaluate("el => el.click()")
                except Exception:
                    pass
            try:
                kein_vertrag_link.wait_for(state="detached", timeout=3000)
            except Exception:
                pass
            print("[OK] 'Kein Vertrag' deaktiviert (falls vorhanden).")
    except Exception as exc:
        print(f"[WARNUNG] Deaktivieren von 'Kein Vertrag' fehlgeschlagen: {exc}")

    try:
        dialog_text = dialog.inner_text()
    except Exception:
        dialog_text = ""
    hire_date_ui = _format_date_for_ui(hire_date)
    hire_date_modal = _first_of_month(hire_date_ui)
    if label in dialog_text and hire_date_modal in dialog_text:
        print("[INFO] Vertragshistorie bereits vorhanden – schließe Dialog.")
        close_button = dialog.locator("button:has-text('schließen'), button:has-text('Schließen')").first
        if close_button.count() > 0:
            try:
                close_button.click()
                print("[OK] Vertrag-Dialog geschlossen.")
            except Exception as exc:
                print(f"[WARNUNG] Vertrag-Dialog schließen fehlgeschlagen: {exc}")
        else:
            print("[WARNUNG] 'schließen' Button im Vertrag-Dialog nicht gefunden.")
        return

    select = dialog.locator("#daten_eintragen_wert").first
    date_input = dialog.locator("#daten_eintragen_gueltig_ab").first
    if select.count() == 0 or date_input.count() == 0:
        print("[WARNUNG] Eingabefelder im Vertrag-Dialog nicht gefunden.")
        return
    select.select_option(label=label)
    date_input.fill(hire_date_modal)
    submit_button = dialog.locator("button:has-text('eintragen')").first
    if submit_button.count() == 0:
        print("[WARNUNG] 'eintragen'-Button im Vertrag-Dialog nicht gefunden.")
        return

    submit_button.click()
    print(f"[OK] Vertrag eingetragen → {label} ab {hire_date_modal}")
    time.sleep(0.5)

    close_button = dialog.locator("button:has-text('schließen'), button:has-text('Schließen')").first
    if close_button.count() > 0:
        try:
            close_button.click()
            print("[OK] Vertrag-Dialog geschlossen.")
        except Exception as exc:
            print(f"[WARNUNG] Vertrag-Dialog schließen fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] 'schließen' Button im Vertrag-Dialog nicht gefunden.")


def _fill_tage_fremd(page: Page, payload: dict) -> None:
    tage = _pick_payload_value(payload, ["tage_gearbeitet"])
    if not tage:
        print("[HINWEIS] Keine tage_gearbeitet im JSON – überspringe Tage Fremdfirmen.")
        return

    vertrag = payload.get("vertrag") or {}
    if not isinstance(vertrag, dict):
        vertrag = {}
    hire_date = str(vertrag.get("hire_date", "")).strip()
    if not hire_date:
        print("[HINWEIS] Kein hire_date im JSON – überspringe Tage Fremdfirmen.")
        return

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    edit_icon = target.locator(
        "img.edit[onclick*=\"daten_historie\"][onclick*=\"'tage_fremd'\"], "
        "img.edit[onclick*='daten_historie'][onclick*='tage_fremd']"
    ).first
    if edit_icon.count() == 0:
        print("[WARNUNG] Tage Fremdfirmen-Edit-Icon nicht gefunden.")
        return
    try:
        edit_icon.scroll_into_view_if_needed()
    except Exception:
        pass
    edit_icon.click(force=True)
    print("[OK] Tage Fremdfirmen-Historie geöffnet.")

    dialog = page.locator("div.ui-dialog").filter(has_text="gültig ab").first
    try:
        dialog.wait_for(state="visible", timeout=8000)
    except Exception:
        print("[WARNUNG] Tage Fremdfirmen-Dialog nicht sichtbar.")
        return

    try:
        dialog_text = dialog.inner_text()
    except Exception:
        dialog_text = ""
    if tage in dialog_text and hire_date in dialog_text:
        print("[INFO] Tage Fremdfirmen bereits vorhanden – schließe Dialog.")
        close_button = dialog.locator("button:has-text('schließen'), button:has-text('Schließen')").first
        if close_button.count() > 0:
            try:
                close_button.click()
                print("[OK] Tage Fremdfirmen-Dialog geschlossen.")
            except Exception as exc:
                print(f"[WARNUNG] Tage Fremdfirmen-Dialog schließen fehlgeschlagen: {exc}")
        else:
            print("[WARNUNG] 'schließen' Button im Tage Fremdfirmen-Dialog nicht gefunden.")
        return

    value_input = dialog.locator("#daten_eintragen_wert").first
    date_input = dialog.locator("#daten_eintragen_gueltig_ab").first
    if value_input.count() == 0 or date_input.count() == 0:
        print("[WARNUNG] Eingabefelder im Tage Fremdfirmen-Dialog nicht gefunden.")
        return
    value_input.fill(tage)
    date_input.fill(hire_date)
    submit_button = dialog.locator("button:has-text('eintragen')").first
    if submit_button.count() == 0:
        print("[WARNUNG] 'eintragen'-Button im Tage Fremdfirmen-Dialog nicht gefunden.")
        return
    submit_button.click()
    print(f"[OK] Tage Fremdfirmen eingetragen → {tage} ab {hire_date}")
    time.sleep(0.5)

    close_button = dialog.locator("button:has-text('schließen'), button:has-text('Schließen')").first
    if close_button.count() > 0:
        try:
            close_button.click()
            print("[OK] Tage Fremdfirmen-Dialog geschlossen.")
        except Exception as exc:
            print(f"[WARNUNG] Tage Fremdfirmen-Dialog schließen fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] 'schließen' Button im Tage Fremdfirmen-Dialog nicht gefunden.")


def _fill_sonstiges(page: Page, payload: dict) -> None:
    vertrag = payload.get("vertrag") if isinstance(payload, dict) else None
    vertrag_value = ""
    if isinstance(vertrag, dict):
        vertrag_value = _pick_payload_value(
            vertrag,
            ["sonstiges", "source", "quelle", "kanal", "channel", "traffic_source", "utm_source"],
        )
    fallback_value = _pick_payload_value(
        payload,
        ["aufmerksam_geworden_durch", "source", "quelle", "kanal", "channel", "traffic_source", "utm_source"],
    )
    value = vertrag_value or fallback_value

    if value is None or str(value).strip() == "":
        print("[HINWEIS] Kein Sonstiges/Kanal im JSON – überspringe Sonstiges.")
        return
    value = str(value).strip()
    source = "vertrag" if vertrag_value else "fragebogen"
    print(f"[INFO] Sonstiges-Value aus JSON ({source}): {value}")

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    def _find_sonstiges_dialog(timeout_s: float = 8.0) -> tuple[Locator | None, Union[Frame, Page] | None]:
        deadline = time.time() + timeout_s
        candidates: list[Union[Frame, Page]] = [page]
        try:
            candidates.extend(page.frames)
        except Exception:
            pass
        if target not in candidates:
            candidates.insert(0, target)

        fallback_dialog = None
        fallback_target: Union[Frame, Page] | None = None

        while time.time() < deadline:
            for candidate in candidates:
                try:
                    dialogs = candidate.locator("div.ui-dialog")
                    for idx in range(dialogs.count()):
                        dialog = dialogs.nth(idx)
                        try:
                            if not dialog.is_visible():
                                continue
                        except Exception:
                            continue
                        try:
                            title_text = dialog.locator(".ui-dialog-title").first.inner_text().strip().lower()
                        except Exception:
                            title_text = ""
                        try:
                            dialog_text = dialog.inner_text().strip().lower()
                        except Exception:
                            dialog_text = ""
                        has_input = dialog.locator("input[type='text'], textarea").count() > 0
                        if not has_input:
                            continue
                        if "sonstiges" in title_text or "sonstiges" in dialog_text:
                            return dialog, candidate
                        if "feld" in title_text and not fallback_dialog:
                            fallback_dialog = dialog
                            fallback_target = candidate
                        if not fallback_dialog:
                            fallback_dialog = dialog
                            fallback_target = candidate
                except Exception:
                    continue
            if fallback_dialog:
                return fallback_dialog, fallback_target
            time.sleep(0.2)
        return None, None

    edit_icon = target.locator(
        "img.edit[onclick*=\"feld_aendern\"][onclick*=\"'sonstiges'\"], "
        "img.edit[onclick*='feld_aendern'][onclick*='sonstiges']"
    ).first
    if edit_icon.count() == 0:
        print("[WARNUNG] Sonstiges-Edit-Icon nicht gefunden.")
        return
    try:
        edit_icon.scroll_into_view_if_needed()
    except Exception:
        pass
    try:
        edit_icon.click(force=True)
        print("[OK] Sonstiges-Dialog geöffnet.")
    except Exception as exc:
        print(f"[WARNUNG] Sonstiges-Dialog konnte nicht geöffnet werden: {exc}")
        return

    dialog, dialog_target = _find_sonstiges_dialog(timeout_s=6.0)
    if dialog is None or dialog_target is None:
        # Fallback: try to call xajax directly and re-check.
        try:
            onclick = edit_icon.get_attribute("onclick") or ""
        except Exception:
            onclick = ""
        match = re.search(r"xajax_feld_aendern\((\d+),\s*'sonstiges'", onclick)
        if match:
            try:
                user_id = int(match.group(1))
                page.evaluate(
                    "(uid) => { if (typeof xajax_feld_aendern === 'function') { xajax_feld_aendern(uid, 'sonstiges', 'sonstiges'); } }",
                    user_id,
                )
            except Exception:
                pass
            dialog, dialog_target = _find_sonstiges_dialog(timeout_s=4.0)
    if dialog is None or dialog_target is None:
        print("[WARNUNG] Sonstiges-Dialog nicht sichtbar.")
        return

    input_field = dialog.locator("input[type='text'], textarea").first
    if input_field.count() == 0:
        print("[WARNUNG] Sonstiges-Eingabefeld nicht gefunden.")
        return
    try:
        input_field.fill(value)
    except Exception:
        try:
            input_field.evaluate(
                """(node, val) => {
                    node.value = val;
                    node.dispatchEvent(new Event('input', { bubbles: true }));
                    node.dispatchEvent(new Event('change', { bubbles: true }));
                    node.dispatchEvent(new Event('blur', { bubbles: true }));
                }""",
                value,
            )
        except Exception as exc:
            print(f"[WARNUNG] Sonstiges-Feld konnte nicht gesetzt werden: {exc}")
            return

    try:
        current_value = input_field.input_value().strip()
        if current_value != value:
            print(f"[WARNUNG] Sonstiges-Wert weicht ab (gesetzt='{value}', gelesen='{current_value}').")
    except Exception:
        pass

    save_button = dialog.locator(
        "button:has-text('speichern'), button:has-text('Speichern'), "
        "button:has-text('OK'), button:has-text('Ok'), button:has-text('Übernehmen')"
    ).first
    if save_button.count() > 0:
        try:
            save_button.click()
            print(f"[OK] Sonstiges gesetzt → {value}")
        except Exception as exc:
            print(f"[WARNUNG] Sonstiges speichern fehlgeschlagen: {exc}")
            try:
                dialog.press("Enter")
            except Exception:
                pass
    else:
        print("[WARNUNG] Sonstiges-Speichern-Button nicht gefunden – versuche Enter/Blur.")
        try:
            input_field.press("Enter")
        except Exception:
            pass
        try:
            dialog.press("Enter")
        except Exception:
            pass
        try:
            page.keyboard.press("Enter")
        except Exception:
            pass
        try:
            # Click outside to trigger blur/save, if the UI uses implicit save.
            page.mouse.click(5, 5)
        except Exception:
            pass
        try:
            dialog.wait_for(state="hidden", timeout=2000)
            print(f"[OK] Sonstiges gesetzt → {value} (Dialog geschlossen).")
        except Exception:
            pass


def _fill_eintritt_austritt(page: Page, payload: dict) -> None:
    vertrag = payload.get("vertrag") or {}
    if not isinstance(vertrag, dict):
        vertrag = {}
    hire_date = str(vertrag.get("hire_date", "")).strip()
    befristung_bis = str(vertrag.get("befristung_bis", "")).strip()
    contract_type = str(vertrag.get("contract_type", "")).strip()
    if not hire_date:
        print("[HINWEIS] Kein hire_date im JSON – überspringe Ein-/Austritt.")
        return

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    edit_icon = target.locator(
        "img.edit[onclick*='eintritt_austritt_editor'], "
        "img.edit[onclick*='eintritt_austritt']"
    ).first
    if edit_icon.count() == 0:
        print("[WARNUNG] Eintritt/Austritt-Edit-Icon nicht gefunden.")
        return
    try:
        edit_icon.scroll_into_view_if_needed()
    except Exception:
        pass
    edit_icon.click(force=True)
    print("[OK] Ein-/Austrittsdatum-Dialog geöffnet.")

    dialog = page.locator("div.ui-dialog:has-text('Ein-/Austrittsdatum ändern')").first
    try:
        dialog.wait_for(state="visible", timeout=8000)
    except Exception:
        print("[WARNUNG] Ein-/Austrittsdatum-Dialog nicht sichtbar.")
        return

    try:
        dialog_text = dialog.inner_text()
    except Exception:
        dialog_text = ""
    expected_end = befristung_bis if befristung_bis else "unbefristet"
    remark = contract_type.upper()
    if hire_date in dialog_text and expected_end in dialog_text and (contract_type in dialog_text or remark in dialog_text):
        print("[INFO] Ein-/Austritt bereits vorhanden – schließe Dialog.")
        close_button = dialog.locator("button:has-text('Schließen'), button:has-text('schließen')").first
        if close_button.count() > 0:
            try:
                close_button.click()
                print("[OK] Ein-/Austrittsdatum-Dialog geschlossen.")
            except Exception as exc:
                print(f"[WARNUNG] Ein-/Austrittsdatum-Dialog schließen fehlgeschlagen: {exc}")
        return

    eintritt_input = dialog.locator("#eintrittsdatum_neu").first
    austritt_input = dialog.locator("#austrittsdatum_neu").first
    bemerkung_input = dialog.locator("#bemerkung").first
    if eintritt_input.count() == 0 or austritt_input.count() == 0 or bemerkung_input.count() == 0:
        print("[WARNUNG] Ein-/Austrittsdatum-Felder nicht gefunden.")
        return
    eintritt_input.fill(hire_date)
    austritt_input.fill(befristung_bis)
    remark = contract_type.upper()
    bemerkung_input.fill(remark)

    save_button = dialog.locator("button:has-text('Speichern')").first
    if save_button.count() == 0:
        print("[WARNUNG] Ein-/Austrittsdatum-Speichern-Button nicht gefunden.")
        return
    save_button.click()
    print(f"[OK] Ein-/Austritt gesetzt → {hire_date} bis {befristung_bis or 'unbefristet'} ({remark})")
    time.sleep(0.5)

    warn_dialog = page.locator("div.ui-dialog:has-text('Warnung')").first
    try:
        warn_dialog.wait_for(state="visible", timeout=4000)
        fortfahren = warn_dialog.locator("button:has-text('Fortfahren')").first
        if fortfahren.count() > 0:
            fortfahren.click()
            print("[OK] Warnung bestätigt (Fortfahren).")
    except Exception:
        pass

    close_button = dialog.locator("button:has-text('Schließen'), button:has-text('schließen')").first
    if close_button.count() > 0:
        try:
            close_button.click()
            print("[OK] Ein-/Austrittsdatum-Dialog geschlossen.")
        except Exception as exc:
            print(f"[WARNUNG] Ein-/Austrittsdatum-Dialog schließen fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] 'Schließen' Button im Ein-/Austrittsdatum-Dialog nicht gefunden.")


def _find_angebot_file() -> str:
    input_dir = Path(os.environ.get("PERSO_INPUT_DIR", "perso-input"))
    candidates = []
    for suffix in ("*.pdf", "*.png", "*.jpg", "*.jpeg"):
        candidates.extend(input_dir.glob(suffix))
    if not candidates:
        return ""
    # Poller speichert den Vertrag standardmäßig unter vertrag.*
    for path in candidates:
        if path.name.lower().startswith("vertrag."):
            return str(path)
    # Prefer files containing "vertrag" or "angebot" (case-insensitive).
    for path in candidates:
        name = path.name.lower()
        if "vertrag" in name or "angebot" in name:
            return str(path)
    return str(candidates[0])


def _format_date_for_ui(date_str: str) -> str:
    if not date_str:
        return ""
    lowered = str(date_str).strip().lower()
    if lowered in {"none", "null", "undefined", "nan"}:
        return ""
    match = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", str(date_str).strip())
    if match:
        year, month, day = match.groups()
        if int(year) < 2005:
            return ""
        return f"{day}.{month}.{year}"
    match = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", str(date_str).strip())
    if match:
        year = int(match.group(3))
        if year < 2005:
            return ""
        return str(date_str).strip()
    return str(date_str).strip()


def _subtract_years(date_str: str, years: int) -> str:
    ui = _format_date_for_ui(date_str)
    match = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", ui)
    if not match:
        return ""
    day, month, year = match.groups()
    try:
        return f"{day}.{month}.{int(year) - years}"
    except Exception:
        return ""


def _parse_month_from_date(date_str: str) -> int | None:
    if not date_str:
        return None
    match = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", date_str)
    if match:
        return int(match.group(2))
    match = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", date_str)
    if match:
        return int(match.group(2))
    return None


def _derive_semester_from_date(date_str: str) -> tuple[str, str]:
    month = _parse_month_from_date(date_str)
    if month is None:
        return "", ""
    if 4 <= month <= 9:
        return "SS", "Sommersemester"
    return "WS", "Wintersemester"


def _resolve_immatrikulation_bemerkung(payload: dict) -> tuple[str, str]:
    employment_mode = payload.get("beschaeftigung_modus")
    status = str(payload.get("kein_beschaeftigungsverhaeltnis") or "").strip().lower()
    is_student = employment_mode == "kein" and status == "studentin"
    is_school = employment_mode == "kein" and status == "schuelerin"
    uploads = payload.get("uploads") if isinstance(payload, dict) else {}
    meta = uploads.get("immatrikulation") if isinstance(uploads, dict) else {}
    valid_until_raw = ""
    semester_raw = ""
    if isinstance(meta, dict):
        valid_until_raw = str(meta.get("validUntil") or "").strip()
        semester_raw = str(meta.get("semesterOption") or "").strip().lower()

    if is_school:
        return "Schulbescheinigung", valid_until_raw

    semester_label = ""
    semester_code = ""
    if semester_raw in {"sommersemester", "sommer", "summer", "ss"}:
        semester_code, semester_label = "SS", "Sommersemester"
    elif semester_raw in {"wintersemester", "winter", "ws"}:
        semester_code, semester_label = "WS", "Wintersemester"
    elif semester_raw and re.match(r"^(\d{4})-(\d{2})-(\d{2})$", semester_raw):
        semester_code, semester_label = _derive_semester_from_date(semester_raw)
    elif valid_until_raw:
        semester_code, semester_label = _derive_semester_from_date(valid_until_raw)

    if semester_code:
        return f"Immatrikulationsbescheinigung {semester_code}", valid_until_raw
    if is_student:
        return "Immatrikulationsbescheinigung", valid_until_raw
    return "Immatrikulations-/Schulbescheinigung", valid_until_raw


def _first_of_month(date_str: str) -> str:
    ui = _format_date_for_ui(date_str)
    match = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", ui)
    if not match:
        return ui
    _day, month, year = match.groups()
    return f"01.{month}.{year}"


def _build_vertrag_bemerkung(payload: dict) -> str:
    vertrag = payload.get("vertrag") or {}
    if not isinstance(vertrag, dict):
        return ""
    contract_type = str(vertrag.get("contract_type", "")).strip().lower()
    hire_date = str(vertrag.get("hire_date", "")).strip()
    if not contract_type or not hire_date:
        return ""
    type_map = {"kb": "KB", "tz": "TZ", "gb": "GB"}
    type_label = type_map.get(contract_type, contract_type.upper())
    hire_date_ui = _format_date_for_ui(hire_date)
    return f"Arbeitsvertrag {type_label} zum {hire_date_ui}"


def _close_document_upload_dialog(page: Page) -> None:
    selectors = [
        "div.ui-dialog:has-text('Dokument hinzufügen')",
        "div.ui-dialog:has(.ui-dialog-title:has-text('Dokument hinzufügen'))",
    ]
    for sel in selectors:
        dialog = page.locator(sel).first
        try:
            if dialog.count() == 0 or not dialog.is_visible():
                continue
        except Exception:
            continue
        close_btn = dialog.locator(
            "button.ui-dialog-titlebar-close, button:has-text('Abbrechen'), button:has-text('Schließen')"
        ).first
        try:
            if close_btn.count() > 0:
                close_btn.click()
            else:
                page.evaluate(
                    """() => {
                        const win = document.getElementById('scnUiWindow');
                        if (win) { win.dispatchEvent(new Event('trigger_close')); }
                    }"""
                )
        except Exception:
            pass
        try:
            dialog.wait_for(state="hidden", timeout=2000)
        except Exception:
            pass


def _open_document_upload_dialog(
    page: Page,
    *,
    force_open: bool = False,
) -> tuple[Locator | None, Union[Frame, Page] | None]:
    def _log_dialog_debug(step: str) -> None:
        try:
            print(f"[DEBUG] Upload-Dialog {step}: page_url={page.url!r}")
        except Exception:
            pass
        try:
            names = []
            for idx, fr in enumerate(page.frames):
                names.append(f"{idx}:{fr.name!r} url={fr.url!r}")
            print(f"[DEBUG] Upload-Dialog {step}: frames={names}")
        except Exception:
            pass
        try:
            print(f"[DEBUG] Upload-Dialog {step}: dialogs(page)={page.locator('div.ui-dialog').count()}")
        except Exception:
            pass

    def _find_dialog_in_targets(targets: list[Union[Frame, Page]]):
        for target in targets:
            try:
                dialog = target.locator("div.ui-dialog:has-text('Dokument hinzufügen')").first
                if dialog.count() == 0:
                    continue
                dialog.wait_for(state="visible", timeout=1500)
                # Ensure this dialog actually contains the dropzone or file input.
                has_dropzone = False
                try:
                    has_dropzone = dialog.locator("#maDokDropzone").count() > 0
                except Exception:
                    has_dropzone = False
                has_file_input = False
                try:
                    has_file_input = dialog.locator("input[type='file']").count() > 0
                except Exception:
                    has_file_input = False
                if not (has_dropzone or has_file_input):
                    continue
                try:
                    target_url = target.url if hasattr(target, "url") else ""
                except Exception:
                    target_url = ""
                print(f"[DEBUG] Upload-Dialog found in target url={target_url!r}")
                return dialog, target
            except Exception:
                continue
        return None, None

    candidates: list[Union[Frame, Page]] = [page]
    candidates.extend(page.frames)
    if not force_open:
        dialog, dialog_target = _find_dialog_in_targets(candidates)
        if dialog is not None:
            return dialog, dialog_target

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    def _click_add_button() -> bool:
        targets: list[Union[Frame, Page]] = [target, page]
        for fr in page.frames:
            targets.append(fr)

        btn_selectors = [
            "button:has-text('Dokument hinzufügen')",
            "button[title*='Dokument hinzufügen']",
            "button[onclick*='mitarbeiter_info_dokumente_editor']",
            "#dokumente button:has-text('Dokument hinzufügen')",
        ]

        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                _dismiss_ui_overlay(page)
                try:
                    page.locator("#loaderContainer").first.wait_for(state="hidden", timeout=1500)
                except Exception:
                    pass
            except Exception:
                pass

            for candidate in targets:
                for btn_sel in btn_selectors:
                    try:
                        add_btn = candidate.locator(btn_sel).first
                        if add_btn.count() == 0:
                            continue
                        try:
                            add_btn.scroll_into_view_if_needed()
                        except Exception:
                            pass
                        try:
                            add_btn.click()
                        except Exception:
                            add_btn.click(force=True)
                        print("[OK] Dokument hinzufügen geöffnet.")
                        return True
                    except Exception:
                        continue

            # JS fallback for dynamically rendered button.
            try:
                clicked = page.evaluate(
                    """() => {
                        const selectors = [
                            "button[title*='Dokument hinzufügen']",
                            "button[onclick*='mitarbeiter_info_dokumente_editor']",
                            "#dokumente button"
                        ];
                        for (const sel of selectors) {
                            const btns = Array.from(document.querySelectorAll(sel));
                            const btn = btns.find(b => (b.textContent || '').includes('Dokument hinzufügen'));
                            if (btn) { btn.scrollIntoView({ block: 'center' }); btn.click(); return true; }
                        }
                        return false;
                    }"""
                )
                if clicked:
                    print("[OK] Dokument hinzufügen geöffnet (JS fallback).")
                    return True
            except Exception:
                pass

            time.sleep(0.4)

        try:
            dlg_sel = "div.ui-dialog:has-text('Dokument hinzufügen')"
            print(
                "[DEBUG] Upload-Dialog Button-Counts: "
                f"inhalt={target.locator(btn_selectors[0]).count()} "
                f"page={page.locator(btn_selectors[0]).count()} "
                f"dialog={page.locator(dlg_sel).count()}"
            )
        except Exception:
            pass
        return False

    if not _click_add_button():
        print("[WARNUNG] 'Dokument hinzufügen' Button nicht gefunden.")
        _log_dialog_debug("not-found")
        return None, None

    dialog = page.locator("div.ui-dialog:has-text('Dokument hinzufügen')").first
    try:
        dialog.wait_for(state="visible", timeout=8000)
    except Exception:
        print("[WARNUNG] Dokument-Dialog nicht sichtbar.")
        _log_dialog_debug("not-visible")
        return None, None
    try:
        dialog.wait_for(selector="#maDokDropzone, input[type='file']", timeout=3000)
    except Exception:
        pass
    try:
        target_url = target.url if hasattr(target, "url") else ""
    except Exception:
        target_url = ""
    print(f"[DEBUG] Upload-Dialog visible after click: target_url={target_url!r}")
    return dialog, target


def _upload_document_with_modal(
    page: Page,
    file_path: str,
    folder_label: str,
    folder_value: str,
    bemerkung_text: str = "",
    gueltig_bis: str = "",
) -> bool:
    _close_document_upload_dialog(page)
    dialog, dialog_target = _open_document_upload_dialog(page, force_open=False)
    if dialog is None or dialog_target is None:
        return False

    def _find_file_input(timeout_s: float = 3.5):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                file_input = dialog.locator("input[type='file']").first
                if file_input.count() > 0:
                    return file_input
                file_input = dialog_target.locator("input[type='file']").first
                if file_input.count() > 0:
                    return file_input
                file_input = page.locator("input[type='file']").first
                if file_input.count() > 0:
                    return file_input
                for fr in page.frames:
                    file_input = fr.locator("input[type='file']").first
                    if file_input.count() > 0:
                        return file_input
            except Exception:
                pass
            time.sleep(0.2)
        return None

    def _try_click_upload_trigger() -> bool:
        triggers = [
            dialog.locator("#maDokDropzone").first,
            dialog.locator("button:has-text('Datei')").first,
            dialog.locator("button:has-text('Datei auswählen')").first,
            dialog.locator("button:has-text('Durchsuchen')").first,
            dialog.locator("label:has-text('Datei')").first,
            dialog.locator("label:has-text('Durchsuchen')").first,
        ]
        for trigger in triggers:
            try:
                if trigger.count() == 0:
                    continue
                try:
                    trigger.scroll_into_view_if_needed()
                except Exception:
                    pass
                try:
                    trigger.click()
                except Exception:
                    trigger.click(force=True)
                return True
            except Exception:
                continue
        return False

    # Dropzone creates a hidden file input on click; use file chooser fallback.
    try:
        with page.expect_file_chooser(timeout=5000) as fc_info:
            if not _try_click_upload_trigger():
                raise RuntimeError("Upload trigger not found")
        file_chooser = fc_info.value
        file_chooser.set_files(file_path)
        print(f"[OK] Datei ausgewählt → {Path(file_path).name}")
    except Exception as exc:
        print(f"[DEBUG] Upload-Dialog dropzone fallback: {exc}")
        file_input = _find_file_input(timeout_s=6.0)
        if file_input is None:
            try:
                target_url = dialog_target.url if hasattr(dialog_target, "url") else ""
            except Exception:
                target_url = ""
            print(f"[DEBUG] Upload-Dialog file input search failed: target_url={target_url!r}")
            try:
                file_sel = "input[type='file']"
                print(
                    "[DEBUG] Upload-Dialog file input counts: "
                    f"dialog={dialog.locator(file_sel).count()} "
                    f"target={dialog_target.locator(file_sel).count()} "
                    f"page={page.locator(file_sel).count()}"
                )
            except Exception:
                pass
            print("[WARNUNG] Datei-Input im Dokument-Dialog nicht gefunden.")
            return False
        file_input.set_input_files(file_path)
        print(f"[OK] Datei ausgewählt → {Path(file_path).name}")

    table_body = dialog.locator("#tableAuflistungDateien tbody").first
    try:
        table_body.wait_for(state="visible", timeout=8000)
    except Exception:
        pass

    row = table_body.locator("tr").first
    deadline = time.time() + 20
    while time.time() < deadline and row.count() == 0:
        time.sleep(0.2)
        row = table_body.locator("tr").first
    if row.count() == 0:
        print("[WARNUNG] Upload-Row nicht erschienen.")
        return False

    if bemerkung_text:
        bemerkung_input = row.locator(
            "textarea[name*='bemerkung'], textarea[id^='fileExtras_'], textarea"
        ).first
        if bemerkung_input.count() > 0:
            bemerkung_input.fill(bemerkung_text)
            print(f"[OK] Bemerkung gesetzt → {bemerkung_text}")
        else:
            print("[WARNUNG] Bemerkung-Feld im Upload-Row nicht gefunden.")

    if gueltig_bis:
        gueltig_input = row.locator(
            "input[name*='gueltig_bis'], input[id^='fileExtrasGueltigBis'], input.datepicker"
        ).first
        if gueltig_input.count() > 0:
            _set_input_value_force(gueltig_input, gueltig_bis)
            print(f"[OK] Gültig bis gesetzt → {gueltig_bis}")
        else:
            print("[WARNUNG] Gültig-bis-Feld im Upload-Row nicht gefunden.")

    folder_select = row.locator("select").first
    if folder_select.count() > 0:
        try:
            folder_select.select_option(label=folder_label)
        except Exception:
            try:
                folder_select.select_option(value=folder_value)
            except Exception:
                pass
        print(f"[OK] Ordner gesetzt → {folder_label}")
    else:
        print("[WARNUNG] Ordner-Auswahl nicht gefunden.")

    save_button = dialog.locator("button:has-text('Speichern')").first
    if save_button.count() == 0:
        print("[WARNUNG] Dokument-Speichern-Button nicht gefunden.")
        return False
    save_button.click()
    print("[OK] Dokument gespeichert.")
    try:
        dialog.wait_for(state="hidden", timeout=8000)
    except Exception:
        try:
            close_button = dialog.locator("button:has-text('Schließen'), button:has-text('Abbrechen')").first
            if close_button.count() > 0:
                close_button.click()
        except Exception:
            pass
    return True


def _find_input_file_by_stem(stem: str) -> str:
    input_dir = Path(os.environ.get("PERSO_INPUT_DIR", "perso-input"))
    candidates = sorted(input_dir.glob(f"{stem}.*"))
    if not candidates and stem == "personalbogen":
        candidates = sorted(input_dir.glob("personalfragebogen.*"))
    if not candidates:
        return ""
    preferred = [".pdf", ".png", ".jpg", ".jpeg"]
    candidates.sort(
        key=lambda path: (
            preferred.index(path.suffix.lower()) if path.suffix.lower() in preferred else 999,
            path.name.lower(),
        )
    )
    return str(candidates[0])


def _download_upload_to_temp(uploads: dict, stem: str) -> str:
    if not isinstance(uploads, dict):
        return ""
    meta = uploads.get(stem)
    if not isinstance(meta, dict):
        return ""
    url = str(meta.get("url") or "").strip()
    if not url:
        return ""
    filename = str(meta.get("name") or f"{stem}.pdf").strip() or f"{stem}.pdf"
    suffix = Path(filename).suffix or ".pdf"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200 or not resp.content:
            return ""
        fd, path = tempfile.mkstemp(prefix=f"perso-{stem}-", suffix=suffix)
        with os.fdopen(fd, "wb") as handle:
            handle.write(resp.content)
        return path
    except Exception:
        return ""


def _ensure_upload_filename(file_path: str, desired_base: str) -> str:
    if not file_path or not desired_base:
        return file_path
    src = Path(file_path)
    suffix = src.suffix or ".pdf"
    desired_name = f"{desired_base}{suffix}"
    if src.name == desired_name:
        return file_path
    try:
        temp_dir = Path(tempfile.mkdtemp(prefix="perso-rename-"))
        target = temp_dir / desired_name
        shutil.copyfile(src, target)
        return str(target)
    except Exception:
        return file_path


def _upload_arbeitsvertrag(page: Page, payload: dict, tracker: FieldTracker | None = None) -> None:
    pdf_path = _find_angebot_file()
    if not pdf_path:
        print("[HINWEIS] Kein Angebots-/Vertrags-PDF in perso-input gefunden – überspringe Dokument-Upload.")
        if tracker:
            tracker.skip("uploads", "arbeitsvertrag", "vorhanden", "fehlend")
        return
    docs_before = _extract_documents_table(page)
    match = _find_document_match(docs_before, ["arbeitsvertrag", "vertrag"])
    if match:
        print(
            "[INFO] Arbeitsvertrag bereits vorhanden – Upload übersprungen. "
            f"(file='{match.get('file', '')}', desc='{match.get('description', '')}')"
        )
        if tracker:
            tracker.skip("uploads", "arbeitsvertrag", "vorhanden", "vorhanden")
        return
    uploaded = _upload_document_with_modal(
        page=page,
        file_path=pdf_path,
        folder_label="- Arbeitsvertrag",
        folder_value="3",
        bemerkung_text=_build_vertrag_bemerkung(payload),
    )
    if not uploaded:
        print("[WARNUNG] Arbeitsvertrag-Upload konnte nicht gestartet werden.")
    found = _wait_for_document_present(page, ["arbeitsvertrag", "vertrag"], timeout_s=14)
    docs_after = _extract_documents_table(page)
    if tracker:
        if found or _document_present(docs_after, ["arbeitsvertrag", "vertrag"]):
            tracker.ok("uploads", "arbeitsvertrag", "vorhanden", "vorhanden")
        else:
            tracker.missing("uploads", "arbeitsvertrag", "vorhanden", "fehlend")


def _upload_additional_documents(page: Page, payload: dict, tracker: FieldTracker | None = None) -> None:
    uploads = payload.get("uploads") if isinstance(payload, dict) else {}
    if not isinstance(uploads, dict):
        uploads = {}

    valid_until_infektionsschutz = ""
    if isinstance(uploads.get("infektionsschutz"), dict):
        valid_until_infektionsschutz = _format_date_for_ui(str(uploads["infektionsschutz"].get("validUntil", "")).strip())

    infektionsschutz_from_date = _subtract_years(valid_until_infektionsschutz, 2) or time.strftime("%d.%m.%Y")

    immatrikulation_bemerkung, immatrikulation_valid_until_raw = _resolve_immatrikulation_bemerkung(payload)
    immatrikulation_valid_until = _format_date_for_ui(str(immatrikulation_valid_until_raw or "").strip())

    jobs = [
        ("personalbogen", "Personalbogen", "- Personalbogen, Rentenbefreiung & Agenda", "5", ""),
        ("rentenbefreiung", "Rentenbefreiung", "- Personalbogen, Rentenbefreiung & Agenda", "5", ""),
        ("zusatzvereinbarung", "Zusatzvereinbarung", "Dokumente", "1", ""),
        ("sicherheitsbelehrung", "Sicherheitsbelehrung", "Dokumente", "1", ""),
        ("immatrikulation", immatrikulation_bemerkung, "- Imma/Schul", "2", immatrikulation_valid_until),
        (
            "infektionsschutz",
            f"Infektionsschutzbelehrung vom {infektionsschutz_from_date}",
            "- Infektionsschutzbelehrung",
            "9",
            valid_until_infektionsschutz,
        ),
    ]

    print("[INFO] Starte Upload zusätzlicher Dokumente …")
    docs_before = _extract_documents_table(page)
    for stem, bemerkung, folder_label, folder_value, gueltig_bis in jobs:
        keywords = {
            "personalbogen": ["personalbogen", "personalfragebogen"],
            "rentenbefreiung": ["rentenbefreiung"],
            "zusatzvereinbarung": ["zusatzvereinbarung"],
            "sicherheitsbelehrung": ["sicherheitsbelehrung"],
            "immatrikulation": ["immatrikulation", "schulbescheinigung", "imma"],
            "infektionsschutz": ["infektionsschutz"],
        }.get(stem, [stem])
        match = None
        if stem == "personalbogen":
            # Extra strict: require a real file name with a known extension.
            for entry in docs_before:
                file_text = str(entry.get("file") or "").strip()
                desc_text = str(entry.get("description") or "").strip()
                normalized = _normalize_doc_text(f"{file_text} {desc_text}")
                if "personalbogen" not in normalized and "personalfragebogen" not in normalized:
                    continue
                if not _entry_has_real_file(entry):
                    continue
                ext = Path(file_text).suffix.lower()
                if file_text and ext in {".pdf", ".png", ".jpg", ".jpeg"}:
                    match = entry
                    break
        if match is None:
            match = _find_document_match(docs_before, keywords, valid_until=gueltig_bis, match_fields=("file", "description"))
        if match:
            print(
                f"[INFO] Dokument bereits vorhanden – überspringe: {stem} "
                f"(file='{match.get('file', '')}', desc='{match.get('description', '')}')"
            )
            if tracker:
                tracker.skip("uploads", stem, "vorhanden", "vorhanden")
            continue
        file_path = _find_input_file_by_stem(stem)
        temp_downloaded = False
        temp_renamed = False
        downloaded_path = ""
        renamed_path = ""
        if not file_path:
            file_path = _download_upload_to_temp(uploads, stem)
            temp_downloaded = bool(file_path)
            downloaded_path = file_path
        if not file_path:
            print(f"[HINWEIS] Zusatzdokument nicht gefunden: {stem}.* (in PERSO_INPUT_DIR)")
            continue
        if stem == "rentenbefreiung":
            renamed = _ensure_upload_filename(file_path, "rentenbefreiung")
            if renamed != file_path:
                file_path = renamed
                renamed_path = renamed
                temp_renamed = True
        print(f"[INFO] Lade zusätzliches Dokument hoch: {Path(file_path).name}")
        uploaded = _upload_document_with_modal(
            page=page,
            file_path=file_path,
            folder_label=folder_label,
            folder_value=folder_value,
            bemerkung_text=bemerkung,
            gueltig_bis=gueltig_bis,
        )
        if temp_downloaded and downloaded_path:
            try:
                os.remove(downloaded_path)
            except Exception:
                pass
        if temp_renamed:
            try:
                parent = None
                if renamed_path:
                    os.remove(renamed_path)
                    parent = Path(renamed_path).parent
                if parent and parent.name.startswith("perso-rename-"):
                    parent.rmdir()
            except Exception:
                pass
        if not uploaded:
            print(f"[WARNUNG] Upload fehlgeschlagen: {Path(file_path).name}")
        found = _wait_for_document_present(page, keywords, valid_until=gueltig_bis, timeout_s=14)
        docs_after = _extract_documents_table(page)
        if tracker:
            if found or _document_present(docs_after, keywords, valid_until=gueltig_bis):
                tracker.ok("uploads", stem, "vorhanden", "vorhanden")
            else:
                tracker.missing("uploads", stem, "vorhanden", "fehlend")
        docs_before = docs_after


def _set_input_value(locator, value: str) -> bool:
    if locator.count() == 0:
        return False
    locator.first.evaluate(
        """(node, val) => {
            node.value = val;
            node.dispatchEvent(new Event('input', { bubbles: true }));
            node.dispatchEvent(new Event('change', { bubbles: true }));
            node.dispatchEvent(new Event('blur', { bubbles: true }));
        }""",
        value,
    )
    return True


def _set_input_value_force(locator, value: str) -> bool:
    if locator.count() == 0:
        return False
    locator.first.evaluate(
        """(node, val) => {
            node.removeAttribute('readonly');
            node.removeAttribute('disabled');
            node.value = val;
            node.dispatchEvent(new Event('input', { bubbles: true }));
            node.dispatchEvent(new Event('change', { bubbles: true }));
            node.dispatchEvent(new Event('blur', { bubbles: true }));
        }""",
        value,
    )
    return True


def _type_text(locator, text: str, delay_ms: int = 20) -> bool:
    if locator.count() == 0:
        return False
    try:
        locator.first.click()
    except Exception:
        pass
    try:
        locator.first.fill("")
    except Exception:
        pass
    try:
        locator.first.type(text, delay=delay_ms)
        return True
    except Exception:
        return False


def _log_locator_state(locator, label: str) -> None:
    if locator.count() == 0:
        print(f"[DEBUG] {label}: locator=0")
        return
    try:
        info = locator.first.evaluate(
            """(el) => ({
                tag: el.tagName,
                id: el.id || '',
                name: el.name || '',
                cls: el.className || '',
                value: el.value || '',
                readonly: !!el.readOnly,
                disabled: !!el.disabled,
                visible: !!(el.offsetParent),
            })"""
        )
        print(f"[DEBUG] {label}: {info}")
    except Exception as exc:
        print(f"[DEBUG] {label}: state fehlgeschlagen: {exc}")


def _prefer_editable_input(target: Union[Frame, Page], selector: str) -> Locator:
    candidates = [
        f"{selector}.writeInput",
        f"{selector}:not([readonly]):not([disabled])",
        selector,
    ]
    for sel in candidates:
        try:
            loc = target.locator(sel).first
            if loc.count() > 0:
                return loc
        except Exception:
            continue
    return target.locator(selector).first


def _debug_autocomplete_lists(list_locators: list[Locator], label: str) -> None:
    try:
        counts = []
        for idx, loc in enumerate(list_locators):
            counts.append(f"{idx}={loc.count()}")
        print(f"[DEBUG] {label}: autocomplete list counts: {', '.join(counts) if counts else 'none'}")
    except Exception as exc:
        print(f"[DEBUG] {label}: autocomplete list count fehlgeschlagen: {exc}")


def _force_autocomplete_hidden_fields(input_locator, label_text: str, bn: str) -> None:
    if input_locator.count() == 0:
        return
    try:
        input_locator.first.evaluate(
            """(el, args) => {
                const { label, bn } = args || {};
                const form = el.closest('form') || document;
                const id = (el.getAttribute('id') || '').toLowerCase();
                const name = (el.getAttribute('name') || '').toLowerCase();
                const scopeKey = id || name || '';
                if (label) {
                    el.value = label;
                }
                if (label) {
                    el.setAttribute('data-value', label);
                }
                if (bn) {
                    el.setAttribute('data-id', bn);
                    el.setAttribute('data-bn', bn);
                }
                const pools = [
                    form.querySelectorAll('input[type="hidden"]'),
                    document.querySelectorAll('input[type="hidden"]'),
                    form.querySelectorAll('input, select, textarea'),
                    document.querySelectorAll('input, select, textarea')
                ];
                const hiddenInputs = Array.from(new Set(
                    pools.flatMap((list) => Array.from(list))
                ));
                const setHiddenValue = (node, val) => {
                    node.value = val;
                    node.dispatchEvent(new Event('input', { bubbles: true }));
                    node.dispatchEvent(new Event('change', { bubbles: true }));
                };
                hiddenInputs.forEach((node) => {
                    const key = `${node.id || ''} ${node.name || ''}`.toLowerCase();
                    if (!key) return;
                    const isActualField = key.includes('tatsaechliche_krankenkasse');
                    const isMainField = key.includes('krankenkasse') && !isActualField;
                    if (scopeKey === 'krankenkasse' && isActualField) return;
                    if (scopeKey === 'tatsaechliche_krankenkasse' && isMainField) return;
                    const isSameField = scopeKey && key.includes(scopeKey);
                    const isKasseField = key.includes('krankenkasse');
                    if (!isSameField && !(isKasseField && !scopeKey)) return;
                    if (bn && (key.includes('bn') || key.includes('id') || key.includes('key'))) {
                        setHiddenValue(node, bn);
                    } else if (label && isKasseField && !key.includes('bn')) {
                        setHiddenValue(node, label);
                    }
                });
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
                el.dispatchEvent(new Event('blur', { bubbles: true }));
            }""",
            {"label": label_text, "bn": bn},
        )
    except Exception:
        return


def _commit_autocomplete_value(input_locator, label_text: str, bn: str) -> None:
    if input_locator.count() == 0 or not label_text:
        return
    try:
        input_locator.first.evaluate(
            """(el, args) => {
                const { label, bn } = args || {};
                if (!label) return;
                el.value = label;
                el.setAttribute('value', label);
                el.setAttribute('data-value', label);
                if (bn) {
                    el.setAttribute('data-id', bn);
                    el.setAttribute('data-bn', bn);
                }
                const events = ['input', 'change', 'blur', 'focusout', 'keyup', 'keydown'];
                events.forEach((name) => el.dispatchEvent(new Event(name, { bubbles: true })));
                try {
                    if (window.jQuery && typeof window.jQuery === 'function') {
                        const $el = window.jQuery(el);
                        if ($el.autocomplete) {
                            try { $el.autocomplete('search', label); } catch (e) {}
                            const data = $el.data('ui-autocomplete') || $el.data('autocomplete');
                            if (data && typeof data._trigger === 'function') {
                                const item = { label, value: label, id: bn || label, bn: bn || '' };
                                data._trigger('select', null, { item });
                                data._trigger('change', null, { item });
                            }
                        }
                        try { $el.trigger('autocompleteselect', { item: { label, value: label } }); } catch (e) {}
                        try { $el.trigger('autocompletechange', { item: { label, value: label } }); } catch (e) {}
                    }
                } catch (e) {}
                // As last resort, update nearby hidden inputs, but only for the same field.
                const form = el.closest('form') || document;
                const id = (el.getAttribute('id') || '').toLowerCase();
                const name = (el.getAttribute('name') || '').toLowerCase();
                const scopeKey = id || name || '';
                const pools = [
                    form.querySelectorAll('input[type="hidden"]'),
                    document.querySelectorAll('input[type="hidden"]'),
                    form.querySelectorAll('input, select, textarea'),
                    document.querySelectorAll('input, select, textarea')
                ];
                const hiddenInputs = Array.from(new Set(
                    pools.flatMap((list) => Array.from(list))
                ));
                const setHiddenValue = (node, val) => {
                    node.value = val;
                    node.dispatchEvent(new Event('input', { bubbles: true }));
                    node.dispatchEvent(new Event('change', { bubbles: true }));
                };
                hiddenInputs.forEach((node) => {
                    const key = `${node.id || ''} ${node.name || ''}`.toLowerCase();
                    if (!key) return;
                    const isActualField = key.includes('tatsaechliche_krankenkasse');
                    const isMainField = key.includes('krankenkasse') && !isActualField;
                    if (scopeKey === 'krankenkasse' && isActualField) return;
                    if (scopeKey === 'tatsaechliche_krankenkasse' && isMainField) return;
                    const isSameField = scopeKey && key.includes(scopeKey);
                    const isKasseField = key.includes('krankenkasse');
                    if (!isSameField && !(isKasseField && !scopeKey)) return;
                    const wantsBn = key.includes('bn') || key.includes('id') || key.includes('key');
                    if (bn && wantsBn) {
                        setHiddenValue(node, bn);
                    } else if (isKasseField && !key.includes('bn')) {
                        setHiddenValue(node, label);
                    }
                });
            }""",
            {"label": label_text, "bn": bn},
        )
    except Exception:
        return


def _debug_krankenkasse_state(target: Union[Frame, Page], input_locator, field_label: str) -> None:
    if input_locator.count() == 0:
        print(f"[DEBUG] {field_label}: locator nicht gefunden.")
        return
    try:
        info = input_locator.first.evaluate(
            """(el) => {
                const form = el.closest('form') || document;
                const value = el.value || '';
                const hidden = Array.from(form.querySelectorAll('input[type="hidden"]'))
                    .filter((node) => {
                        const key = `${node.id || ''} ${node.name || ''}`.toLowerCase();
                        return key.includes('krankenkasse');
                    })
                    .map((node) => ({
                        id: node.id || '',
                        name: node.name || '',
                        value: node.value || ''
                    }));
                const allHidden = Array.from(document.querySelectorAll('input[type="hidden"]'))
                    .filter((node) => {
                        const key = `${node.id || ''} ${node.name || ''}`.toLowerCase();
                        return key.includes('krankenkasse');
                    })
                    .map((node) => ({
                        id: node.id || '',
                        name: node.name || '',
                        value: node.value || ''
                    }));
                return { value, hidden, allHidden };
            }"""
        )
        hidden = info.get("hidden") if isinstance(info, dict) else []
        all_hidden = info.get("allHidden") if isinstance(info, dict) else []
        print(
            f"[DEBUG] {field_label}: value='{info.get('value') if isinstance(info, dict) else ''}' "
            f"hidden={hidden} all_hidden={all_hidden}"
        )
    except Exception as exc:
        print(f"[DEBUG] {field_label}: Status-Check fehlgeschlagen: {exc}")


def _verify_input_value(locator, expected: str, field_label: str) -> bool:
    if locator.count() == 0:
        return False
    try:
        current = locator.first.input_value().strip()
    except Exception:
        current = ""
    if expected and current != expected:
        print(f"[WARNUNG] {field_label}: Wert weicht ab (soll='{expected}', ist='{current or '—'}').")
        return False
    return True


def _safe_input_value(locator) -> str:
    if locator.count() == 0:
        return ""
    try:
        return str(locator.first.input_value() or "").strip()
    except Exception:
        return ""


def _safe_select_value(locator) -> str:
    if locator.count() == 0:
        return ""
    try:
        return str(locator.first.evaluate("(node) => node.value") or "").strip()
    except Exception:
        return ""


def _normalize_doc_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    return re.sub(r"[^a-z0-9]+", "", text)


def _normalize_valid_until(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r"\s+", "", text)


def _has_allowed_extension(filename: str) -> bool:
    ext = Path(str(filename or "")).suffix.lower()
    return ext in {".pdf", ".png", ".jpg", ".jpeg"}


def _entry_has_real_file(entry: dict) -> bool:
    if not isinstance(entry, dict):
        return False
    if entry.get("has_download"):
        return True
    return _has_allowed_extension(str(entry.get("file") or ""))


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
            file_cell = cells.nth(1)
            file_text = file_cell.inner_text().strip()
            desc_text = cells.nth(2).inner_text().strip()
            valid_text = cells.nth(4).inner_text().strip()
            if not file_text and not desc_text:
                continue
            has_download = False
            try:
                has_download = file_cell.locator("a").count() > 0
            except Exception:
                has_download = False
            entries.append(
                {
                    "file": file_text,
                    "description": desc_text,
                    "valid_until": valid_text,
                    "has_download": has_download,
                }
            )
        except Exception:
            continue
    return entries


def _find_document_match(
    docs: list[dict],
    keywords: list[str],
    valid_until: str = "",
    *,
    match_fields: tuple[str, ...] = ("file", "description"),
) -> dict | None:
    if not docs:
        return None
    normalized_docs = []
    for entry in docs:
        if not _entry_has_real_file(entry):
            continue
        normalized_docs.append(
            {
                "file": _normalize_doc_text(entry.get("file", "")),
                "description": _normalize_doc_text(entry.get("description", "")),
                "valid_until": str(entry.get("valid_until") or "").strip(),
                "raw": entry,
            }
        )
    normalized_keywords = [_normalize_doc_text(key) for key in keywords if key]
    for doc in normalized_docs:
        fields = [doc.get(field, "") for field in match_fields]
        if any(k and any(k in field for field in fields) for k in normalized_keywords):
            if valid_until:
                expected = _normalize_valid_until(valid_until)
                actual = _normalize_valid_until(doc["valid_until"])
                if expected and actual and expected not in actual and actual not in expected:
                    continue
            return doc.get("raw")
    return None


def _document_present(
    docs: list[dict],
    keywords: list[str],
    valid_until: str = "",
    *,
    match_fields: tuple[str, ...] = ("file", "description"),
) -> bool:
    return _find_document_match(docs, keywords, valid_until=valid_until, match_fields=match_fields) is not None


def _wait_for_document_present(
    page: Page,
    keywords: list[str],
    valid_until: str = "",
    *,
    timeout_s: float = 12.0,
    match_fields: tuple[str, ...] = ("file", "description"),
) -> bool:
    deadline = time.time() + max(1.0, timeout_s)
    while time.time() < deadline:
        docs = _extract_documents_table(page)
        if _document_present(docs, keywords, valid_until=valid_until, match_fields=match_fields):
            return True
        time.sleep(0.4)
    return False


def _select_autocomplete_by_typing(
    input_locator,
    label_text: str,
    field_label: str,
) -> bool:
    if input_locator.count() == 0 or not label_text:
        return False
    if not _type_text(input_locator, label_text):
        return False
    time.sleep(0.2)
    try:
        input_locator.first.press("ArrowDown")
        input_locator.first.press("Enter")
    except Exception:
        pass
    time.sleep(0.2)
    _set_input_value(input_locator, label_text)
    _commit_autocomplete_value(input_locator, label_text, _extract_bn(label_text))
    return _verify_input_value(input_locator, label_text, field_label)


def _set_select_value(locator, value: str) -> bool:
    if locator.count() == 0:
        return False
    try:
        locator.first.select_option(value=value)
        return True
    except Exception:
        return False


def _set_select_value_with_fallback(locator, value: str, label: str | None = None) -> bool:
    if locator.count() == 0:
        return False
    try:
        locator.first.evaluate("(node) => { node.removeAttribute('disabled'); }")
    except Exception:
        pass
    if value and _set_select_value(locator, value):
        return True
    if label:
        try:
            locator.first.select_option(label=label)
            return True
        except Exception:
            return False


def _get_select_value(locator) -> str:
    if locator.count() == 0:
        return ""
    try:
        return str(locator.first.evaluate("(node) => node.value") or "").strip()
    except Exception:
        return ""


def _force_set_select_value(locator, value: str) -> bool:
    if locator.count() == 0:
        return False
    try:
        locator.first.evaluate(
            """(node, val) => {
                node.removeAttribute('disabled');
                node.value = val;
                node.dispatchEvent(new Event('input', { bubbles: true }));
                node.dispatchEvent(new Event('change', { bubbles: true }));
                node.dispatchEvent(new Event('blur', { bubbles: true }));
            }""",
            value,
        )
        return True
    except Exception:
        return False


def _set_select_value_logged(locator, value: str, field_label: str) -> bool:
    if locator.count() == 0:
        print(f"[WARNUNG] Feld nicht gefunden: {field_label}")
        return False
    try:
        locator.first.evaluate("(node) => { node.removeAttribute('disabled'); }")
    except Exception:
        pass
    ok = _set_select_value(locator, value)
    actual = _get_select_value(locator)
    if ok and actual == value:
        print(f"[OK] {field_label} gesetzt → {value}")
        return True
    if not ok or actual != value:
        forced = _force_set_select_value(locator, value)
        actual = _get_select_value(locator)
        if forced and actual == value:
            print(f"[OK] {field_label} per Fallback gesetzt → {value}")
            return True
    print(f"[WARNUNG] {field_label} nicht gesetzt (soll={value}, ist={actual or '—'})")
    return False


def _parse_language_entries(value) -> list[dict]:
    if not value:
        return []
    if isinstance(value, list):
        parts = [str(item).strip() for item in value if str(item).strip()]
        raw = ", ".join(parts)
    else:
        raw = str(value).strip()
    if not raw:
        return []
    items = [part.strip() for part in re.split(r"[,\n;/]+", raw) if part.strip()]
    entries: list[dict] = []
    for item in items:
        match = re.match(r"^(.*?)\s*\((.*?)\)\s*$", item)
        if match:
            language = match.group(1).strip()
            level = match.group(2).strip()
        else:
            language = item.strip()
            level = ""
        if language:
            entries.append({"language": language, "level": level})
    return entries


def _fill_language_fields(target: Union[Frame, Page], entries: list[dict]) -> None:
    pairs = [
        ("sprache01a", "sprache01b"),
        ("sprache02a", "sprache02b"),
        ("sprache03a", "sprache03b"),
        ("sprache04a", "sprache04b"),
    ]
    for idx, (lang_field, level_field) in enumerate(pairs):
        if idx >= len(entries):
            break
        entry = entries[idx]
        language = entry.get("language", "")
        level = entry.get("level", "")
        if language:
            loc = target.locator(f"[name='{lang_field}'], #{lang_field}")
            if _set_input_value_force(loc, language):
                print(f"[OK] sedcard {lang_field} → {language}")
            else:
                print(f"[WARNUNG] sedcard {lang_field} nicht gesetzt.")
        if level:
            loc = target.locator(f"[name='{level_field}'], #{level_field}")
            if _set_input_value_force(loc, level):
                print(f"[OK] sedcard {level_field} → {level}")
            else:
                print(f"[WARNUNG] sedcard {level_field} nicht gesetzt.")
    if len(entries) > len(pairs):
        extras = ", ".join([e.get("language", "") for e in entries[len(pairs):] if e.get("language")])
        if extras:
            loc = target.locator("[name='sprache04'], #sprache04")
            if _set_input_value_force(loc, extras):
                print(f"[OK] sedcard sprache04 → {extras}")
            else:
                print("[WARNUNG] sedcard sprache04 nicht gesetzt.")


def _map_schulabschluss_to_value(value) -> str | None:
    if not value:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if "ohne" in normalized:
        return "1"
    if "haupt" in normalized or "volks" in normalized:
        return "2"
    if "mittlere" in normalized or "reife" in normalized or "realschule" in normalized or "gleichwertig" in normalized:
        return "3"
    if "abitur" in normalized:
        return "4"
    if "unbekannt" in normalized:
        return "9"
    return None


def _fill_stammdaten_fields(page: Page, payload: dict, tracker: FieldTracker | None = None) -> None:
    schulabschluss_raw = _pick_payload_value(payload, ["schulabschluss"])
    if not schulabschluss_raw:
        print("[HINWEIS] Kein Schulabschluss im JSON – überspringe Stammdaten.")
        return

    target, panel = _open_stammdaten_tab(page, "stammdaten", "Stammdaten")
    if not target or not panel:
        print("[WARNUNG] Tab 'Stammdaten' nicht gefunden.")
        if tracker:
            tracker.missing("stammdaten", "tab", "sichtbar", "nicht gefunden")
        return

    edit_icon = panel.locator("img[src*='b_edit.png'][onclick*='makeEdited'], img[title='Bearbeiten']").first
    if edit_icon.count() == 0:
        edit_icon = target.locator("img[src*='b_edit.png'][onclick*='makeEdited'], img[title='Bearbeiten']").first
    if edit_icon.count() > 0:
        try:
            edit_icon.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            edit_icon.click(force=True)
            print("[OK] Stammdaten Edit-Stift geklickt.")
        except Exception as exc:
            try:
                clicked = edit_icon.evaluate("el => { el.click(); return true; }")
                if clicked:
                    print("[OK] Stammdaten Edit-Stift per JS geklickt.")
                else:
                    print(f"[WARNUNG] Stammdaten Edit-Stift nicht klickbar: {exc}")
            except Exception as js_exc:
                print(f"[WARNUNG] Stammdaten Edit-Stift nicht klickbar: {exc} / JS: {js_exc}")
    else:
        print("[WARNUNG] Stammdaten Edit-Stift nicht gefunden.")

    value = _map_schulabschluss_to_value(schulabschluss_raw)
    if value:
        loc = panel.locator("#schulabschluss_taetigkeitschluessel, [name='schulabschluss_taetigkeitschluessel']")
        current = _safe_select_value(loc)
        if current == value:
            print(f"[OK] Stammdaten schulabschluss bereits korrekt → {schulabschluss_raw}")
            if tracker:
                tracker.skip("stammdaten", "schulabschluss", value, current)
        else:
            label = None
            try:
                label = loc.locator(f"option[value='{value}']").first.inner_text()
            except Exception:
                label = None
            if _set_select_value_with_fallback(loc, value, label=label):
                print(f"[OK] Stammdaten schulabschluss → {schulabschluss_raw}")
            else:
                print("[WARNUNG] Stammdaten schulabschluss nicht gesetzt.")
        if tracker:
            actual = _safe_select_value(loc)
            if actual == value:
                tracker.ok("stammdaten", "schulabschluss", value, actual)
            else:
                tracker.missing("stammdaten", "schulabschluss", value, actual)
    else:
        print(f"[WARNUNG] Schulabschluss nicht gemappt: {schulabschluss_raw}")

    save_button = panel.locator(
        "input[type='submit'].speichern, input[type='submit'][value*='Daten speichern'], button:has-text('Daten speichern')"
    ).first
    if save_button.count() > 0:
        try:
            save_button.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            save_button.click()
            print("[OK] Stammdaten gespeichert.")
        except Exception as exc:
            try:
                # Fallback: Click via JS even if hidden.
                save_button.evaluate("el => el.click()")
                print("[OK] Stammdaten gespeichert (JS-Fallback).")
            except Exception:
                print(f"[WARNUNG] Stammdaten speichern fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] Stammdaten Speichern-Button nicht gefunden.")


def _dismiss_ui_overlay(page: Page) -> None:
    overlay = page.locator("div.ui-widget-overlay.ui-front").first
    try:
        if overlay.count() > 0 and overlay.is_visible():
            page.keyboard.press("Escape")
            time.sleep(0.2)
    except Exception:
        pass
    try:
        if overlay.count() > 0 and overlay.is_visible():
            close_button = page.locator(
                "div.ui-dialog:visible button:has-text('Schließen'), "
                "div.ui-dialog:visible button:has-text('Fertig'), "
                "div.ui-dialog:visible button.ui-dialog-titlebar-close"
            ).first
            if close_button.count() > 0:
                close_button.click()
                time.sleep(0.2)
    except Exception:
        pass
    try:
        page.evaluate(
            """() => {
                document.querySelectorAll('div.ui-widget-overlay.ui-front').forEach((el) => {
                    el.style.pointerEvents = 'none';
                    el.style.display = 'none';
                    el.style.visibility = 'hidden';
                });
            }"""
        )
    except Exception:
        pass


def _select_autocomplete_by_bn(
    target: Union[Frame, Page],
    input_locator,
    bn: str,
    fallback_text: str,
    field_label: str = "krankenkasse",
) -> bool:
    locator_count = input_locator.count()
    if locator_count == 0:
        print(f"[WARNUNG] {field_label}: Eingabefeld nicht gefunden – übersprungen.")
        return False
    _log_locator_state(input_locator, f"{field_label} input (vor)")
    if not bn:
        if fallback_text:
            print(f"[WARNUNG] {field_label}: BN fehlt, versuche Textsuche → {fallback_text}")
            try:
                input_locator.first.click()
            except Exception:
                pass
            input_locator.first.fill(fallback_text)
            list_locators: list[Locator] = []
            try:
                list_locators.append(target.locator("ul.ui-autocomplete li.ui-menu-item"))
            except Exception:
                pass
            if isinstance(target, Frame):
                try:
                    list_locators.append(target.page.locator("ul.ui-autocomplete li.ui-menu-item"))
                except Exception:
                    pass
            else:
                try:
                    for frame in target.frames:
                        list_locators.append(frame.locator("ul.ui-autocomplete li.ui-menu-item"))
                except Exception:
                    pass
            _debug_autocomplete_lists(list_locators, f"{field_label} (fallback)")
            deadline = time.time() + 6
            while time.time() < deadline:
                for list_locator in list_locators:
                    item = list_locator.filter(has_text=fallback_text).first
                    if item.count() > 0 and item.is_visible():
                        try:
                            item.click()
                            print(f"[OK] {field_label}: Autocomplete Treffer → {fallback_text}")
                            return True
                        except Exception:
                            break
                time.sleep(0.2)
            _set_input_value(input_locator, fallback_text)
            print(f"[WARNUNG] {field_label}: Kein Autocomplete Treffer – Fallback gesetzt → {fallback_text}")
            _log_locator_state(input_locator, f"{field_label} input (fallback)")
            return False
        print(f"[WARNUNG] {field_label}: BN fehlt und kein Fallback-Text – übersprungen.")
        return False
    def _collect_lists() -> list[Locator]:
        lists: list[Locator] = []
        try:
            lists.append(target.locator("ul.ui-autocomplete li.ui-menu-item"))
        except Exception:
            pass
        if isinstance(target, Frame):
            try:
                lists.append(target.page.locator("ul.ui-autocomplete li.ui-menu-item"))
            except Exception:
                pass
        else:
            try:
                for frame in target.frames:
                    lists.append(frame.locator("ul.ui-autocomplete li.ui-menu-item"))
            except Exception:
                pass
        return lists

    def _try_select_from_lists(list_locators: list[Locator], bn_value: str, label_hint: str) -> str:
        deadline = time.time() + 4
        while time.time() < deadline:
            for list_locator in list_locators:
                item = None
                if bn_value:
                    item = list_locator.filter(has_text=f"[Bn: {bn_value}]").first
                if (item is None or item.count() == 0) and label_hint:
                    item = list_locator.filter(has_text=label_hint).first
                if item is None or item.count() == 0 or not item.is_visible():
                    continue
                try:
                    label_text = item.inner_text().strip()
                except Exception:
                    label_text = ""
                try:
                    item.click()
                except Exception:
                    try:
                        item.evaluate("el => el.click()")
                    except Exception:
                        pass
                return label_text
            time.sleep(0.2)
        return ""

    # Try label first (most autocompletes search by name), then BN as fallback.
    label_text = ""
    if fallback_text:
        _type_text(input_locator, fallback_text)
        list_locators = _collect_lists()
        _debug_autocomplete_lists(list_locators, f"{field_label} (label search)")
        label_text = _try_select_from_lists(list_locators, bn, fallback_text)

    if not label_text:
        _type_text(input_locator, bn)
        list_locators = _collect_lists()
        _debug_autocomplete_lists(list_locators, f"{field_label} (bn search)")
        label_text = _try_select_from_lists(list_locators, bn, fallback_text)

    if label_text:
        _set_input_value(input_locator, label_text)
        _force_autocomplete_hidden_fields(input_locator, label_text, bn)
        _commit_autocomplete_value(input_locator, label_text, bn)
        print(f"[OK] {field_label}: Autocomplete Treffer → {label_text}")
        _log_locator_state(input_locator, f"{field_label} input (nach)")
        if not _verify_input_value(input_locator, label_text, field_label):
            _select_autocomplete_by_typing(input_locator, label_text, field_label)
        return True

    if fallback_text:
        _set_input_value(input_locator, fallback_text)
        _force_autocomplete_hidden_fields(input_locator, fallback_text, bn)
        _commit_autocomplete_value(input_locator, fallback_text, bn)
        _select_autocomplete_by_typing(input_locator, fallback_text, field_label)
        print(f"[WARNUNG] {field_label}: Kein Autocomplete Treffer für BN {bn} – Fallback gesetzt → {fallback_text}")
        _log_locator_state(input_locator, f"{field_label} input (bn fallback)")
    return False


def _fill_notfallkontakt(page: Page, payload: dict, tracker: FieldTracker | None = None) -> None:
    name = _pick_payload_value(payload, ["notfall_name", "notfallkontakt_name"])
    relation = _pick_payload_value(payload, ["verwandschaftsgrad", "notfallkontakt_relation"])
    phone = _pick_payload_value(payload, ["notfall_tel", "notfallkontakt_tel", "notfallkontakt_telefon"])
    nested = payload.get("notfallkontakt")
    if isinstance(nested, dict):
        name = name or _pick_payload_value(nested, ["name", "notfall_name", "notfallkontakt_name"])
        relation = relation or _pick_payload_value(nested, ["relation", "verwandschaftsgrad", "notfallkontakt_relation"])
        phone = phone or _pick_payload_value(nested, ["telefon", "phone", "notfall_tel", "notfallkontakt_tel"])
    print(f"[DEBUG] Notfallkontakt Werte: name='{name}' relation='{relation}' phone='{phone}'")
    if not any([name, relation, phone]):
        print("[HINWEIS] Kein Notfallkontakt im JSON – überspringe.")
        return
    print("[INFO] Öffne Notfallkontakt und trage Werte ein …")

    panel_id = "administration_user_stammdaten_tabs_notfallkontakt"
    target, panel = _open_stammdaten_tab(page, "notfallkontakt", "Notfallkontakt")
    if not target or not panel:
        print("[WARNUNG] Tab 'Notfallkontakt' nicht gefunden.")
        if tracker:
            tracker.missing("notfallkontakt", "tab", "sichtbar", "nicht gefunden")
        return

    edit_icon = panel.locator("img[src*='b_edit.png'][onclick*='makeEdited'], img[title='Bearbeiten']").first
    if edit_icon.count() > 0:
        try:
            edit_icon.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            if not edit_icon.is_visible():
                print("[WARNUNG] Notfallkontakt Edit-Stift nicht sichtbar.")
            edit_icon.click(timeout=3000)
            print("[OK] Notfallkontakt Edit-Stift geklickt.")
        except Exception as exc:
            try:
                clicked = edit_icon.evaluate("el => { el.click(); return true; }")
                if clicked:
                    print("[OK] Notfallkontakt Edit-Stift per JS geklickt.")
                else:
                    print(f"[WARNUNG] Notfallkontakt Edit-Stift Klick fehlgeschlagen: {exc}")
            except Exception as js_exc:
                print(f"[WARNUNG] Notfallkontakt Edit-Stift Klick fehlgeschlagen: {exc} / JS: {js_exc}")
    else:
        print("[WARNUNG] Notfallkontakt Edit-Stift nicht gefunden.")

    try:
        target.evaluate(
            """(panelId) => {
                if (typeof makeEdited === 'function') {
                    try { makeEdited(); } catch (e) {}
                }
                const panel = document.getElementById(panelId);
                if (!panel) return;
                panel.querySelectorAll('input, select, textarea').forEach((el) => {
                    el.removeAttribute('readonly');
                    el.removeAttribute('disabled');
                });
                panel.querySelectorAll('.editWorker').forEach((el) => {
                    el.classList.remove('hideElement');
                    el.classList.add('showElement');
                    el.style.display = 'inline-block';
                    el.removeAttribute('disabled');
                });
                const save = panel.querySelector("input.speichern, input[type='submit'][value*='Daten speichern']");
                if (save) {
                    save.classList.remove('hideElement');
                    save.style.display = 'inline-block';
                    save.removeAttribute('disabled');
                }
            }""",
            panel_id,
        )
    except Exception:
        pass

    if name:
        loc = panel.locator("#notfallkontakt_name, [name='notfallkontakt_name']")
        print(f"[DEBUG] notfallkontakt_name Locator count={loc.count()}")
        current = _safe_input_value(loc)
        if current == name:
            print(f"[OK] notfallkontakt_name bereits korrekt → {name}")
            if tracker:
                tracker.skip("notfallkontakt", "name", name, current)
        elif _set_input_value_force(loc, name):
            print(f"[OK] notfallkontakt_name → {name}")
        if tracker:
            actual = _safe_input_value(loc)
            if actual == name:
                tracker.ok("notfallkontakt", "name", name, actual)
            else:
                tracker.missing("notfallkontakt", "name", name, actual)
    if phone:
        loc = panel.locator("#notfallkontakt_telefon, [name='notfallkontakt_telefon']")
        print(f"[DEBUG] notfallkontakt_telefon Locator count={loc.count()}")
        current = _safe_input_value(loc)
        if current == phone:
            print(f"[OK] notfallkontakt_telefon bereits korrekt → {phone}")
            if tracker:
                tracker.skip("notfallkontakt", "telefon", phone, current)
        elif _set_input_value_force(loc, phone):
            print(f"[OK] notfallkontakt_telefon → {phone}")
        if tracker:
            actual = _safe_input_value(loc)
            if actual == phone:
                tracker.ok("notfallkontakt", "telefon", phone, actual)
            else:
                tracker.missing("notfallkontakt", "telefon", phone, actual)
    if relation:
        loc = panel.locator("#notfallkontakt_relation, [name='notfallkontakt_relation']")
        print(f"[DEBUG] notfallkontakt_relation Locator count={loc.count()}")
        current = _safe_input_value(loc)
        if current == relation:
            print(f"[OK] notfallkontakt_relation bereits korrekt → {relation}")
            if tracker:
                tracker.skip("notfallkontakt", "relation", relation, current)
        elif _set_input_value_force(loc, relation):
            print(f"[OK] notfallkontakt_relation → {relation}")
        if tracker:
            actual = _safe_input_value(loc)
            if actual == relation:
                tracker.ok("notfallkontakt", "relation", relation, actual)
            else:
                tracker.missing("notfallkontakt", "relation", relation, actual)

    save_button = panel.locator("input[type='submit'].speichern, input[type='submit'][value*='Daten speichern']").first
    print(f"[DEBUG] Notfallkontakt Speichern-Button count={save_button.count()}")
    if save_button.count() > 0:
        try:
            save_button.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            save_button.click()
            print("[OK] Notfallkontakt gespeichert.")
        except Exception as exc:
            try:
                target.evaluate(
                    """(panelId) => {
                        const panel = document.getElementById(panelId);
                        const btn = panel?.querySelector("input[type='submit'][value*='Daten speichern'], input.speichern");
                        if (!btn) return false;
                        btn.classList.remove('hideElement');
                        btn.style.display = 'inline-block';
                        btn.removeAttribute('disabled');
                        try { btn.click(); } catch (e) {}
                        const form = panel.closest('form');
                        if (form) {
                            try { form.requestSubmit ? form.requestSubmit(btn) : form.submit(); } catch (e) {}
                        }
                        return true;
                    }""",
                    panel_id,
                )
                print("[OK] Notfallkontakt gespeichert (JS-Fallback).")
            except Exception:
                print(f"[WARNUNG] Notfallkontakt speichern fehlgeschlagen: {exc}")
    else:
        print("[WARNUNG] Notfallkontakt Speichern-Button nicht gefunden.")


def _resolve_lohnabrechnung_values(payload: dict) -> dict:
    variant = str(payload.get("form_variant", "")).strip().lower()
    if variant == "geringfuegig":
        variant = "gb"
    krankenkasse_value = str(payload.get("krankenkasse_value", "") or "").strip()
    krankenkasse_label = str(payload.get("krankenkasse_label", "") or "").strip()
    krankenkasse_pf = krankenkasse_value or str(payload.get("krankenkasse", "") or "").strip()
    krankenkasse_pf = _resolve_kasse_label(krankenkasse_pf)
    krankenkasse_bn = (
        str(payload.get("krankenkasse_bn") or payload.get("krankenkasse_bn_nummer") or payload.get("krankenkasse_bn_nr") or "")
        .strip()
    )
    if not krankenkasse_bn:
        krankenkasse_bn = _extract_bn(krankenkasse_pf)
    if not krankenkasse_bn and krankenkasse_pf:
        krankenkasse_bn = _resolve_bn_from_name(krankenkasse_pf)
        if krankenkasse_bn:
            print(f"[INFO] krankenkasse: BN via Name-Mapping → {krankenkasse_bn}")
            krankenkasse_pf = _KRANKENKASSE_LABEL_BY_BN.get(krankenkasse_bn, krankenkasse_pf)
    if krankenkasse_pf and krankenkasse_pf not in _KRANKENKASSE_LABEL_BY_BN.values():
        if krankenkasse_label:
            print(f"[INFO] krankenkasse: Fallback Label vorhanden → {krankenkasse_label}")
        else:
            print(f"[WARNUNG] krankenkasse: Kein exakter Treffer in Options → {krankenkasse_pf}")

    vertrag = payload.get("vertrag") or {}
    if not isinstance(vertrag, dict):
        vertrag = {}
    contract_type = str(vertrag.get("contract_type", "")).strip().lower()

    if contract_type == "kb":
        krankenkasse = "Knappschaft Hauptverwaltung [Bn: 98000006]"
        tatsaechliche = krankenkasse_pf
        tatsaechliche_bn = krankenkasse_bn
        personengruppe = "110"
        vertragsform = "4"
        steuerklasse = "1"
    elif contract_type == "gb":
        krankenkasse = "Knappschaft Hauptverwaltung [Bn: 98000006]"
        tatsaechliche = krankenkasse_pf
        tatsaechliche_bn = krankenkasse_bn
        personengruppe = "109"
        vertragsform = "2"
        steuerklasse = "M"
    elif contract_type == "tz":
        krankenkasse = krankenkasse_pf
        tatsaechliche = ""
        tatsaechliche_bn = ""
        personengruppe = "101"
        vertragsform = "2"
        steuerklasse = "1"
    elif variant == "kb":
        krankenkasse = "Knappschaft Hauptverwaltung [Bn: 98000006]"
        tatsaechliche = krankenkasse_pf
        tatsaechliche_bn = krankenkasse_bn
        personengruppe = "110"
        vertragsform = "4"
        steuerklasse = "1"
    elif variant == "gb":
        krankenkasse = "Knappschaft Hauptverwaltung [Bn: 98000006]"
        tatsaechliche = krankenkasse_pf
        tatsaechliche_bn = krankenkasse_bn
        personengruppe = "109"
        vertragsform = "2"
        steuerklasse = "M"
    else:
        krankenkasse = krankenkasse_pf
        tatsaechliche = ""
        tatsaechliche_bn = ""
        personengruppe = "101"
        vertragsform = "2"
        steuerklasse = "1"

    return {
        "variant": variant,
        "krankenkasse": krankenkasse,
        "krankenkasse_bn": "98000006" if contract_type == "kb" else ("98000006" if variant == "kb" else krankenkasse_bn),
        "tatsaechliche_krankenkasse": tatsaechliche,
        "tatsaechliche_bn": tatsaechliche_bn,
        "personengruppe": personengruppe,
        "vertragsform": vertragsform,
        "steuerklasse": steuerklasse,
        "taetigkeitsbezeichnung": "63301",
    }


def _fill_lohnabrechnung_fields(page: Page, payload: dict, tracker: FieldTracker | None = None) -> None:
    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    values = _resolve_lohnabrechnung_values(payload)
    panel = target.locator("#administration_user_stammdaten_tabs_lohnabrechnung")

    try:
        panel.wait_for(state="visible", timeout=8000)
    except Exception:
        pass
    try:
        target.evaluate(
            """() => {
                if (typeof makeEdited === 'function') {
                    try { makeEdited(); } catch (e) {}
                }
                const panel = document.querySelector('#administration_user_stammdaten_tabs_lohnabrechnung');
                if (!panel) return;
                panel.querySelectorAll('input, select, textarea').forEach((el) => {
                    el.removeAttribute('readonly');
                    el.removeAttribute('disabled');
                });
                const save = panel.querySelector("input.speichern, input[type='submit'][value*='Daten speichern']");
                if (save) {
                    save.classList.remove('hideElement');
                    save.style.display = 'inline-block';
                    save.removeAttribute('disabled');
                }
            }"""
        )
    except Exception:
        pass

    schulabschluss_raw = _pick_payload_value(payload, ["schulabschluss"])
    if schulabschluss_raw:
        schulabschluss_value = _map_schulabschluss_to_value(schulabschluss_raw)
        if schulabschluss_value:
            loc = panel.locator("#schulabschluss_taetigkeitschluessel, [name='schulabschluss_taetigkeitschluessel']")
            current = _safe_select_value(loc)
            if current == schulabschluss_value:
                print("[OK] Schulabschluss bereits korrekt – überspringe.")
                if tracker:
                    tracker.skip("lohnabrechnung", "schulabschluss", schulabschluss_value, current)
            else:
                _set_select_value_logged(loc, schulabschluss_value, "Schulabschluss")
            if tracker:
                actual = _safe_select_value(loc)
                if actual == schulabschluss_value:
                    tracker.ok("lohnabrechnung", "schulabschluss", schulabschluss_value, actual)
                else:
                    tracker.missing("lohnabrechnung", "schulabschluss", schulabschluss_value, actual)
        else:
            print(f"[WARNUNG] Schulabschluss nicht gemappt: {schulabschluss_raw}")

    krankenkasse_input = _prefer_editable_input(panel, "#krankenkasse, [name='krankenkasse']")
    if tracker and krankenkasse_input.count() == 0:
        tracker.missing("lohnabrechnung", "krankenkasse", values["krankenkasse"], "nicht gefunden")
    try:
        sel_all = "#krankenkasse, [name='krankenkasse']"
        sel_write = "#krankenkasse.writeInput, [name='krankenkasse'].writeInput"
        sel_editable = "#krankenkasse:not([readonly]):not([disabled]), [name='krankenkasse']:not([readonly]):not([disabled])"
        print(
            "[DEBUG] krankenkasse locator counts: "
            f"all={panel.locator(sel_all).count()} "
            f"write={panel.locator(sel_write).count()} "
            f"editable={panel.locator(sel_editable).count()}"
        )
    except Exception:
        pass
    current_kasse = _safe_input_value(krankenkasse_input)
    if current_kasse == values["krankenkasse"]:
        print("[OK] krankenkasse bereits korrekt – überspringe.")
        if tracker:
            tracker.skip("lohnabrechnung", "krankenkasse", values["krankenkasse"], current_kasse)
    else:
        _select_autocomplete_by_bn(
            target,
            krankenkasse_input,
            values["krankenkasse_bn"],
            values["krankenkasse"],
            "krankenkasse",
        )
        _verify_input_value(krankenkasse_input, values["krankenkasse"], "krankenkasse")
        _commit_autocomplete_value(krankenkasse_input, values["krankenkasse"], values["krankenkasse_bn"])
        _debug_krankenkasse_state(target, krankenkasse_input, "krankenkasse")
    if tracker:
        actual = _safe_input_value(krankenkasse_input)
        if actual == values["krankenkasse"]:
            tracker.ok("lohnabrechnung", "krankenkasse", values["krankenkasse"], actual)
        else:
            tracker.missing("lohnabrechnung", "krankenkasse", values["krankenkasse"], actual)
    if values["tatsaechliche_krankenkasse"]:
        tatsaechliche_input = _prefer_editable_input(
            panel, "#tatsaechliche_krankenkasse, [name='tatsaechliche_krankenkasse']"
        )
        if tracker and tatsaechliche_input.count() == 0:
            tracker.missing(
                "lohnabrechnung",
                "tatsaechliche_krankenkasse",
                values["tatsaechliche_krankenkasse"],
                "nicht gefunden",
            )
        try:
            sel_all = "#tatsaechliche_krankenkasse, [name='tatsaechliche_krankenkasse']"
            sel_write = "#tatsaechliche_krankenkasse.writeInput, [name='tatsaechliche_krankenkasse'].writeInput"
            sel_editable = (
                "#tatsaechliche_krankenkasse:not([readonly]):not([disabled]), "
                "[name='tatsaechliche_krankenkasse']:not([readonly]):not([disabled])"
            )
            print(
                "[DEBUG] tatsaechliche_krankenkasse locator counts: "
                f"all={panel.locator(sel_all).count()} "
                f"write={panel.locator(sel_write).count()} "
                f"editable={panel.locator(sel_editable).count()}"
            )
        except Exception:
            pass
        current_tk = _safe_input_value(tatsaechliche_input)
        if current_tk == values["tatsaechliche_krankenkasse"]:
            print("[OK] tatsaechliche_krankenkasse bereits korrekt – überspringe.")
            if tracker:
                tracker.skip(
                    "lohnabrechnung",
                    "tatsaechliche_krankenkasse",
                    values["tatsaechliche_krankenkasse"],
                    current_tk,
                )
        else:
            _select_autocomplete_by_bn(
                target,
                tatsaechliche_input,
                values["tatsaechliche_bn"],
                values["tatsaechliche_krankenkasse"],
                "tatsaechliche_krankenkasse",
            )
            _verify_input_value(
                tatsaechliche_input, values["tatsaechliche_krankenkasse"], "tatsaechliche_krankenkasse"
            )
            _commit_autocomplete_value(
                tatsaechliche_input,
                values["tatsaechliche_krankenkasse"],
                values["tatsaechliche_bn"],
            )
            _debug_krankenkasse_state(target, tatsaechliche_input, "tatsaechliche_krankenkasse")
        if tracker:
            actual = _safe_input_value(tatsaechliche_input)
            if actual == values["tatsaechliche_krankenkasse"]:
                tracker.ok(
                    "lohnabrechnung",
                    "tatsaechliche_krankenkasse",
                    values["tatsaechliche_krankenkasse"],
                    actual,
                )
            else:
                tracker.missing(
                    "lohnabrechnung",
                    "tatsaechliche_krankenkasse",
                    values["tatsaechliche_krankenkasse"],
                    actual,
                )
        if values["krankenkasse"] and values["krankenkasse"] != values["tatsaechliche_krankenkasse"]:
            _select_autocomplete_by_bn(
                target,
                krankenkasse_input,
                values["krankenkasse_bn"],
                values["krankenkasse"],
                "krankenkasse",
            )
            _verify_input_value(krankenkasse_input, values["krankenkasse"], "krankenkasse")
            _commit_autocomplete_value(krankenkasse_input, values["krankenkasse"], values["krankenkasse_bn"])
            _debug_krankenkasse_state(target, krankenkasse_input, "krankenkasse (post)")
    print(
        "[INFO] Lohnabrechnung Zielwerte: "
        f"personengruppe={values['personengruppe']}, "
        f"vertragsform={values['vertragsform']}, "
        f"steuerklasse={values['steuerklasse']}"
    )
    ok_personengruppe = _set_select_value_logged(panel.locator("#personengruppe"), values["personengruppe"], "Personengruppe")
    _set_input_value(panel.locator("#taetigkeitsbezeichnung"), values["taetigkeitsbezeichnung"])
    ok_vertragsform = _set_select_value_logged(
        panel.locator("#vertragsform_taetigkeitschluessel"), values["vertragsform"], "Vertragsform"
    )
    ok_arbeitnehmer = _set_select_value_logged(
        panel.locator("#arbeitnehmerueberlassung_taetigkeitschluessel"),
        "2",
        "Arbeitnehmerüberlassung",
    )
    ok_steuerklasse = _set_select_value_logged(panel.locator("#steuerklasse"), values["steuerklasse"], "Steuerklasse")
    if tracker:
        if not ok_personengruppe:
            tracker.missing("lohnabrechnung", "personengruppe", values["personengruppe"], "nicht gesetzt")
        if not ok_vertragsform:
            tracker.missing("lohnabrechnung", "vertragsform", values["vertragsform"], "nicht gesetzt")
        if not ok_arbeitnehmer:
            tracker.missing("lohnabrechnung", "arbeitnehmerueberlassung", "2", "nicht gesetzt")
        if not ok_steuerklasse:
            tracker.missing("lohnabrechnung", "steuerklasse", values["steuerklasse"], "nicht gesetzt")
    if tracker:
        for field_id, locator, expected in [
            ("personengruppe", panel.locator("#personengruppe"), values["personengruppe"]),
            ("taetigkeitsbezeichnung", panel.locator("#taetigkeitsbezeichnung"), values["taetigkeitsbezeichnung"]),
            ("vertragsform", panel.locator("#vertragsform_taetigkeitschluessel"), values["vertragsform"]),
            ("arbeitnehmerueberlassung", panel.locator("#arbeitnehmerueberlassung_taetigkeitschluessel"), "2"),
            ("steuerklasse", panel.locator("#steuerklasse"), values["steuerklasse"]),
        ]:
            actual = _safe_select_value(locator) if field_id != "taetigkeitsbezeichnung" else _safe_input_value(locator)
            if actual == expected:
                tracker.ok("lohnabrechnung", field_id, expected, actual)
            else:
                tracker.missing("lohnabrechnung", field_id, expected, actual)

    try:
        target.evaluate(
            "typeof taetigkeitsschluessel_generieren === 'function' && taetigkeitsschluessel_generieren()"
        )
    except Exception:
        pass
    try:
        target.evaluate(
            "typeof beitragsgruppenschluessel_generieren === 'function' && beitragsgruppenschluessel_generieren()"
        )
    except Exception:
        pass


def _fill_vertragsdaten(page: Page) -> None:
    entries = [
        ("01.01.2026", "14,96"),
        ("01.09.2026", "15,33"),
        ("01.04.2027", "15,87"),
    ]

    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    tab = target.locator("a:has-text('Vertragsdaten')").first
    if tab.count() == 0:
        print("[HINWEIS] Tab 'Vertragsdaten' nicht gefunden – überspringe Vertragsdaten.")
        return

    href = tab.get_attribute("href") or ""
    tab.click()

    panel = target
    if href.startswith("#"):
        panel = target.locator(href)
    try:
        panel.wait_for(state="visible", timeout=8000)
    except Exception:
        pass

    rows = panel.locator("tr")
    filled = 0
    for i in range(rows.count()):
        if filled >= len(entries):
            break
        row = rows.nth(i)
        date_input = row.locator(
            "input[type='text'].datepicker, input[type='text'][name*='datum'], input[type='text'][id*='datum'], "
            "input[type='text'][name*='von'], input[type='text'][id*='von']"
        )
        amount_input = row.locator(
            "input[type='text'][name*='lohn'], input[type='text'][id*='lohn'], input[type='text'][name*='betrag'], "
            "input[type='text'][id*='betrag'], input[type='text'][name*='stunden'], input[type='text'][id*='stunden']"
        )
        if date_input.count() == 0 or amount_input.count() == 0:
            continue
        date_value, amount_value = entries[filled]
        _set_input_value(date_input, date_value)
        _set_input_value(amount_input, amount_value)
        filled += 1

    if filled < len(entries):
        print("[HINWEIS] Vertragsdaten unvollständig gesetzt – bitte HTML/Selector prüfen.")


def _click_daten_speichern(page: Page, timeout_seconds: float = 6.0) -> bool:
    target: Union[Frame, Page] = page
    frame = page.frame(name="inhalt")
    if frame:
        target = frame

    panel = target.locator("#administration_user_stammdaten_tabs_lohnabrechnung")
    if panel.count() > 0:
        try:
            panel.first.wait_for(state="visible", timeout=4000)
        except Exception:
            pass

    selectors = [
        "#administration_user_stammdaten_tabs_lohnabrechnung input.editWorker.button.speichern.showElement",
        "#administration_user_stammdaten_tabs_lohnabrechnung input[type='submit'][value='Daten speichern']",
        "input.editWorker.button.speichern.showElement",
        "input[type='submit'][value='Daten speichern']",
        "div[style*='padding-top:10px'] input[type='submit'][value='Daten speichern']",
        "form input[type='submit'][value='Daten speichern']",
    ]
    button = None
    for sel in selectors:
        locator = target.locator(sel).first
        if locator.count() > 0:
            button = locator
            break
    if button is None:
        try:
            clicked = target.evaluate(
                """() => {
                    const panel = document.querySelector('#administration_user_stammdaten_tabs_lohnabrechnung');
                    const btn = panel?.querySelector("input[type='submit'][value='Daten speichern']");
                    if (btn) { btn.click(); return true; }
                    const fallback = document.querySelector("input[type='submit'][value='Daten speichern']");
                    if (fallback) { fallback.click(); return true; }
                    return false;
                }"""
            )
        except Exception:
            clicked = False
        if clicked:
            print("[OK] 'Daten speichern' geklickt (JS fallback).")
            return True
        return False
    deadline = time.time() + max(1.0, timeout_seconds)
    while time.time() < deadline:
        try:
            button.wait_for(state="visible", timeout=800)
            button.scroll_into_view_if_needed()
            button.click()
            print("[OK] 'Daten speichern' geklickt.")
            return True
        except Exception:
            time.sleep(0.3)
    return False


def _click_fertig_in_dialog(page: Page, timeout_seconds: float = 3.0) -> bool:
    dialog = page.locator(
        "div.ui-dialog.ui-dialog-buttons:has(button:has-text('Fertig')), "
        "div.ui-dialog.ui-widget.ui-widget-content.ui-corner-all.ui-front.ui-dialog-buttons"
        ":has(button:has-text('Fertig'))"
    ).first
    try:
        dialog.wait_for(state="visible", timeout=int(timeout_seconds * 1000))
    except Exception:
        return False
    fertig_button = dialog.locator("button:has-text('Fertig')").first
    if fertig_button.count() == 0:
        return False
    try:
        fertig_button.click()
        print("[OK] Modal bestätigt: 'Fertig'.")
        return True
    except Exception:
        return False


def _wait_for_dialog_closed(page: Page, timeout_seconds: float = 6.0) -> None:
    dialog = page.locator("div.ui-dialog.ui-dialog-buttons").first
    try:
        dialog.wait_for(state="hidden", timeout=int(timeout_seconds * 1000))
    except Exception:
        pass


def _run_personal_step(
    step_label: str,
    action_fn,
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    wait_seconds: int = 0,
) -> None:
    headless = config.HEADLESS if headless is None else headless
    slowmo_ms = config.SLOWMO_MS if slowmo_ms is None else slowmo_ms

    state_path = Path(config.STATE_PATH)
    if not state_path.exists():
        raise RuntimeError(f"[FEHLER] Kein gespeicherter Login-State unter {state_path}. Bitte zuerst 'login' ausführen.")

    payload = _load_personalbogen_json()
    email = str(payload.get("email", "")).strip()
    if not email:
        raise RuntimeError("[FEHLER] Keine E-Mail im personalbogen-JSON gefunden.")

    tracker = FieldTracker(attempt=1, max_retries=0)
    print(f"[INFO] Schritt gestartet: {step_label}")
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
                raise RuntimeError("[FEHLER] Kein Treffer geklickt – Abbruch.")

            action_fn(target_page, payload, tracker)

            if wait_seconds > 0:
                print(f"[INFO] Pause für manuelle Schritte ({wait_seconds}s) …")
                time.sleep(max(1, wait_seconds))

            browser.close()

        sys.stdout = prev_stdout
        sys.stderr = prev_stderr
        combined_log = stdout_buffer.getvalue() + "\n" + stderr_buffer.getvalue()
        if _has_retry_warning(combined_log):
            print("[WARNUNG] Warnungen erkannt – Schritt läuft weiter (kein Abbruch).")
        tracker.log_summary()
        missing = tracker.missing_fields()
        if missing:
            print(f"[WARNUNG] Fehlende Felder erkannt – Schritt läuft weiter: {missing}")
        print("[INFO] Schritt abgeschlossen.")
    except Exception as exc:
        sys.stdout = prev_stdout
        sys.stderr = prev_stderr
        tracker.error("run", "exception", str(exc))
        tracker.log_summary()
        raise


def run_mitarbeiter_lohnabrechnung(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    wait_seconds: int = 0,
):
    def _action(target_page: Page, payload: dict, tracker: FieldTracker) -> None:
        if _open_lohnabrechnung_and_edit(target_page):
            _fill_lohnabrechnung_fields(target_page, payload, tracker=tracker)
            if _click_fertig_in_dialog(target_page, timeout_seconds=5.0):
                _wait_for_dialog_closed(target_page, timeout_seconds=6.0)
            if not _click_daten_speichern(target_page, timeout_seconds=8.0):
                print("[WARNUNG] 'Daten speichern' nicht gefunden/geklickt.")
                tracker.missing("lohnabrechnung", "daten_speichern", "geklickt", "fehlgeschlagen")
        else:
            tracker.missing("lohnabrechnung", "tab", "geöffnet", "fehlgeschlagen")

    _run_personal_step(
        "lohnabrechnung",
        _action,
        headless=headless,
        slowmo_ms=slowmo_ms,
        wait_seconds=wait_seconds,
    )


def run_mitarbeiter_stammdaten(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    wait_seconds: int = 0,
):
    def _action(target_page: Page, payload: dict, tracker: FieldTracker) -> None:
        _fill_stammdaten_fields(target_page, payload, tracker=tracker)
        _fill_notfallkontakt(target_page, payload, tracker=tracker)

    _run_personal_step(
        "stammdaten",
        _action,
        headless=headless,
        slowmo_ms=slowmo_ms,
        wait_seconds=wait_seconds,
    )


def run_mitarbeiter_sedcard(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    wait_seconds: int = 0,
):
    def _action(target_page: Page, payload: dict, tracker: FieldTracker) -> None:
        if _open_sedcard(target_page):
            print("[INFO] Sedcard geöffnet.")
            _fill_sedcard_fields(target_page, payload, tracker=tracker)
        else:
            tracker.missing("sedcard", "tab", "geöffnet", "fehlgeschlagen")

    _run_personal_step(
        "sedcard",
        _action,
        headless=headless,
        slowmo_ms=slowmo_ms,
        wait_seconds=wait_seconds,
    )


def run_mitarbeiter_vertragsdaten(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    wait_seconds: int = 0,
):
    def _action(target_page: Page, payload: dict, tracker: FieldTracker) -> None:
        if _open_vertragsdaten(target_page):
            print("[INFO] Vertragsdaten geöffnet.")
            _fill_grundlohn_history(target_page)
            _fill_vertrag_history(target_page, payload)
            _fill_tage_fremd(target_page, payload)
            _fill_sonstiges(target_page, payload)
            _fill_eintritt_austritt(target_page, payload)
        else:
            tracker.missing("vertragsdaten", "tab", "geöffnet", "fehlgeschlagen")

    _run_personal_step(
        "vertragsdaten",
        _action,
        headless=headless,
        slowmo_ms=slowmo_ms,
        wait_seconds=wait_seconds,
    )


def run_mitarbeiter_dokumente(
    headless: bool | None = None,
    slowmo_ms: int | None = None,
    wait_seconds: int = 0,
):
    def _action(target_page: Page, payload: dict, tracker: FieldTracker) -> None:
        if _open_mitarbeiterinformationen(target_page):
            print("[INFO] Mitarbeiterinformationen geöffnet.")
            _upload_arbeitsvertrag(target_page, payload, tracker=tracker)
            _upload_additional_documents(target_page, payload, tracker=tracker)
        else:
            tracker.missing("uploads", "tab", "geöffnet", "fehlgeschlagen")

    _run_personal_step(
        "dokumente",
        _action,
        headless=headless,
        slowmo_ms=slowmo_ms,
        wait_seconds=wait_seconds,
    )


def run_mitarbeiter_vervollstaendigen(
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

    max_retries = int(os.environ.get("PERSONAL_SCRAPER_MAX_RETRIES", "1"))
    if max_retries > 1:
        max_retries = 1
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
                page = context.new_page()

                print("[INFO] Lade Startseite mit gespeicherter Session …")
                page.goto(config.BASE_URL, wait_until="domcontentloaded")

                try:
                    target = _open_user_overview(page)
                except Exception as exc:
                    print(f"[WARNUNG] Übersicht nicht geladen (Session evtl. abgelaufen): {exc} – versuche Login …")
                    page = browser.new_page()
                    do_login(page)
                    target = _open_user_overview(page)

                search_input = _locate_search_input(target)
                if search_input.count() == 0:
                    try:
                        if isinstance(target, Frame):
                            target.page.wait_for_selector("input[type='search']", timeout=6000)
                        else:
                            target.wait_for_selector("input[type='search']", timeout=6000)
                    except Exception:
                        pass
                    search_input = _locate_search_input(target)
                if search_input.count() == 0:
                    try:
                        if isinstance(target, Frame):
                            print(f"[DEBUG] user.php Frames: {[f.name for f in target.page.frames]}")
                        else:
                            print(f"[DEBUG] user.php Frames: {[f.name for f in target.frames]}")
                    except Exception:
                        pass
                    raise RuntimeError("[FEHLER] Suchfeld in user.php nicht gefunden.")

                search_input.fill(email)
                time.sleep(0.2)
                print(f"[INFO] Suche nach E-Mail: {email}")

                target_page = _click_lastname_link(target, email)
                if target_page:
                    if _open_lohnabrechnung_and_edit(target_page):
                        _fill_lohnabrechnung_fields(target_page, payload, tracker=tracker)
                        if _click_fertig_in_dialog(target_page, timeout_seconds=5.0):
                            _wait_for_dialog_closed(target_page, timeout_seconds=6.0)
                        if not _click_daten_speichern(target_page, timeout_seconds=8.0):
                            print("[WARNUNG] 'Daten speichern' nicht gefunden/geklickt.")
                            tracker.missing("lohnabrechnung", "daten_speichern", "geklickt", "fehlgeschlagen")
                    _fill_stammdaten_fields(target_page, payload, tracker=tracker)
                    _fill_notfallkontakt(target_page, payload, tracker=tracker)
                    if _open_sedcard(target_page):
                        print("[INFO] Sedcard geöffnet.")
                        _fill_sedcard_fields(target_page, payload, tracker=tracker)
                    else:
                        tracker.missing("sedcard", "tab", "geöffnet", "fehlgeschlagen")
                    if _open_vertragsdaten(target_page):
                        print("[INFO] Vertragsdaten geöffnet.")
                        _fill_grundlohn_history(target_page)
                        _fill_vertrag_history(target_page, payload)
                        _fill_tage_fremd(target_page, payload)
                        _fill_sonstiges(target_page, payload)
                        _fill_eintritt_austritt(target_page, payload)
                    if _open_mitarbeiterinformationen(target_page):
                        print("[INFO] Mitarbeiterinformationen geöffnet.")
                        _upload_arbeitsvertrag(target_page, payload, tracker=tracker)
                        _upload_additional_documents(target_page, payload, tracker=tracker)
                    else:
                        tracker.missing("uploads", "tab", "geöffnet", "fehlgeschlagen")
                    print(f"[INFO] Pause für manuelle Schritte ({wait_seconds}s) …")
                    deadline = time.time() + max(1, wait_seconds)
                    while time.time() < deadline:
                        _click_fertig_in_dialog(target_page, timeout_seconds=0.5)
                        time.sleep(0.5)
                else:
                    print("[INFO] Kein Treffer geklickt – keine Pause.")

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
