import os
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

# --- .env für Basisdaten (Login etc.) ---
load_dotenv(override=True)

# --- Basis-Konfiguration aus .env ---
BASE_URL   = os.getenv("PERSPLAN_BASE_URL", "https://greatstaff.persplan.net/").rstrip("/") + "/"
USERNAME   = os.getenv("PERSPLAN_USER", "")
PASSWORD   = os.getenv("PERSPLAN_PASS", "")
HEADLESS   = os.getenv("HEADLESS", "false").lower() in ("1", "true", "yes")
SLOWMO_MS  = int(os.getenv("SLOWMO_MS", "0"))
STATE_PATH = os.getenv("STATE_PATH", "auth/state.json")

# --- Zusätzliche Optionen aus configuration.txt ---
CONFIG_PATH = Path(__file__).parent / "configuration.txt"

# Standardwerte
CONFIG = {
    "month": str(datetime.now().month),
    "vertragstyp": "2",       # Kurzf. Beschäftigte
    "year": str(datetime.now().year),
    "export_dir": "exports",
    "max_ma_loop": "0",       # 0 = alle, >0 = Limit für Testläufe

    # Erweiterung: Urlaubsplanung
    "urlaub_month": str(datetime.now().month),
    "urlaub_year": str(datetime.now().year),
    "save_uu": "false",       # Neu: Steuert, ob im Modal gespeichert wird

    # Tagesplan (alt)
    "tagesplan_in_tagen": "7",

    # Kleidungsrückgabe
    "kleidungs_max_rows": "1",
    "kleidungs_debug_rows": "",
}


def parse_config_line(line: str):
    """Hilfsfunktion: 'key=value' Zeilen parsen"""
    if "=" not in line:
        return None, None
    key, value = line.strip().split("=", 1)
    return key.strip().lower(), value.strip()


def _parse_int_setting(raw_value: str | None, fallback: int) -> int:
    """Int-Parser, der deutsche Tausender-/Dezimaltrennzeichen toleriert."""
    if raw_value is None:
        return fallback
    normalized = raw_value.strip().replace(".", "").replace(",", "").replace(" ", "")
    if not normalized or normalized in {"+", "-"}:
        return fallback
    try:
        return int(normalized)
    except ValueError:
        print(f"[WARNUNG] Kann '{raw_value}' nicht als Integer lesen – verwende {fallback}.")
        return fallback


def _parse_int_setting(raw_value: str | None, fallback: int) -> int:
    """Robust int-Parser, toleriert deutsche Tausender-/Dezimaltrennzeichen."""
    if raw_value is None:
        return fallback
    normalized = raw_value.strip().replace(".", "").replace(",", "").replace(" ", "")
    if not normalized or normalized in {"+", "-"}:
        return fallback
    try:
        return int(normalized)
    except ValueError:
        print(f"[WARNUNG] Kann '{raw_value}' nicht als Integer lesen – verwende {fallback}.")
        return fallback

# --- configuration.txt einlesen ---
if CONFIG_PATH.exists():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, value = parse_config_line(line)
            if key and value:
                CONFIG[key] = value
    print(f"[INFO] Benutzerkonfiguration geladen aus {CONFIG_PATH}")
else:
    print(f"[WARNUNG] Keine configuration.txt gefunden, verwende Standardwerte.")

# --- Globale Variablen ---
MONTH = int(CONFIG.get("month", datetime.now().month))
VERTRAGSTYP = CONFIG.get("vertragstyp", "2")
YEAR = CONFIG.get("year", str(datetime.now().year))
EXPORT_DIR = CONFIG.get("export_dir", "exports")
MAX_MA_LOOP = _parse_int_setting(CONFIG.get("max_ma_loop", "0"), 0)  # 0 = keine Begrenzung

# --- Erweiterte Werte für Urlaubsplanung ---
URLAUB_MONTH = int(CONFIG.get("urlaub_month", MONTH))
URLAUB_YEAR  = int(CONFIG.get("urlaub_year", YEAR))
SAVE_UU = CONFIG.get("save_uu", "false").lower() in ("1", "true", "yes")
TAGESPLAN_IN_TAGEN = _parse_int_setting(CONFIG.get("tagesplan_in_tagen", "7"), 7)
KLEIDUNGS_MAX_ROWS = _parse_int_setting(CONFIG.get("kleidungs_max_rows", "1"), 1)


def _split_debug_rows(value: str) -> set[str]:
    return {entry.strip() for entry in value.split(",") if entry.strip()}


KLEIDUNGS_DEBUG_ROWS = _split_debug_rows(
    os.getenv("KLEIDUNGS_DEBUG_ROWS", CONFIG.get("kleidungs_debug_rows", ""))
)

# --- Checks ---
def assert_env_ready():
    missing = []
    if not USERNAME:
        missing.append("PERSPLAN_USER")
    if not PASSWORD:
        missing.append("PERSPLAN_PASS")
    if missing:
        raise RuntimeError(
            f"Fehlende Umgebungsvariablen: {', '.join(missing)}. "
            f"Bitte .env ausfüllen (siehe .env.example)."
        )

print(
    f"[INFO] Aktive Konfiguration: "
    f"Monat={MONTH}, Vertragstyp={VERTRAGSTYP}, Jahr={YEAR}, "
    f"Export={EXPORT_DIR}, Limit={MAX_MA_LOOP}, "
    f"Urlaubsmonat={URLAUB_MONTH}, Urlaubsjahr={URLAUB_YEAR}, "
    f"SaveUU={SAVE_UU}, KleidungsMaxRows={KLEIDUNGS_MAX_ROWS}"
)
