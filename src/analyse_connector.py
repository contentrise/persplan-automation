import pandas as pd
import os

# === KONFIGURATION ===
EINGANGS_ORDNER = "eingang"
EXPORTS_ORDNER = "exports"
AUSGANGS_ORDNER = "ausgang"

def analyse(anfragen_csv, keine_schichten_csv, output_csv):
    print(f"📊 Starte Abgleich: {keine_schichten_csv} ↔ {anfragen_csv}")

    # 1️⃣ CSVs laden
    df_a = pd.read_csv(anfragen_csv, encoding="utf-8-sig")  # PersPlan-Ergebnisse
    df_b = pd.read_csv(keine_schichten_csv, sep=";", encoding="utf-8-sig")  # Liste der 0-Stunden-Mitarbeiter

    # 2️⃣ Normalisieren
    df_a["personalnummer"] = df_a["personalnummer"].astype(str).str.strip()
    df_b["PersNr"] = df_b["PersNr"].astype(str).str.strip()

    # 3️⃣ Prüfen, ob es für die PersNr in PersPlan „Anfragen“ gibt
    def hat_anfragen(persnr):
        subset = df_a[df_a["personalnummer"] == persnr]
        echte_anfragen = subset[subset["typ"] == "Anfrage"]
        return len(echte_anfragen) > 0

    df_b["Hat_Anfrage"] = df_b["PersNr"].apply(hat_anfragen)
    df_b["Muss_angeschrieben_werden"] = ~df_b["Hat_Anfrage"]

    # 4️⃣ Statistik
    total = len(df_b)
    hat_anfrage = df_b["Hat_Anfrage"].sum()
    muss_anschreiben = df_b["Muss_angeschrieben_werden"].sum()

    print(f"\n📈 Analyse-Ergebnis:")
    print(f"👥 Insgesamt: {total} Mitarbeiter mit 0 Schichten")
    print(f"✅ {hat_anfrage} haben bereits Anfragen")
    print(f"❗ {muss_anschreiben} müssen kontaktiert werden\n")

    # 5️⃣ Speichern
    os.makedirs(AUSGANGS_ORDNER, exist_ok=True)
    df_b.to_csv(output_csv, sep=";", index=False, encoding="utf-8-sig")
    print(f"💾 Ergebnis gespeichert unter: {output_csv}")

if __name__ == "__main__":
    # === Schritt 1: Neueste Anfragen-Datei im Export-Ordner finden ===
    export_files = [
        os.path.join(EXPORTS_ORDNER, f)
        for f in os.listdir(EXPORTS_ORDNER)
        if f.startswith("anfragen_") and f.endswith(".csv")
    ]

    if not export_files:
        print("⚠️ Keine 'anfragen_*.csv'-Datei im Ordner 'exports/' gefunden.")
        exit()

    # Neueste Datei anhand Änderungsdatum wählen
    anfragen_csv = max(export_files, key=os.path.getmtime)

    # === Schritt 2: Neueste 'Keine Schichten'-Datei im Eingangsordner finden ===
    files = [
        os.path.join(EINGANGS_ORDNER, f)
        for f in os.listdir(EINGANGS_ORDNER)
        if "Keine_Schichten" in f and f.endswith(".csv")
    ]

    if not files:
        print("⚠️ Keine Datei mit 'Keine_Schichten' im Namen im Eingangsordner gefunden.")
        exit()

    # Neueste Datei wählen
    keine_schichten_csv = max(files, key=os.path.getmtime)

    # === Schritt 3: Ausgabe-Dateiname automatisch ableiten ===
    month_label = (
        os.path.basename(keine_schichten_csv)
        .replace("Check_Keine_Schichten_", "")
        .replace(".csv", "")
    )
    output_csv = os.path.join(AUSGANGS_ORDNER, f"Analyse_Keine_Schichten_vs_Anfragen_{month_label}.csv")

    # === Schritt 4: Analyse starten ===
    analyse(anfragen_csv, keine_schichten_csv, output_csv)
