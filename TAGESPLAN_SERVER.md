# Tagesplan-Vortag auf dem Server (Cron)

Dieser Scraper (`src/tagesplan_vortag.py`) laeuft komplett headless und legt seine CSV automatisch im Bucket `s3://greatstaff-data-storage/einchecken/<YYYY-MM-DD>/...` ab. Damit kannst du das Repo auf dem Server klonen und per Cron laufen lassen - genauso wie das Schichtbestaetigungs-Skript.

## Vorbereitung

1. Repository auf dem Server ablegen (z. B. `/var/www/persplan-automation`):
   ```bash
   git clone git@github.com:greatstaff/persplan-schichten.git
   cd persplan-schichten
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   playwright install chromium
   ```
2. `.env` mit PersPlan-Daten erzeugen (siehe `.env.example`):
   ```
   PERSPLAN_BASE_URL=https://greatstaff.persplan.net/
   PERSPLAN_USER=***
   PERSPLAN_PASS=***
   HEADLESS=true
   ```
   Optional kannst du hier auch gleich die AWS-Creds hinterlegen:
   ```
   AWS_ACCESS_KEY_ID=***
   AWS_SECRET_ACCESS_KEY=***
   AWS_DEFAULT_REGION=eu-central-1
   ```
3. Einmalig einloggen, damit `auth/state.json` existiert:
   ```bash
   source .venv/bin/activate
   python -m src.main login --headless true
   ```

## Manuell testen

```bash
source .venv/bin/activate
python -m src.main tagesplan-vortag --headless true --wait-seconds 0 --days-back 1
```

* CSV landet lokal in `exports/`.
* Danach Upload nach `s3://greatstaff-data-storage/einchecken/<Zieldatum>/...`.
* `CHECKIN_BUCKET` und `CHECKIN_PREFIX` lassen sich per Env ueberschreiben (Default `greatstaff-data-storage` / `einchecken`).

## Cronjob

Beispiel fuer `/etc/cron.d/tagesplan`:

```
MAILTO=""
0 6 * * * www-data cd /var/www/persplan-automation && \
  source .venv/bin/activate && \
  CHECKIN_BUCKET=greatstaff-data-storage CHECKIN_PREFIX=einchecken \
  python -m src.main tagesplan-vortag --headless true --wait-seconds 0 >> /var/log/tagesplan.log 2>&1
```

So wird jeden Morgen um 06:00 Uhr der Vortag exportiert. `--days-back` oder `--date` kannst du je nach Bedarf anpassen. Die AWS-Creds stellst du - wie bei der Schichtbestaetigung - entweder global (`/etc/environment`) oder direkt in der Cron-Zeile bereit.
