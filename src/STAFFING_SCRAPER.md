# Staffing Scraper Integration

Dieses Repository enthält jetzt ein Polling-Skript (`staffing_scraper_poller.py`), das den neuen manuellen Dienstplan-Scraper aus dem Hub bedient. So richtest du alles ein:

## 1. Environment vorbereiten

Im Projekt-Root (`/var/www/persplan-automation`) läuft bereits ein virtuelles Python-Env (`.venv`). Installiere die zusätzlichen Abhängigkeiten:

```bash
cd /var/www/persplan-automation
source .venv/bin/activate
pip install requests boto3
```

Setze die benötigten Variablen (z. B. in einer `.env` oder direkt in systemd):

| Variable | Beschreibung |
| --- | --- |
| `STAFFING_API_BASE` | Basis-URL der Hub API, Standard `https://api.greatstaff.com`. |
| `STAFFING_RUN_SECRET` | Secret aus der Lambda (`STAFFING_RUN_SECRET` / `STAFFING_SCRAPER_SECRET`). |
| `STAFFING_BUCKET` | S3-Bucket (z. B. `greatstaff-data-storage`). |
| `STAFFING_PLAN_FOLDER` | Ordner im Bucket (Default `staffing/dienstplan`). |
| `SCRAPER_COMMAND` | Aufruf für Playwright (Default `-m src.main schicht-bestaetigen --headless true`). |
| `SCRAPER_POLL_INTERVAL` | Pause zwischen Claim-Versuchen ohne Lauf (Default 60 Sekunden). |
| `SCRAPER_EXPORT_DIR` | Ordner, in dem Playwright CSVs ablegt (Default `exports`). |

Optionale Variablen: `SCRAPER_PYTHON_CMD`, `AWS_REGION`, `STAFFING_FOLDER_DATE`, `STAFFING_LOG_LEVEL`.

## 2. systemd-Service anlegen

`/etc/systemd/system/staffing-scraper.service`:

```
[Unit]
Description=Staffing Scraper Poller
After=network.target

[Service]
WorkingDirectory=/var/www/persplan-automation
Environment="STAFFING_API_BASE=https://api.greatstaff.com"
Environment="STAFFING_RUN_SECRET=***"
Environment="STAFFING_BUCKET=greatstaff-data-storage"
Environment="STAFFING_PLAN_FOLDER=staffing/dienstplan"
ExecStart=/bin/bash -c 'source .venv/bin/activate && python staffing_scraper_poller.py'
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

Anschließend:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now staffing-scraper.service
journalctl -u staffing-scraper.service -f
```

## 3. Ablauf

1. Nutzer klickt im Hub auf „Scraper starten“ → `POST /staffing/scraper/run`.
2. Dieses Skript pollt `/staffing/scraper/claim`. Sobald ein Lauf bereitsteht, wird `python -m src.main schicht-bestaetigen --headless true` ausgeführt.
3. Die neu entstandene CSV landet in `exports/`, wird nach `s3://<Bucket>/<Ordner>/<Datum>/<Datei>` hochgeladen und der Lauf mit `/staffing/scraper/complete` als abgeschlossen gemeldet.
4. Fehler (Playwright, Upload, API) werden automatisch als `status=error` an die API gemeldet.

Damit ersetzt du die bisherigen Cronjobs, und das Hub-Frontend zeigt jederzeit den aktuellen Status der Läufe an. Kopiere diese Dateien (`staffing_scraper_poller.py` & dieses Dokument) auf deinen VPS, passe die Variablen an und starte den Service – fertig.
