IMDB Top 250 scraper

Files
- `imdb_pipeline.py` — Scrapes IMDB Top 250, enriches each movie with year and rating, optionally uploads to BigQuery when `IMDB_UPLOAD=1` and `GOOGLE_APPLICATION_CREDENTIALS` is set.
- `convert_to_csv.py` — Converts `imdb_top_250.json` into `imdb_top_250.csv` with HTML entities unescaped.
- `reorder_bq.py` — Replaces BigQuery table with an ordered copy by `rank`.

Quick start (PowerShell)

1) Create & activate virtualenv
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

2) Install dependencies
```powershell
pip install --upgrade pip
pip install -r requirements.txt
```

3) Run scraper (no upload)
```powershell
python imdb_pipeline.py
```

4) Convert JSON -> CSV
```powershell
python convert_to_csv.py
Get-Content -Path imdb_top_250.csv -TotalCount 10
```

5) Upload to BigQuery (optional)
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS = 'service-account-key.json'
$env:IMDB_UPLOAD = '1'
python imdb_pipeline.py
```

Single-command run

- To run the full pipeline (scrape, enrich, save JSON, convert to CSV) without uploading to BigQuery:

```powershell
python run_all.py
```

- To run and upload to BigQuery (ensure `service-account-key.json` is available and valid):

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS = 'service-account-key.json'
$env:IMDB_UPLOAD = '1'
python run_all.py
```

Notes
- `service-account-key.json` should NOT be committed to git. It's ignored in `.gitignore`.
- If you prefer not to keep data files in the repo, remove `imdb_top_250.json` and `imdb_top_250.csv` from the repository and add them to `.gitignore` (already added).
