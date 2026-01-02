# Pipedrive Data Sync Script

Syncs LIGR Postgres user data to Pipedrive, using Postgres as the source of truth.

## What It Does

1. **Creates missing organizations** - Any org in Postgres that doesn't exist in Pipedrive gets created
2. **Links persons to organizations** - Using smart merge logic:
   - If person's current Pipedrive org matches ANY of their Postgres orgs → keeps current
   - If no match or no org assigned → assigns first Postgres org
3. **Populates "All LIGR Organizations" field** - Custom text field with all orgs: `"Football West (44), LIGR Demo (347)"`
4. **Tags orphaned contacts** - Pipedrive contacts not in Postgres get tagged with "not in db"

## Files Required

- `pipedrive_sync.py` - The main script
- `query_result_2026-01-01T07_09_46_49449815Z.csv` - Postgres export (same directory)
- `requirements.txt` - Python dependencies

## Running Locally

```bash
pip install -r requirements.txt
python pipedrive_sync.py
```

## Running on Render

### Option 1: Background Worker (Recommended)

1. Create a new **Background Worker** on Render
2. Connect your repo or upload files
3. Set:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python pipedrive_sync.py`
4. Deploy

### Option 2: Render Shell

1. Create a new **Web Service** (minimal, just to get shell access)
2. Use Render Shell to upload files and run manually

## Configuration

Edit these values in `pipedrive_sync.py`:

```python
API_TOKEN = "your_pipedrive_api_token"
CSV_PATH = "your_postgres_export.csv"
DRY_RUN = True  # Set to False to make actual changes
```

## Dry Run Mode

By default, `DRY_RUN = True` - the script will:
- Show what changes WOULD be made
- Not actually modify anything in Pipedrive
- Generate log files showing planned changes

**Review the logs first**, then set `DRY_RUN = False` and run again.

## Output Files

The script generates two CSV log files:

1. `pipedrive_sync_log_YYYYMMDD_HHMMSS.csv` - All person changes
   - email, name, current_org, new_org, org_action, all_orgs, status

2. `pipedrive_orgs_created_YYYYMMDD_HHMMSS.csv` - Organizations created
   - name, pd_id, status

## Rate Limiting

Pipedrive allows 100 requests per 10 seconds. The script includes a 0.12s delay between requests to stay under this limit.

For ~13,000 users + ~9,800 orgs, expect the script to take approximately 30-45 minutes.
