# Tracker-X

A GitHub Actions automation that fetches live market data (stock indices,
commodities, and crypto) and writes it to a Google Sheet — updated hourly.

## Tracked Assets

| Asset         | Source       |
|---------------|--------------|
| NASDAQ        | Yahoo Finance |
| S&P 500       | Yahoo Finance |
| DOW JONES     | Yahoo Finance |
| SENSEX        | Yahoo Finance |
| NIFTY 50      | Yahoo Finance |
| NIKKEI 225    | Yahoo Finance |
| GOLD 24 CARAT | Yahoo Finance |
| SILVER        | Yahoo Finance |
| OIL (BRENT)   | Yahoo Finance |
| BITCOIN       | CoinGecko    |
| ETHEREUM      | CoinGecko    |

## Google Sheet Layout

| Column | Content          |
|--------|------------------|
| A      | Asset name       |
| B      | Current price    |
| C      | 52-week low      |
| D      | 52-week high     |
| E      | 5-year low       |
| F      | 5-year high      |

- **Row 14** — last updated timestamp (IST)
- **Row 15** — last updated timestamp (EAT / Uganda)

## Setup

### 1. Google Cloud — Service Account

1. Create a project in [Google Cloud Console](https://console.cloud.google.com).
2. Enable the **Google Sheets API** and **Google Drive API**.
3. Create a **Service Account** and download its JSON key file.
4. Share your Google Sheet with the service account's email address
   (give it **Editor** access).

### 2. GitHub Secrets

Add the following secrets to your repository
(**Settings → Secrets and variables → Actions**):

| Secret name  | Value                                              |
|--------------|----------------------------------------------------|
| `GCP_SA_KEY` | The full contents of the service-account JSON file |

### 3. GitHub Variables / Environment

The Spreadsheet ID is passed as an environment variable in the workflow.
Update `SPREADSHEET_ID` in `.github/workflows/sync_sheet.yml` to match
your own Google Sheet ID (the long string in its URL).

### 4. Trigger

The workflow runs **automatically every hour** and can also be triggered
manually from the **Actions** tab via *workflow_dispatch*.

## Local Development

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Provide required environment variables
export GCP_SA_KEY='{ ... service account JSON ... }'
export SPREADSHEET_ID='your-sheet-id'

# Run
python main.py
```

## Dependencies

See [`requirements.txt`](requirements.txt) for pinned versions.
