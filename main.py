"""Tracker-X: Fetches market data and syncs it to a Google Sheet."""

import json
import os
from datetime import datetime

import gspread
import pytz
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Sheet layout constants ────────────────────────────────────────────────────
SHEET_DATA_RANGE = "A2:F12"
SHEET_INDIA_TIME_CELL = "A14"
SHEET_UGANDA_TIME_CELL = "A15"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

ASSETS: list[dict] = [
    {"name": "NASDAQ",        "ticker": "^IXIC",  "type": "yahoo"},
    {"name": "S&P 500",       "ticker": "^GSPC",  "type": "yahoo"},
    {"name": "DOW JONES",     "ticker": "^DJI",   "type": "yahoo"},
    {"name": "SENSEX",        "ticker": "^BSESN", "type": "yahoo"},
    {"name": "NIFTY 50",      "ticker": "^NSEI",  "type": "yahoo"},
    {"name": "NIKKEI 225",    "ticker": "^N225",  "type": "yahoo"},
    {"name": "GOLD 24 CARAT", "ticker": "GC=F",   "type": "yahoo"},
    {"name": "SILVER",        "ticker": "SI=F",   "type": "yahoo"},
    {"name": "OIL (BRENT)",   "ticker": "BZ=F",   "type": "yahoo"},
    {"name": "BITCOIN",       "id": "bitcoin",    "type": "crypto"},
    {"name": "ETHEREUM",      "id": "ethereum",   "type": "crypto"},
]

# ── HTTP session with automatic retries ───────────────────────────────────────

def _make_session() -> requests.Session:
    """Return a Session with exponential-backoff retries on transient errors."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


SESSION = _make_session()
_YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt(value: object) -> str:
    """Format a numeric value to 2 decimal places; pass strings through."""
    if isinstance(value, (int, float)):
        return f"{value:.2f}"
    return str(value)


# ── Data fetchers ─────────────────────────────────────────────────────────────

def get_yahoo_data(ticker: str) -> dict | None:
    """Return price, 52-week and 5-year high/low for *ticker* from Yahoo Finance.

    Returns ``None`` on any error so the caller can preserve the last-good
    sheet value instead of overwriting it with an error marker.
    """
    print(f"  Fetching Yahoo: {ticker}")
    try:
        base_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        meta = SESSION.get(base_url, headers=_YAHOO_HEADERS, timeout=15).json()[
            "chart"
        ]["result"][0]["meta"]

        five_year_url = f"{base_url}?range=5y&interval=1d"
        quote_data = SESSION.get(five_year_url, headers=_YAHOO_HEADERS, timeout=15).json()[
            "chart"
        ]["result"][0]["indicators"]["quote"][0]

        valid_highs = [h for h in quote_data["high"] if h is not None]
        valid_lows  = [low for low in quote_data["low"]  if low is not None]

        return {
            "price":    meta.get("regularMarketPrice", "ERROR"),
            "52w_low":  meta.get("fiftyTwoWeekLow",    "ERROR"),
            "52w_high": meta.get("fiftyTwoWeekHigh",   "ERROR"),
            "5y_low":   min(valid_lows)  if valid_lows  else "ERROR",
            "5y_high":  max(valid_highs) if valid_highs else "ERROR",
        }
    except Exception as exc:
        print(f"    Error fetching {ticker}: {exc}")
        return None


def get_crypto_data(crypto_id: str) -> dict | None:
    """Return price, 52-week and 5-year high/low for *crypto_id* from CoinGecko.

    The 5-year range is attempted on a best-effort basis; CoinGecko's free
    tier may reject it, in which case ``'N/A'`` is returned for those fields.
    Returns ``None`` on a fatal fetch error.
    """
    print(f"  Fetching Crypto: {crypto_id}")
    base = "https://api.coingecko.com/api/v3/coins"
    try:
        one_year_url = (
            f"{base}/{crypto_id}/market_chart"
            "?vs_currency=usd&days=365&interval=daily"
        )
        resp = SESSION.get(one_year_url, timeout=15)
        resp.raise_for_status()
        prices_1y = [p[1] for p in resp.json()["prices"]]

        # Best-effort 5-year data
        five_y_low: float | str = "N/A"
        five_y_high: float | str = "N/A"
        try:
            five_year_url = (
                f"{base}/{crypto_id}/market_chart"
                "?vs_currency=usd&days=1825&interval=daily"
            )
            resp5 = SESSION.get(five_year_url, timeout=15)
            resp5.raise_for_status()
            prices_5y = [p[1] for p in resp5.json()["prices"]]
            five_y_low  = min(prices_5y)
            five_y_high = max(prices_5y)
        except Exception:
            pass  # Free-tier limitation; leave as N/A

        return {
            "price":    prices_1y[-1],
            "52w_low":  min(prices_1y),
            "52w_high": max(prices_1y),
            "5y_low":   five_y_low,
            "5y_high":  five_y_high,
        }
    except Exception as exc:
        print(f"    Error fetching {crypto_id}: {exc}")
        return None


# ── Row builder ───────────────────────────────────────────────────────────────

def build_row(asset: dict, data: dict | None) -> list[str] | None:
    """Convert fetched *data* into a 6-column sheet row.

    Returns ``None`` when *data* is ``None`` so the caller knows to keep the
    existing sheet row intact.
    """
    if data is None:
        return None
    return [
        asset["name"],
        _fmt(data["price"]),
        _fmt(data["52w_low"]),
        _fmt(data["52w_high"]),
        _fmt(data["5y_low"]),
        _fmt(data["5y_high"]),
    ]


# ── Main sync ─────────────────────────────────────────────────────────────────

def sync_data() -> None:
    """Fetch all asset data and write it to Google Sheets."""
    print("--- Starting Stock Data Sync ---")

    # Fetch data for every asset
    new_rows: list[list[str] | None] = []
    for asset in ASSETS:
        if asset["type"] == "yahoo":
            data = get_yahoo_data(asset["ticker"])
        elif asset["type"] == "crypto":
            data = get_crypto_data(asset["id"])
        else:
            data = None
        new_rows.append(build_row(asset, data))

    # Connect to Google Sheets
    print("\nConnecting to Google Sheets...")
    creds_json = json.loads(os.environ["GCP_SA_KEY"])
    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    client = gspread.service_account_from_dict(creds_json, scopes=SCOPES)
    sheet = client.open_by_key(spreadsheet_id).sheet1

    # Read current values so failed fetches preserve the last-good data
    current_rows: list[list] = sheet.get(SHEET_DATA_RANGE)
    while len(current_rows) < len(ASSETS):
        current_rows.append([""] * 6)

    for i, row in enumerate(new_rows):
        if row is not None:
            current_rows[i] = row
        else:
            print(f"  Keeping existing sheet data for: {ASSETS[i]['name']}")

    print(f"  Updating data in range {SHEET_DATA_RANGE}...")
    sheet.update(SHEET_DATA_RANGE, current_rows)

    # Write both timestamps in a single API call
    india_tz  = pytz.timezone("Asia/Kolkata")
    uganda_tz = pytz.timezone("Africa/Kampala")
    now_utc   = datetime.now(pytz.utc)
    india_time  = now_utc.astimezone(india_tz).strftime("%d-%m-%Y %H:%M:%S")
    uganda_time = now_utc.astimezone(uganda_tz).strftime("%d-%m-%Y %H:%M:%S")

    sheet.batch_update([
        {"range": SHEET_INDIA_TIME_CELL,  "values": [[india_time]]},
        {"range": SHEET_UGANDA_TIME_CELL, "values": [[uganda_time]]},
    ])

    print("--- Sync Complete! ---")


if __name__ == "__main__":
    sync_data()
