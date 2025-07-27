import requests
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz
import os

# --- This is our main logic, now inside a function ---
def sync_data():
    # Configuration
    ASSETS = [
        {'name': 'NASDAQ', 'ticker': '^IXIC', 'type': 'yahoo'},
        {'name': 'S&P 500', 'ticker': '^GSPC', 'type': 'yahoo'},
        {'name': 'DOW JONES', 'ticker': '^DJI', 'type': 'yahoo'},
        {'name': 'SENSEX', 'ticker': '^BSESN', 'type': 'yahoo'},
        {'name': 'NIFTY 50', 'ticker': '^NSEI', 'type': 'yahoo'},
        {'name': 'NIKKEI 225', 'ticker': '^N225', 'type': 'yahoo'},
        {'name': 'GOLD 24 CARAT', 'ticker': 'GC=F', 'type': 'yahoo'},
        {'name': 'SILVER', 'ticker': 'SI=F', 'type': 'yahoo'},
        {'name': 'OIL (BRENT)', 'ticker': 'BZ=F', 'type': 'yahoo'},
        {'name': 'BITCOin', 'id': 'bitcoin', 'type': 'crypto'},
        {'name': 'ETHEREUM', 'id': 'ethereum', 'type': 'crypto'},
    ]
    SPREADSHEET_ID = "1HPEkoD_CGxQeNqLt_Xn98TasvV2_lrxGn7yYipRx9No"

    # Data Fetching Functions
    def get_yahoo_data(ticker):
        print(f"Fetching Yahoo: {ticker}")
        try:
            quote_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            five_year_url = f"{quote_url}?range=5y&interval=1d"
            headers = {'User-Agent': 'Mozilla/5.0'}
            quote_res = requests.get(quote_url, headers=headers).json()
            meta = quote_res['chart']['result'][0]['meta']
            five_year_res = requests.get(five_year_url, headers=headers).json()
            highs = five_year_res['chart']['result'][0]['indicators']['quote'][0]['high']
            lows = five_year_res['chart']['result'][0]['indicators']['quote'][0]['low']
            valid_highs = [h for h in highs if h is not None]
            valid_lows = [l for l in lows if l is not None]
            return {
                'price': meta.get('regularMarketPrice', 'ERROR'),
                '52w_low': meta.get('fiftyTwoWeekLow', 'ERROR'),
                '52w_high': meta.get('fiftyTwoWeekHigh', 'ERROR'),
                '5y_low': min(valid_lows) if valid_lows else 'ERROR',
                '5y_high': max(valid_highs) if valid_highs else 'ERROR'
            }
        except Exception as e:
            print(f"  Error fetching {ticker}: {e}")
            return None

    def get_crypto_data(crypto_id):
        print(f"Fetching Crypto: {crypto_id}")
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{crypto_id}/market_chart?vs_currency=usd&days=365&interval=daily"
            res = requests.get(url)
            res.raise_for_status()
            data = res.json()
            prices = [p[1] for p in data['prices']]
            one_year_low = min(prices)
            one_year_high = max(prices)
            return {
                'price': prices[-1],
                '52w_low': one_year_low,
                '52w_high': one_year_high,
                '5y_low': 'N/A',
                '5y_high': 'N/A'
            }
        except Exception as e:
            print(f"  Error fetching {crypto_id}: {e}")
            return None

    # Main Script Logic
    print("--- Starting Stock Data Sync ---")
    all_asset_data = []
    for asset in ASSETS:
        data = None
        if asset['type'] == 'yahoo':
            data = get_yahoo_data(asset['ticker'])
        elif asset['type'] == 'crypto':
            data = get_crypto_data(asset['id'])
            
        if data:
            all_asset_data.append([
                asset['name'],
                f"{data['price']:.2f}" if isinstance(data['price'], (int, float)) else data['price'],
                f"{data['52w_low']:.2f}" if isinstance(data['52w_low'], (int, float)) else data['52w_low'],
                f"{data['52w_high']:.2f}" if isinstance(data['52w_high'], (int, float)) else data['52w_high'],
                f"{data['5y_low']:.2f}" if isinstance(data['5y_low'], (int, float)) else data['5y_low'],
                f"{data['5y_high']:.2f}" if isinstance(data['5y_high'], (int, float)) else data['5y_high']
            ])
        else:
            all_asset_data.append([asset['name'], 'ERROR', 'ERROR', 'ERROR', 'ERROR', 'ERROR'])

    # Update Google Sheet
    print("\nConnecting to Google Sheets...")
    try:
        # This part is now modified to read from a GitHub Secret
        creds_json_string = os.environ['GCP_SA_KEY']
        creds_json = json.loads(creds_json_string)

        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID).sheet1
        
        print("Updating main data in range A2:F12...")
        sheet.update(range_name='A2:F12', values=all_asset_data)
        
        print("Updating timestamps...")
        uganda_tz = pytz.timezone("Africa/Kampala")
        india_tz = pytz.timezone("Asia/Kolkata")
        now_utc = datetime.now(pytz.utc)
        uganda_time = now_utc.astimezone(uganda_tz).strftime('%d-%m-%Y %H:%M:%S')
        india_time = now_utc.astimezone(india_tz).strftime('%d-%m-%Y %H:%M:%S')
        sheet.update_acell("A14", india_time)
        sheet.update_acell("A15", uganda_time)
        
        print("--- Sync Complete! ---")
    except KeyError:
        print("Error: The GCP_SA_KEY secret is not set. Please add it to your repository's secrets.")
    except Exception as e:
        print(f"An error occurred while updating the sheet: {e}")

# --- Main execution ---
if __name__ == "__main__":
    sync_data()
