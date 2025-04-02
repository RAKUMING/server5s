from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import requests
import datetime
import pandas as pd
import os

app = FastAPI()

# Directorio donde se guardarán los CSVs
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Servir archivos estáticos (CSV) sin CORS
app.mount("/files", StaticFiles(directory=OUTPUT_DIR), name="files")

API_KEY = "84bd6d2d-4045-4b53-8b61-151c618d4311"

def get_liquidation_history(symbols, interval, from_timestamp, to_timestamp):
    """ Obtiene el historial de liquidación desde la API de Coinalyze. """
    base_url = "https://api.coinalyze.net/v1/liquidation-history"
    symbols_str = ",".join(symbols)

    params = {
        "api_key": API_KEY,
        "symbols": symbols_str,
        "interval": interval,
        "from": from_timestamp,
        "to": to_timestamp,
        "convert_to_usd": "false"
    }

    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        return None

    return response.json()

def parse_liquidation_data(data):
    """ Convierte los datos en un DataFrame. """
    all_data = []
    for symbol_data in data:
        symbol = symbol_data["symbol"]
        history = symbol_data["history"]
        
        for entry in history:
            entry["symbol"] = symbol
            all_data.append(entry)

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    return df

@app.get("/fetch")
def fetch_data():
    """ Obtiene y guarda los datos de liquidaciones. """
    symbols = ["BTCUSDT_PERP.A"]
    interval = "1hour"

    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=7)
    from_timestamp = int(start_date.timestamp())
    to_timestamp = int(end_date.timestamp())

    liquidation_data = get_liquidation_history(symbols, interval, from_timestamp, to_timestamp)

    if liquidation_data:
        df = parse_liquidation_data(liquidation_data)
        if not df.empty:
            filename = f"btcusdt_liquidations_{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}.csv"
            filepath = os.path.join(OUTPUT_DIR, filename)
            df.to_csv(filepath, index=False)
            return {"message": "Data saved", "download_url": f"/files/{filename}"}
    
    return {"message": "No liquidation data found"}
