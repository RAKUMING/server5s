import sqlite3
import time
import requests
from flask import Flask, jsonify

app = Flask(__name__)

# Conectar a la base de datos
conn = sqlite3.connect("btc_data.db", check_same_thread=False)
cursor = conn.cursor()

# Crear tabla si no existe
cursor.execute("""
    CREATE TABLE IF NOT EXISTS candles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL
    )
""")
conn.commit()

# Obtener precio BTC/USDT desde Binance
def get_btc_price():
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    try:
        response = requests.get(url).json()
        return float(response["price"])
    except Exception as e:
        print("Error obteniendo precio:", e)
        return None

# Generar velas de 5 segundos
def generate_candle():
    open_price = get_btc_price()
    if open_price is None:
        return

    high_price = open_price
    low_price = open_price
    close_price = open_price
    start_time = time.strftime("%Y-%m-%d %H:%M:%S")

    for _ in range(5):
        price = get_btc_price()
        if price:
            high_price = max(high_price, price)
            low_price = min(low_price, price)
            close_price = price
        time.sleep(1)

    # Guardar en la base de datos
    cursor.execute("INSERT INTO candles (timestamp, open, high, low, close) VALUES (?, ?, ?, ?, ?)",
                   (start_time, open_price, high_price, low_price, close_price))
    conn.commit()

# Ruta para descargar historial de velas
@app.route("/historical", methods=["GET"])
def get_historical_data():
    cursor.execute("SELECT * FROM candles ORDER BY id DESC LIMIT 100")
    data = cursor.fetchall()
    return jsonify([{"timestamp": row[1], "open": row[2], "high": row[3], "low": row[4], "close": row[5]} for row in data])

# Iniciar la captura de datos en segundo plano
import threading
def run_data_collector():
    while True:
        generate_candle()

threading.Thread(target=run_data_collector, daemon=True).start()

# Iniciar el servidor
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
