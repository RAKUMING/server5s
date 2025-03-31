const express = require("express");
const sqlite3 = require("sqlite3").verbose();
const axios = require("axios");

const app = express();
const db = new sqlite3.Database("btc_data.db");

// Crear tabla si no existe
db.run(`
    CREATE TABLE IF NOT EXISTS candles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL
    )
`);

// Obtener precio BTC/USDT desde CoinGecko
async function getBtcPrice() {
    try {
        // Usando la API de CoinGecko para obtener el precio de Bitcoin en USD
        const response = await axios.get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd");
        return response.data.bitcoin.usd;
    } catch (error) {
        console.error("❌ Error obteniendo precio:", error.message);
        return null;
    }
}

// Generar velas de 5 segundos
async function generateCandle() {
    const openPrice = await getBtcPrice();
    if (!openPrice) return;

    let highPrice = openPrice;
    let lowPrice = openPrice;
    let closePrice = openPrice;
    const startTime = new Date().toISOString();

    for (let i = 0; i < 5; i++) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        const price = await getBtcPrice();
        if (price) {
            highPrice = Math.max(highPrice, price);
            lowPrice = Math.min(lowPrice, price);
            closePrice = price;
        }
    }

    db.run("INSERT INTO candles (timestamp, open, high, low, close) VALUES (?, ?, ?, ?, ?)",
        [startTime, openPrice, highPrice, lowPrice, closePrice]);

    console.log(`📊 Vela guardada: ${startTime} - Open: ${openPrice}, Close: ${closePrice}`);
}

// Iniciar la captura de datos
setInterval(generateCandle, 5000);

// Ruta para descargar historial de velas
app.get("/historical", (req, res) => {
    db.all("SELECT * FROM candles ORDER BY id DESC LIMIT 100", (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(rows);
    });
});

// Iniciar el servidor
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`🚀 Servidor corriendo en http://localhost:${PORT}`));
