const express = require("express");
const sqlite3 = require("sqlite3").verbose();
const axios = require("axios");
const WebSocket = require("ws");
const http = require("http");

const app = express();
const server = http.createServer(app);

// Configuración
const DATA_SOURCES = {
    BINANCE: "binance",
    COINBASE: "coinbase",
    KRAKEN: "kraken",
    COINGECKO: "coingecko"
};

// Fuente de datos actual (puede cambiar automáticamente si falla)
let currentSource = DATA_SOURCES.COINGECKO;
let websocket = null;

// Base de datos SQLite
const db = new sqlite3.Database("btc_data.db", (err) => {
    if (err) {
        console.error("❌ Error conectando a la base de datos:", err.message);
    } else {
        console.log("✅ Base de datos conectada.");
    }
});

// Crear tabla si no existe
db.run(`
    CREATE TABLE IF NOT EXISTS candles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        source TEXT
    )
`);

// Variables para manejar las velas de 5 segundos
let currentCandle = null;
let candleStartTime = null;
const CANDLE_PERIOD = 5000; // 5 segundos
let failedAttempts = 0;
const MAX_FAILED_ATTEMPTS = 5;

// Funciones para obtener precios de diferentes fuentes
async function getBtcPriceFromCoinGecko() {
    try {
        const response = await axios.get("https://api.coingecko.com/api/v3/simple/price", {
            params: {
                ids: "bitcoin",
                vs_currencies: "usd"
            },
            timeout: 5000
        });
        console.log(`✅ Precio obtenido de CoinGecko: $${response.data.bitcoin.usd}`);
        failedAttempts = 0;
        return response.data.bitcoin.usd;
    } catch (error) {
        console.error(`❌ Error obteniendo precio de CoinGecko: ${error.message}`);
        failedAttempts++;
        return null;
    }
}

async function getBtcPriceFromKraken() {
    try {
        const response = await axios.get("https://api.kraken.com/0/public/Ticker?pair=XBTUSD", {
            timeout: 5000
        });
        const price = parseFloat(response.data.result.XXBTZUSD.c[0]);
        console.log(`✅ Precio obtenido de Kraken: $${price}`);
        failedAttempts = 0;
        return price;
    } catch (error) {
        console.error(`❌ Error obteniendo precio de Kraken: ${error.message}`);
        failedAttempts++;
        return null;
    }
}

async function getBtcPriceFromCoinbase() {
    try {
        const response = await axios.get("https://api.coinbase.com/v2/prices/BTC-USD/spot", {
            timeout: 5000
        });
        const price = parseFloat(response.data.data.amount);
        console.log(`✅ Precio obtenido de Coinbase: $${price}`);
        failedAttempts = 0;
        return price;
    } catch (error) {
        console.error(`❌ Error obteniendo precio de Coinbase: ${error.message}`);
        failedAttempts++;
        return null;
    }
}

// Obtener precio de BTC según la fuente actual
async function getBtcPrice() {
    switch (currentSource) {
        case DATA_SOURCES.BINANCE:
            // Si ya estamos usando WebSocket, no necesitamos hacer nada aquí
            return null;
        case DATA_SOURCES.COINBASE:
            return await getBtcPriceFromCoinbase();
        case DATA_SOURCES.KRAKEN:
            return await getBtcPriceFromKraken();
        case DATA_SOURCES.COINGECKO:
            return await getBtcPriceFromCoinGecko();
        default:
            return await getBtcPriceFromCoinGecko();
    }
}

// Cambiar a la siguiente fuente si la actual falla
function rotateDataSource() {
    const sources = Object.values(DATA_SOURCES);
    const currentIndex = sources.indexOf(currentSource);
    const nextIndex = (currentIndex + 1) % sources.length;
    const newSource = sources[nextIndex];
    
    console.log(`🔄 Cambiando fuente de datos: ${currentSource} -> ${newSource}`);
    currentSource = newSource;
    
    // Si cambiamos a Binance, intentamos usar WebSocket
    if (currentSource === DATA_SOURCES.BINANCE) {
        tryConnectBinanceWebSocket();
    } else if (websocket) {
        // Si estamos cambiando desde Binance, cerramos el WebSocket
        websocket.close();
        websocket = null;
    }
    
    failedAttempts = 0;
}

// Intentar conectar al WebSocket de Binance
function tryConnectBinanceWebSocket() {
    console.log("🔌 Intentando conectar a WebSocket de Binance...");
    
    try {
        websocket = new WebSocket("wss://stream.binance.com:9443/ws/btcusdt@kline_1s");
        
        websocket.on("open", () => {
            console.log("✅ Conexión WebSocket establecida con Binance");
        });
        
        websocket.on("message", (data) => {
            try {
                const message = JSON.parse(data);
                const kline = message.k;
                processTickData(parseFloat(kline.c), DATA_SOURCES.BINANCE);
            } catch (error) {
                console.error("❌ Error procesando mensaje WebSocket:", error);
            }
        });
        
        websocket.on("error", (error) => {
            console.error(`❌ Error en WebSocket: ${error.message}`);
            websocket = null;
            rotateDataSource();
        });
        
        websocket.on("close", () => {
            console.log("⚠️ Conexión WebSocket cerrada");
            websocket = null;
            rotateDataSource();
        });
        
        return true;
    } catch (error) {
        console.error(`❌ Error conectando a WebSocket: ${error.message}`);
        websocket = null;
        rotateDataSource();
        return false;
    }
}

// Procesar cada tick de precio para formar velas de 5 segundos
function processTickData(price, source) {
    if (!price) return;
    
    const now = Date.now();
    
    // Si no hay vela actual o ha pasado el período, iniciamos una nueva
    if (!currentCandle || !candleStartTime || (now - candleStartTime) >= CANDLE_PERIOD) {
        // Si hay vela anterior, la guardamos
        if (currentCandle) {
            saveCandle(currentCandle);
        }
        
        // Iniciar nueva vela
        candleStartTime = now;
        currentCandle = {
            timestamp: new Date(candleStartTime).toISOString(),
            open: price,
            high: price,
            low: price,
            close: price,
            volume: 1,
            source: source || currentSource
        };
        
        console.log(`🕐 Nueva vela iniciada a las ${currentCandle.timestamp} [Open: ${price.toFixed(2)}] (${currentCandle.source})`);
    } else {
        // Actualizar vela actual
        currentCandle.high = Math.max(currentCandle.high, price);
        currentCandle.low = Math.min(currentCandle.low, price);
        currentCandle.close = price;
        currentCandle.volume += 1;
    }
}

// Guardar vela en la base de datos
function saveCandle(candle) {
    if (!candle) return;
    
    const priceChange = candle.close - candle.open;
    const priceChangePercent = (priceChange / candle.open) * 100;
    const direction = priceChange >= 0 ? "🟢" : "🔴";
    
    db.run(
        "INSERT INTO candles (timestamp, open, high, low, close, volume, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [candle.timestamp, candle.open, candle.high, candle.low, candle.close, candle.volume, candle.source],
        (err) => {
            if (err) {
                console.error("❌ Error guardando vela:", err);
            } else {
                console.log(
                    `📊 Vela guardada: ${direction} ${candle.timestamp} - ` +
                    `O: ${candle.open.toFixed(2)} H: ${candle.high.toFixed(2)} ` +
                    `L: ${candle.low.toFixed(2)} C: ${candle.close.toFixed(2)} ` +
                    `[${priceChangePercent.toFixed(4)}%] Ticks: ${candle.volume} (${candle.source})`
                );
            }
        }
    );
}

// Generar velas mediante polling si no estamos usando WebSocket
async function generateCandle() {
    // Si estamos usando WebSocket, no necesitamos polling
    if (currentSource === DATA_SOURCES.BINANCE && websocket) {
        return;
    }
    
    const price = await getBtcPrice();
    
    // Si fallamos demasiadas veces, rotar la fuente
    if (!price) {
        if (failedAttempts >= MAX_FAILED_ATTEMPTS) {
            rotateDataSource();
        }
        return;
    }
    
    processTickData(price);
}

// Rutas HTTP para API
app.use(express.json());

// Ruta para descargar velas con límite personalizable
app.get("/api/candles", (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    
    db.all("SELECT * FROM candles ORDER BY timestamp DESC LIMIT ?", 
        [limit], 
        (err, rows) => {
            if (err) {
                console.error("❌ Error obteniendo velas:", err.message);
                return res.status(500).json({ error: err.message });
            }
            console.log(`📡 Enviadas ${rows.length} velas al cliente [limit=${limit}]`);
            res.json(rows);
        }
    );
});

// Ruta para descargar un número específico de velas
app.get("/api/limit/:count", (req, res) => {
    const limit = parseInt(req.params.count) || 10;
    
    db.all("SELECT * FROM candles ORDER BY timestamp DESC LIMIT ?", 
        [limit], 
        (err, rows) => {
            if (err) {
                console.error("❌ Error obteniendo velas:", err.message);
                return res.status(500).json({ error: err.message });
            }
            console.log(`📡 Enviadas ${rows.length} velas al cliente [limit=${limit}]`);
            res.json({
                candles: rows,
                timestamp: new Date().toISOString(),
                count: rows.length
            });
        }
    );
});

// Ruta para obtener el precio actual
app.get("/api/price", async (req, res) => {
    let price;
    
    if (currentCandle) {
        price = currentCandle.close;
    } else {
        price = await getBtcPrice();
    }
    
    if (!price) {
        return res.status(503).json({ 
            error: "No se pudo obtener el precio", 
            source: currentSource,
            timestamp: new Date().toISOString()
        });
    }
    
    res.json({
        price,
        source: currentSource,
        timestamp: new Date().toISOString()
    });
});

// Ruta para cambiar la fuente de datos manualmente
app.post("/api/source", (req, res) => {
    const { source } = req.body;
    
    if (!source || !Object.values(DATA_SOURCES).includes(source)) {
        return res.status(400).json({ 
            error: "Fuente de datos inválida", 
            validSources: Object.values(DATA_SOURCES)
        });
    }
    
    console.log(`🔄 Cambiando fuente de datos manualmente: ${currentSource} -> ${source}`);
    currentSource = source;
    
    // Si cambiamos a Binance, intentamos usar WebSocket
    if (currentSource === DATA_SOURCES.BINANCE) {
        tryConnectBinanceWebSocket();
    } else if (websocket) {
        // Si estamos cambiando desde Binance, cerramos el WebSocket
        websocket.close();
        websocket = null;
    }
    
    res.json({ success: true, source });
});

// Ruta para obtener estadísticas
app.get("/api/stats", (req, res) => {
    db.get("SELECT COUNT(*) as total FROM candles", (err, result) => {
        if (err) {
            console.error("❌ Error obteniendo estadísticas:", err.message);
            return res.status(500).json({ error: err.message });
        }
        
        db.all("SELECT source, COUNT(*) as count FROM candles GROUP BY source", (err, sources) => {
            if (err) {
                console.error("❌ Error obteniendo estadísticas de fuentes:", err.message);
                return res.status(500).json({ error: err.message });
            }
            
            res.json({
                totalCandles: result.total,
                sources: sources,
                currentSource: currentSource,
                usingWebSocket: currentSource === DATA_SOURCES.BINANCE && websocket !== null,
                currentPrice: currentCandle ? currentCandle.close : null,
                serverTime: new Date().toISOString(),
                currentCandleTime: currentCandle ? currentCandle.timestamp : null
            });
        });
    });
});

// Método para detener la aplicación correctamente
function shutdown() {
    console.log("🛑 Cerrando servidor...");
    
    // Guardar la vela actual si existe
    if (currentCandle) {
        saveCandle(currentCandle);
    }
    
    // Cerrar WebSocket si existe
    if (websocket) {
        websocket.close();
    }
    
    // Cerrar la base de datos
    db.close((err) => {
        if (err) {
            console.error("❌ Error al cerrar la base de datos:", err.message);
        } else {
            console.log("✅ Base de datos cerrada correctamente");
        }
        
        process.exit(0);
    });
}

// Manejar señales de terminación
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// Iniciar el servidor
const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
    console.log(`
    ████████╗██████╗  █████╗ ██████╗ ███████╗██████╗     ██████╗ ████████╗ ██████╗
    ╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗    ██╔══██╗╚══██╔══╝██╔════╝
       ██║   ██████╔╝███████║██║  ██║█████╗  ██████╔╝    ██████╔╝   ██║   ██║     
       ██║   ██╔══██╗██╔══██║██║  ██║██╔══╝  ██╔══██╗    ██╔══██╗   ██║   ██║     
       ██║   ██║  ██║██║  ██║██████╔╝███████╗██║  ██║    ██████╔╝   ██║   ╚██████╗
       ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚══════╝╚═╝  ╚═╝    ╚═════╝    ╚═╝    ╚═════╝
    `);
    console.log(`🚀 Servidor corriendo en http://localhost:${PORT}`);
    console.log(`📈 Fuente de datos inicial: ${currentSource}`);
    console.log(`🕐 Generando velas de 5 segundos`);
    console.log(`📡 API endpoints disponibles:`);
    console.log(`   - GET /api/candles?limit=100`);
    console.log(`   - GET /api/limit/10`);
    console.log(`   - GET /api/price`);
    console.log(`   - GET /api/stats`);
    console.log(`   - POST /api/source (body: {"source": "coingecko"})`);
    
    // Intentar conectar al WebSocket de Binance primero
    if (currentSource === DATA_SOURCES.BINANCE) {
        const connected = tryConnectBinanceWebSocket();
        if (!connected) {
            // Si falla, empezar con polling de CoinGecko
            currentSource = DATA_SOURCES.COINGECKO;
            console.log(`⚠️ No se pudo conectar a Binance WebSocket, usando ${currentSource}`);
        }
    }
    
    // Iniciar el polling para fuentes no-WebSocket
    setInterval(generateCandle, 1000);
});
