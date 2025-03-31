const express = require("express");
const sqlite3 = require("sqlite3").verbose();
const WebSocket = require("ws");
const http = require("http");

const app = express();
const server = http.createServer(app);

// Base de datos SQLite
const db = new sqlite3.Database("btc_data.db");

// Crear tabla si no existe
db.run(`
    CREATE TABLE IF NOT EXISTS candles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL
    )
`);

// Variables para manejar las velas de 5 segundos
let currentCandle = null;
let candleStartTime = null;
const CANDLE_PERIOD = 5000; // 5 segundos

// Conectar al WebSocket de Binance
function connectBinanceWebSocket() {
    console.log("🔌 Conectando a WebSocket de Binance...");
    
    // Usamos el stream de kline_1s (1 segundo) para tener la mayor granularidad posible
    const binanceWs = new WebSocket("wss://stream.binance.com:9443/ws/btcusdt@kline_1s");
    
    binanceWs.on("open", () => {
        console.log("✅ Conexión WebSocket establecida con Binance");
    });
    
    binanceWs.on("message", (data) => {
        try {
            const message = JSON.parse(data);
            const kline = message.k;
            
            // Procesamos cada tick para formar nuestras velas personalizadas
            processTickData(parseFloat(kline.c)); // Usamos el precio de cierre
        } catch (error) {
            console.error("❌ Error procesando mensaje WebSocket:", error);
        }
    });
    
    binanceWs.on("error", (error) => {
        console.error("❌ Error en WebSocket:", error);
        setTimeout(connectBinanceWebSocket, 5000); // Reconectar después de 5 segundos
    });
    
    binanceWs.on("close", () => {
        console.log("⚠️ Conexión WebSocket cerrada. Intentando reconectar...");
        setTimeout(connectBinanceWebSocket, 5000); // Reconectar después de 5 segundos
    });
}

// Procesar cada tick de precio para formar velas de 5 segundos
function processTickData(price) {
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
            volume: 0
        };
        
        console.log(`🕐 Nueva vela iniciada a las ${currentCandle.timestamp} [Open: ${price.toFixed(2)}]`);
    } else {
        // Actualizar vela actual
        currentCandle.high = Math.max(currentCandle.high, price);
        currentCandle.low = Math.min(currentCandle.low, price);
        currentCandle.close = price;
        currentCandle.volume += 1; // Incrementamos como contador de ticks
    }
}

// Guardar vela en la base de datos
function saveCandle(candle) {
    const priceChange = candle.close - candle.open;
    const priceChangePercent = (priceChange / candle.open) * 100;
    const direction = priceChange >= 0 ? "🟢" : "🔴";
    
    db.run(
        "INSERT INTO candles (timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?)",
        [candle.timestamp, candle.open, candle.high, candle.low, candle.close, candle.volume],
        (err) => {
            if (err) {
                console.error("❌ Error guardando vela:", err);
            } else {
                console.log(
                    `📊 Vela guardada: ${direction} ${candle.timestamp} - ` +
                    `O: ${candle.open.toFixed(2)} H: ${candle.high.toFixed(2)} ` +
                    `L: ${candle.low.toFixed(2)} C: ${candle.close.toFixed(2)} ` +
                    `[${priceChangePercent.toFixed(4)}%] Ticks: ${candle.volume}`
                );
            }
        }
    );
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

// Ruta para obtener estadísticas
app.get("/api/stats", (req, res) => {
    db.get("SELECT COUNT(*) as total FROM candles", (err, result) => {
        if (err) {
            console.error("❌ Error obteniendo estadísticas:", err.message);
            return res.status(500).json({ error: err.message });
        }
        
        console.log(`📊 Estadísticas enviadas: ${result.total} velas en total`);
        res.json({
            totalCandles: result.total,
            currentPrice: currentCandle ? currentCandle.close : null,
            serverTime: new Date().toISOString(),
            currentCandleTime: currentCandle ? currentCandle.timestamp : null
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
    console.log(`📈 Obteniendo datos de BTC/USDT desde Binance WebSocket`);
    console.log(`🕐 Generando velas de 5 segundos`);
    console.log(`📡 API endpoints disponibles:`);
    console.log(`   - GET /api/candles?limit=100`);
    console.log(`   - GET /api/limit/10`);
    console.log(`   - GET /api/stats`);
    
    // Conectar al WebSocket de Binance
    connectBinanceWebSocket();
});
