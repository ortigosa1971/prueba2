// server.js
// Proxy + static server for Weather Underground (Weather.com) PWS intraday history (Railway-friendly)
// Node >=18 (fetch nativo)

import express from "express";
import dotenv from "dotenv";
import pg from "pg";
import cron from "node-cron";

dotenv.config();

// ========= Helpers de variables (compatibles con nombres "en español") =========
function envFirst(keys) {
  for (const k of keys) {
    const v = process.env[k];
    if (v !== undefined && v !== null && String(v).trim() !== "") return String(v).trim();
  }
  return null;
}
function envBool(keys, defaultValue=false) {
  const v = envFirst(keys);
  if (v === null) return defaultValue;
  const s = String(v).toLowerCase();
  if (["1","true","yes","si","sí","on"].includes(s)) return true;
  if (["0","false","no","off"].includes(s)) return false;
  return defaultValue;
}


const DATABASE_URL = envFirst(["DATABASE_URL","URL_DE_LA_BASE_DE_DATOS","URL DE LA BASE DE DATOS"]);
// Pool solo si hay DB configurada (no rompe nada si no está)
const pool = DATABASE_URL
  ? new pg.Pool({
      connectionString: DATABASE_URL,
      ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false,
    })
  : null;

async function initDb() {
  if (!pool) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS wu_daily (
      fecha date PRIMARY KEY,
      station_id text NOT NULL,
      lluvia_mm numeric(10,2),
      tmin_c numeric(10,2),
      tmax_c numeric(10,2),
      humedad_media numeric(10,2),
      viento_max_mps numeric(10,2),
      creado_en timestamptz DEFAULT now()
    );
  `);

  // Guarda "todos los campos" (payload completo) por día
  await pool.query(`
    CREATE TABLE IF NOT EXISTS wu_daily_payload (
      fecha date PRIMARY KEY,
      station_id text NOT NULL,
      payload jsonb NOT NULL,
      creado_en timestamptz DEFAULT now()
    );
  `);
}


function yyyymmddToDate(yyyymmdd) {
  const s = String(yyyymmdd);
  if (!/^\d{8}$/.test(s)) return null;
  return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`;
}

function summarizeWuPayload(payload) {
  // Weather.com suele devolver { observations: [...] }
  const obs = Array.isArray(payload?.observations) ? payload.observations : [];
  if (!obs.length) return null;

  const nums = (arr) => arr.filter((v) => Number.isFinite(v));
  const get = (o, path) => path.split(".").reduce((a,k)=> (a && a[k] !== undefined ? a[k] : undefined), o);

  const precipTotals = nums(obs.map(o => Number(get(o,"metric.precipTotal"))));
  const temps = nums(obs.map(o => Number(get(o,"metric.temp"))));
  const hums = nums(obs.map(o => Number(o.humidity)));
  const winds = nums(obs.map(o => Number(get(o,"metric.windSpeed"))));

  const lluvia_mm = precipTotals.length ? Math.max(...precipTotals) : null;
  const tmin_c = temps.length ? Math.min(...temps) : null;
  const tmax_c = temps.length ? Math.max(...temps) : null;
  const humedad_media = hums.length ? (hums.reduce((a,b)=>a+b,0)/hums.length) : null;
  const viento_max_mps = winds.length ? Math.max(...winds) : null;

  return { lluvia_mm, tmin_c, tmax_c, humedad_media, viento_max_mps, n_obs: obs.length };
}


const app = express();
const PORT = process.env.PORT || 3000;
const WU_API_KEY = envFirst(["WU_API_KEY","CLAVE_API_WU","CLAVE API WU","API_KEY_WU"]);

// (Opcional) UA para evitar bloqueos tontos en algunos entornos
const UA =
  process.env.USER_AGENT ||
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari";

/* ============ Anti-cache para /api ============ */
app.use((req, res, next) => {
  if (req.path && req.path.startsWith("/api")) {
    res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
    res.set("Pragma", "no-cache");
    res.set("Expires", "0");
    res.set("Surrogate-Control", "no-store");
  }
  next();
});

/* ============ Health ============ */
app.get("/health", (_req, res) => res.json({ ok: true }));

/* ============ Frontend estático ============ */
app.use(express.static("public"));

/* ============ API: intradía (la que usa tu public/app.js) ============ */
// Mantiene el path original del front: /api/wu/history?stationId=XXXX&date=YYYYMMDD
// - Si date es HOY (Madrid) -> /v2/pws/observations/all/1day  (NO lleva date)
// - Si date es pasado      -> /v2/pws/history/all?date=YYYYMMDD
app.get("/api/wu/history", async (req, res) => {
  try {
    const { stationId, date } = req.query;

    if (!WU_API_KEY) {
      return res
        .status(500)
        .json({ error: "Falta WU_API_KEY en variables de entorno" });
    }
    if (!stationId || !date) {
      return res
        .status(400)
        .json({ error: "Parámetros requeridos: stationId y date (YYYYMMDD)" });
    }

    // "hoy" en Madrid (Railway/Node suele estar en UTC)
    const todayMadrid = new Date()
      .toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" })
      .replaceAll("-", ""); // YYYYMMDD
    const isToday = String(date) === todayMadrid;

    const upstreamBase = isToday
      ? "https://api.weather.com/v2/pws/observations/all/1day"
      : "https://api.weather.com/v2/pws/history/all";

    const api = new URL(upstreamBase);
    api.searchParams.set("stationId", String(stationId));
    api.searchParams.set("format", "json");
    api.searchParams.set("units", "m");
    api.searchParams.set("apiKey", WU_API_KEY);
    // OJO: 1day NO lleva date
    if (!isToday) api.searchParams.set("date", String(date));

    const upstream = await fetch(api, {
      headers: {
        accept: "application/json",
        "user-agent": UA,
      },
    });

    const text = await upstream.text();
    const ct = (upstream.headers.get("content-type") || "").toLowerCase();

    // Pass-through status
    res.status(upstream.status);

    if (!upstream.ok) {
      return res.json({
        error: "weather.com denied",
        status: upstream.status,
        contentType: ct,
        bodyPreview: text.slice(0, 300),
      });
    }

    // Respuesta normal: intenta JSON, si no, lo envía como texto JSON
    try {
      return res.json(JSON.parse(text));
    } catch {
      return res.type("application/json").send(text);
    }
  } catch (err) {
    console.error(err);
    res
      .status(500)
      .json({ error: "Error al consultar Weather.com", details: String(err) });
  }
});



/* ============ API: guardar resumen diario en Postgres (no afecta al proxy) ============ */
/**
 * GET /api/ingest/daily?stationId=IALFAR30&date=YYYYMMDD
 * - Consulta Weather.com (igual que /api/wu/history)
 * - Calcula resumen del día
 * - Inserta/actualiza en Postgres (tabla wu_daily)
 */
app.get("/api/ingest/daily", async (req, res) => {
  try {
    const { stationId, date } = req.query;

    if (!pool) {
      return res.status(500).json({
        error: "Falta DATABASE_URL en variables de entorno (servicio web)",
      });
    }
    if (!WU_API_KEY) {
      return res
        .status(500)
        .json({ error: "Falta WU_API_KEY en variables de entorno" });
    }
    if (!stationId || !date) {
      return res
        .status(400)
        .json({ error: "Parámetros requeridos: stationId y date (YYYYMMDD)" });
    }

    const todayMadrid = new Date()
      .toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" })
      .replaceAll("-", "");
    const isToday = String(date) === todayMadrid;

    const upstreamBase = isToday
      ? "https://api.weather.com/v2/pws/observations/all/1day"
      : "https://api.weather.com/v2/pws/history/all";

    const api = new URL(upstreamBase);
    api.searchParams.set("stationId", String(stationId));
    api.searchParams.set("format", "json");
    api.searchParams.set("units", "m");
    api.searchParams.set("apiKey", WU_API_KEY);
    if (!isToday) api.searchParams.set("date", String(date));

    const upstream = await fetch(api, {
      headers: { accept: "application/json", "user-agent": UA },
    });

    const txt = await upstream.text();
    if (!upstream.ok) {
      return res.status(upstream.status).json({
        error: "weather.com denied",
        status: upstream.status,
        bodyPreview: txt.slice(0, 300),
      });
    }

    let payload;
    try {
      payload = JSON.parse(txt);
    } catch {
      return res.status(500).json({ error: "Respuesta no-JSON de Weather.com" });
    }

    const summary = summarizeWuPayload(payload);
    if (!summary) {
      return res.status(500).json({ error: "Payload sin observations[]" });
    }

    const fecha = yyyymmddToDate(date);
    if (!fecha) return res.status(400).json({ error: "date inválida" });

    await pool.query(
      `
      INSERT INTO wu_daily (fecha, station_id, lluvia_mm, tmin_c, tmax_c, humedad_media, viento_max_mps)
      VALUES ($1,$2,$3,$4,$5,$6,$7)
      ON CONFLICT (fecha) DO UPDATE SET
        station_id = EXCLUDED.station_id,
        lluvia_mm = EXCLUDED.lluvia_mm,
        tmin_c = EXCLUDED.tmin_c,
        tmax_c = EXCLUDED.tmax_c,
        humedad_media = EXCLUDED.humedad_media,
        viento_max_mps = EXCLUDED.viento_max_mps;
      `,
      [
        fecha,
        String(stationId),
        summary.lluvia_mm,
        summary.tmin_c,
        summary.tmax_c,
        summary.humedad_media,
        summary.viento_max_mps,
      ]
    );

    
    // Guardar payload completo (todos los campos) sin afectar al resumen existente
    await pool.query(
      `
      INSERT INTO wu_daily_payload (fecha, station_id, payload)
      VALUES ($1,$2,$3::jsonb)
      ON CONFLICT (fecha) DO UPDATE SET
        station_id = EXCLUDED.station_id,
        payload = EXCLUDED.payload,
        creado_en = now();
      `,
      [fecha, String(stationId), payload]
    );

    return res.json({ ok: true, fecha, stationId, ...summary });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Fallo ingest", details: String(e?.message || e) });
  }
});

/* ============ Cron diario opcional (02:05 Madrid) ============ */
/**
 * Actívalo poniendo ENABLE_DAILY_INGEST=true y DAILY_STATION_ID=TU_ID
 * Guarda el día en curso (fecha de Madrid).
 */
if (pool && envBool(["ENABLE_DAILY_INGEST","HABILITAR_CARGA_DIARIA","ENABLE DAILY INGEST"], false)) {
  const station = envFirst(["DAILY_STATION_ID","WU_STATION_ID","WU_STATIONID","DAILY_STATION","ESTACION_DIARIA","ID_DE_ESTACION","id_de_estación","WU_STATION_ID"]);
  if (station) {
    cron.schedule(
      "5 2 * * *",
      async () => {
        const date = new Date()
          .toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" })
          .replaceAll("-", "");
        const url = `http://127.0.0.1:${PORT}/api/ingest/daily?stationId=${encodeURIComponent(
          station
        )}&date=${encodeURIComponent(date)}`;
        try {
          await fetch(url);
          console.log("✅ Ingest diario ok", station, date);
        } catch (err) {
          console.error("❌ Ingest diario error", err);
        }
      },
      { timezone: "Europe/Madrid" }
    );
    console.log("⏱️ Cron ingest diario habilitado (02:05 Europe/Madrid)");
  } else {
    console.log("⚠️ ENABLE_DAILY_INGEST=true pero falta DAILY_STATION_ID");
  }
}
/* ============ Arranque ============ */
(async () => {
  try {
    await initDb();
  } catch (e) {
    console.error("DB init failed (continuo sin DB):", e);
  }

  
  // Logs de arranque (sin enseñar secretos)
  console.log("🔧 Config:", {
    hasDB: !!DATABASE_URL,
    hasWUKey: !!WU_API_KEY,
    stationEnvPresent: !!envFirst(["DAILY_STATION_ID","WU_STATION_ID","WU_STATIONID","DAILY_STATION","ESTACION_DIARIA","ID_DE_ESTACION","WU_STATION_ID"]),
    dailyIngestEnabled: envBool(["ENABLE_DAILY_INGEST","HABILITAR_CARGA_DIARIA","ENABLE DAILY INGEST"], false)
  });

app.listen(PORT, "0.0.0.0", () => {
    console.log(`Servidor escuchando en http://0.0.0.0:${PORT}`);
  });
})();
