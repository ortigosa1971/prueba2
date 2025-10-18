# WU Railway App (HTML + JS + Node Proxy)

Frontend estático + proxy Node/Express para consumir la API oficial de Weather Underground (api.weather.com) y mostrar observaciones históricas de una estación PWS.

## Variables de entorno

Copia `.env.example` a `.env` y rellena:

```
WU_API_KEY=tu_api_key_de_weather_com
```

> **Railway**: No subas `.env`. En el panel del proyecto añade una variable `WU_API_KEY` con tu clave.

## Ejecutar en local

```bash
npm i
# crea .env con tu WU_API_KEY
npm run dev
# abre http://localhost:3000
```

## Despliegue en Railway

1. Sube este proyecto a un repo de GitHub.
2. En Railway crea un nuevo servicio desde ese repo.
3. En **Variables**, añade `WU_API_KEY` con tu clave.
4. No hace falta definir puerto: Railway inyecta `PORT` y Express lo usa.

## Endpoint interno

`GET /api/wu/history?stationId=IALFAR32&date=YYYYMMDD`

- `stationId`: ID de estación PWS (ej. IALFAR32)
- `date`: fecha en formato `YYYYMMDD` (p.ej. `20250919`)

Proxy hacia: `https://api.weather.com/v2/pws/history/all` con `units=m&format=json&apiKey=...`

## Estructura

```
/public       # frontend estático
  index.html
  app.js
server.js     # proxy + estáticos
package.json
.env.example
```

## Notas

- Si antes usabas `19/09/2025`, cambia a `2025-09-19` en el selector y el código convertirá a `20250919` automáticamente.
- La UI calcula mínimos/máximos con los campos disponibles (`metric.tempLow/tempHigh/tempAvg`). Tolerante a variaciones del JSON de Weather.com.
