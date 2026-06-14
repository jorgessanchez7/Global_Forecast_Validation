# Sistema de almacenamiento SATMA

Proyecto para descargar y almacenar de forma incremental los datos de las
estaciones SATMA (`https://api.satma.co`).

## Estructura del proyecto

```
satma_db/
├── config.py            # Configuración (rutas, URL, umbrales)
├── utils.py             # Cliente HTTP, logger, conexión SQLite
├── init_db.py           # Crea la BD y carga metadatos del CSV
├── update_data.py       # Actualización (diaria o inicial)
├── export_csv.py        # Exporta la BD a archivos CSV
├── SATMA_GEOGLOWS.csv   # CSV de metadatos (lo provees tú)
├── data/
│   └── satma.db         # Base SQLite generada
├── logs/
│   └── satma_YYYYMMDD.log
└── exports/             # CSVs generados por export_csv.py
```

## Esquema de la base de datos

**`estaciones`** — Metadatos + estado.
Columnas: `idEstacion (PK)`, `codEstacion`, `Estacion`, `Latitud`, `Longitud`,
`Altitud`, `Ubicacion`, `Zona`, `FechaInstalacion`, `Propietario`, `Tipo`,
`TypeDB`, las 11 flags booleanas (`temperature`, `realPrecipitation`, ...,
`riverFlow`), `estado` (`ACTIVA`/`INACTIVA`/`DESCONOCIDO`),
`ultima_actualizacion`, `ultima_observacion`.

**`mediciones`** — Datos en formato largo.
`(id, idEstacion, variable, timestamp, valor)` con `UNIQUE(idEstacion, variable,
timestamp)`. Re-correr la actualización no duplica datos gracias a
`INSERT OR IGNORE`.

**`log_actualizaciones`** — Bitácora de cada llamada a la API.
`(fecha_ejecucion, idEstacion, variable, status, registros_nuevos, mensaje)`.
`status` puede ser `success`, `no_data` o `error`.

## Primer arranque

```bash
pip install requests
python init_db.py                # crea esquema + carga metadatos del CSV
python update_data.py --initial  # carga histórica de 180 días
```

La carga inicial puede tardar (≈ 1 llamada por (estación × variable habilitada)).
Para ~127 estaciones con 5–10 variables cada una, son ~600–1200 llamadas.

## Actualización diaria

```bash
python update_data.py            # pide los últimos 2 días (configurable)
```

### Programar la ejecución diaria

**Linux / macOS (cron)** — corre todos los días a las 03:00:

```cron
0 3 * * * cd /ruta/a/satma_db && /usr/bin/python3 update_data.py >> logs/cron.log 2>&1
```

**Windows (Task Scheduler)**:
1. Crear tarea básica → Disparador diario.
2. Acción: Iniciar programa `python`, argumentos `update_data.py`,
   iniciar en `C:\ruta\a\satma_db`.

## Lógica de estado (ACTIVA / INACTIVA)

Tras procesar cada estación, `update_data.py` consulta el `MAX(timestamp)` de
sus mediciones en *cualquier* variable:

- Si el dato más reciente es de hace ≤ `INACTIVITY_THRESHOLD_DAYS` (180 por
  defecto) → **ACTIVA**.
- Si no hay datos, o el más reciente es más viejo → **INACTIVA**.

Las estaciones INACTIVAS **se siguen consultando todos los días**: si vuelven a
emitir datos, pasarán a ACTIVA automáticamente.

## Consultas útiles

```sql
-- Estaciones inactivas y desde cuándo
SELECT codEstacion, Estacion, ultima_observacion
FROM estaciones
WHERE estado = 'INACTIVA'
ORDER BY ultima_observacion DESC NULLS LAST;

-- Cuántos registros tengo por estación y variable
SELECT idEstacion, variable, COUNT(*) AS n, MIN(timestamp), MAX(timestamp)
FROM mediciones
GROUP BY idEstacion, variable
ORDER BY idEstacion, variable;

-- Errores recientes
SELECT fecha_ejecucion, idEstacion, variable, mensaje
FROM log_actualizaciones
WHERE status = 'error'
ORDER BY fecha_ejecucion DESC
LIMIT 50;
```

## Notas / ajustes probables

1. **Formato de respuesta de la API**: `utils._normalize_records` intenta
   adivinar las claves (`fecha`/`timestamp`/`date` y `valor`/`value`). Cuando
   veas la primera respuesta real, ajusta esa función a las claves exactas.
2. **Pausa entre llamadas**: `config.API_SLEEP_BETWEEN_CALLS = 0.3 s`. Si la
   API es lenta o limita peticiones, súbelo. Si va sobrada, bájalo.
3. **Migrar a PostgreSQL / MySQL más adelante**: el esquema es estándar, solo
   habría que cambiar `utils.get_connection()` y los `AUTOINCREMENT`.
