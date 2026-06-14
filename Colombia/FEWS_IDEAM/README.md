# Sistema de almacenamiento FEWS IDEAM

Descarga y almacena de forma incremental los datos de nivel y caudal
publicados por el IDEAM en `https://fews.ideam.gov.co`. Diseñado para
correr en paralelo al proyecto SATMA siguiendo el mismo patrón.

## Flujo de 3 pasos

```
[ 1. build_stations_csv.py ]  →  FEWS_estaciones.csv
                                       ↓
[ 2. init_db.py ]             →  data/fews.db (tablas + estaciones)
                                       ↓
[ 3. update_data.py ]         →  data/fews.db (mediciones, todos los días)
```

### Paso 1: construir el CSV maestro (una sola vez)

```bash
python build_stations_csv.py
```

Descarga los 5 endpoints del IDEAM, deduplica por `id`, y para cada
estación prueba `/jsonH/{id}.json` y `/jsonQ/{id}.json` para marcar
`NIVEL`/`CAUDAL` como TRUE/FALSE.

Resultado: `FEWS_estaciones.csv` (análogo al `SATMA_GEOGLOWS.csv`).
Puedes inspeccionarlo, filtrarlo o editarlo a mano si quieres antes del
paso 2.

Tarda ~10 min para ~1000 estaciones (2 llamadas de descubrimiento por
estación + el sleep entre ellas). Solo se corre cuando quieras refrescar
el catálogo.

Opciones útiles:
```bash
python build_stations_csv.py --max 50          # solo primeras 50, para pruebas
python build_stations_csv.py --skip-discovery  # CSV sin probar /jsonH y /jsonQ
                                               # (NIVEL/CAUDAL quedan en FALSE)
```

### Paso 2: inicializar la BD (una sola vez)

```bash
python init_db.py
```

Lee `FEWS_estaciones.csv`, crea las tablas y carga estaciones. Si la BD ya
existía y vuelves a correr, hace UPSERT — actualiza metadatos sin tocar
las mediciones ya guardadas.

### Paso 3: actualización diaria

```bash
python update_data.py
```

Para cada estación con `NIVEL=1` o `CAUDAL=1` en la BD, descarga las
series correspondientes y las guarda. Las estaciones con ambos FALSE se
saltan. `INSERT OR IGNORE` evita duplicados al re-correr.

La API IDEAM retiene ~1 mes, por eso el umbral de inactividad es 30 días.
No hay flag `--initial` como en SATMA porque la API no acepta un
parámetro de cantidad de días: simplemente devuelve lo que tenga, y la BD
local va acumulando histórico día a día.

Opciones:
```bash
python update_data.py --estacion 0011027030    # solo una estación
python update_data.py --max 20                 # primeras 20 (pruebas)
```

### Paso opcional: exportar a CSV

```bash
python export_csv.py
```

Genera `exports/estaciones.csv` + `exports/mediciones/{id}_{var}.csv`.

## Cómo programar la corrida diaria

**Windows (Task Scheduler):**
- Crear tarea básica, disparador diario (ej. 03:00).
- Acción: `python.exe` con argumento `update_data.py`, iniciar en la
  carpeta del proyecto.

**Linux (cron):**
```cron
0 3 * * * cd /ruta/a/fews_db && /usr/bin/python3 update_data.py >> logs/cron.log 2>&1
```

## Esquema de la BD

**`estaciones`** — Una fila por estación. Mismas columnas que el CSV:

- Identificación: `id` (PK), `nombre`, `lng`, `lat`, `altitud`.
- Geografía: `corriente`, `zona`, `subzona`, `cenpoblado`, `municipio`, `depart`.
- Operación: `ctg`, `cotacero`, `areaaferete`.
- Máximos: `maxnivel` (Hsim), `maxcaudal` (Qsim).
- Umbrales numéricos: `uroja`, `unaranja`, `uamarilla`, `ubajos`, `umaxhis`.
- `subred` (cuando viene de Calidad).
- Disponibilidad: `NIVEL`, `CAUDAL` (0/1, del CSV).
- Estado runtime: `estado`, `ultima_actualizacion`, `ultima_observacion`.

Todos los strings se normalizan a ASCII al extraerlos de los JSON del
IDEAM (`NARIÑO → NARINO`, `Limnigráfica → Limnigrafica`), para evitar
problemas de codificación al exportar y consultar.

Los campos volátiles del IDEAM (`ultimonivelsen`, `ultimoqsen`, `Estado`
textual, datos de Calidad, etc.) **no** se guardan acá: quedarían
desactualizados al instante. El dato más reciente se deriva de
`mediciones`.

**`mediciones`** — Formato largo:
`(id_estacion, variable, timestamp, valor)` con
`variable ∈ {Hobs, Hsen, Qobs, Qsen}` y
`UNIQUE(id_estacion, variable, timestamp)`.

**`log_actualizaciones`** — Una fila por llamada con `status` ∈
`{success, no_data, not_found, error}`.

## Lógica de estado

Tras procesar cada estación, `update_data.py` mira el `MAX(timestamp)` de
sus mediciones:

- ≤ 30 días → **ACTIVA**
- > 30 días o sin datos → **INACTIVA**

Las INACTIVAS se siguen consultando. Si vuelven a emitir datos, pasan
solas a ACTIVA en la siguiente corrida.

Las estaciones con `NIVEL=0` y `CAUDAL=0` quedan en `DESCONOCIDO` porque
`update_data.py` nunca las consulta. Para incluirlas, refresca el catálogo
con `build_stations_csv.py`.

## Refrescar el catálogo

Si sospechas que aparecieron estaciones nuevas, o que alguna empezó/dejó
de publicar:

```bash
python build_stations_csv.py   # regenera el CSV
python init_db.py              # UPSERT a la BD (no toca mediciones)
```

Tus datos ya descargados quedan intactos.

## Consultas SQL útiles

```sql
-- Distribución de variables
SELECT NIVEL, CAUDAL, COUNT(*) FROM estaciones GROUP BY NIVEL, CAUDAL;

-- Estaciones ACTIVAS con más datos
SELECT e.id, e.nombre, e.depart, COUNT(*) AS n
FROM estaciones e JOIN mediciones m ON m.id_estacion = e.id
WHERE e.estado = 'ACTIVA'
GROUP BY e.id, e.nombre, e.depart
ORDER BY n DESC LIMIT 20;

-- Errores recientes
SELECT fecha_ejecucion, id_estacion, variable, mensaje
FROM log_actualizaciones WHERE status = 'error'
ORDER BY fecha_ejecucion DESC LIMIT 50;
```

## Notas operativas

- El IDEAM da 503 transitorios con frecuencia. El cliente reintenta 3
  veces con backoff. Si una estación falla, se loguea y se reintenta al
  día siguiente.
- `verify=False` está activado por defecto (cert autofirmado del IDEAM).
- Si la API te lanza muchos 503 sube `HTTP_SLEEP_BETWEEN_CALLS` a `0.5` o
  `1.0` en `config.py`.
