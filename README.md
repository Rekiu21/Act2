# ACT 2 — Normalización de logs y tablero de métricas

Este repositorio contiene un flujo ETL para procesar archivos de logs en JSON/NDJSON y generar:

- Tablas normalizadas (`events`, `faces`, `sources`).
- Estadísticas descriptivas y una matriz de correlación en `stats.json`.
- Un tablero web (HTML) que consume `stats.json` y muestra tabla + gráficas.

## Estructura

- `fund_big_data/logtool/`: paquete con la CLI y utilidades.
- `fund_big_data/requirements.txt`: dependencias.
- `fund_big_data/out_full/web/index.html`: tablero web.
- `fund_big_data/out_full/stats.json`: estadísticas usadas por el tablero.

## Flujo de trabajo

1) Lectura de entradas en streaming
- Soporta NDJSON (1 objeto JSON por línea) y JSON concatenado.

2) Transformación
- Normaliza la información a tablas (`events`, `faces`, `sources`).
- Calcula métricas por columna numérica: media, varianza, mediana, moda, sesgo y kurtosis.
- Calcula correlaciones entre columnas numéricas.

3) Carga / salida
- Exporta salidas en formatos de análisis (JSON/JSONL/CSV según opciones).
- Genera `stats.json` para el consumo en la web.

## Columnas usadas en la vista

Para mantener consistencia entre tabla y gráficas del tablero, se usan estas columnas (con alias de UI):

- `estado`
- `decode`
- `detect`
- `refine`
- `vector`
- `commit`
- `entire`
- `fuentes_count` (UI: `fuentes_co`)
- `rostros_count` (UI: `rostros_co`)
- `tiempo_epoch_ms` (UI: `tiempo_epo`)

## Cómo correr

### 1) Instalar dependencias

```bash
cd fund_big_data
pip install -r requirements.txt
```

### 2) Parsear logs (ETL)

Ejemplo (ajusta rutas):

```bash
python -m logtool parse --input "C:\\ruta\\a\\tu\\archivo.log" --out out
```

### 3) Calcular estadísticas (stats.json)

```bash
python -m logtool stats --input-dir out --out out
```

## Abrir el tablero en local

Desde la raíz del repo:

```bash
python -m http.server 8000
```

Luego abre:

- `http://localhost:8000/fund_big_data/out_full/web/index.html`

