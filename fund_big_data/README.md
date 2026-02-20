# Normalizador de logs (JSON/NDJSON) → tablas

Este proyecto toma un archivo de logs donde cada evento es un objeto JSON (normalmente **1 JSON por línea**, tipo NDJSON) y lo convierte a tablas para análisis con pandas y para consumir desde una web.

## Qué genera
- `events`: un registro por request/evento
- `faces`: una fila por rostro detectado (si existe `rostros`)
- `sources`: una fila por elemento en `fuentes`

Por defecto exporta a JSON en formato **records**.

## Requisitos
- Python 3.9+ (recomendado 3.11+)

Instalar dependencias:

```bash
pip install -r requirements.txt
```

## Uso

```bash
python -m logtool parse --input "C:\\ruta\\a\\tu\\archivo.log" --out out
```

También puedes pasar varios archivos o una carpeta completa:

```bash
python -m logtool parse --input "C:\\logs\\20250701.log" "C:\\logs\\20250702.log" --out out
python -m logtool parse --input "C:\\logs" --glob "*.log" --out out
python -m logtool parse --input-dir "C:\\logs" --glob "*.log" --out out
```

Calcular estadísticas (sobre TODO el dataset) usando DuckDB:

```bash
# 1) Exporta JSONL (es lo mejor para datasets grandes)
python -m logtool parse --input "C:\\ruta\\a\\tu\\archivo.log" --out out --also-jsonl --no-json

# 2) Calcula media/varianza/mediana/moda/sesgo/kurtosis por columna numérica
python -m logtool stats --input-dir out --out out
```

Opciones útiles:
- `--chunk-size 10000`: procesa el archivo por bloques para no cargar todo en RAM.
- `--encoding utf-8`: especifica el encoding del archivo si no es UTF-8.
- `--single-file`: también genera un `dataset.json` con `events`, `faces`, `sources`.
- `--also-csv`: además exporta `events.csv`, `faces.csv`, `sources.csv`.
- `--also-jsonl`: además exporta `events.jsonl`, `faces.jsonl`, `sources.jsonl` (1 objeto por línea).
- `--also-sqlite`: además exporta una base SQLite con tablas `events`, `faces`, `sources`.
- `--sqlite-path dataset.sqlite`: nombre/ruta del archivo SQLite (por defecto dentro de `--out`).
- `--sqlite-overwrite`: si el SQLite existe, lo recrea.

Para archivos muy grandes (recomendado si solo quieres SQL):

```bash
python -m logtool parse --input tu.log --out out --also-sqlite --sqlite-overwrite --only-sqlite
```

## Notas sobre el formato de entrada
- Soporta NDJSON (un objeto JSON por línea).
- Si el archivo contiene JSON concatenado (sin saltos claros), intenta un parser por streaming como fallback.

## Notas de performance
- La exportación de `events.json`, `faces.json`, `sources.json` se hace en streaming (JSON array incremental), pensado para archivos grandes.
