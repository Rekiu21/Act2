from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from itertools import chain
from pathlib import Path

import pandas as pd

from .io import JsonArrayWriter, copy_file_text, json_default, read_events, write_json
from .normalize import normalize
from .stats import TableSpec, compute_stats_from_jsonl


class _NullArrayWriter:
    def __init__(self, *args, **kwargs):
        return

    def __enter__(self):
        return self

    def write_many(self, objs):
        return

    def __exit__(self, exc_type, exc, tb):
        return


def _expand_inputs(
    inputs: list[str] | None,
    *,
    input_dir: str | None,
    glob_pattern: str,
) -> list[Path]:
    paths: list[Path] = []

    if inputs:
        for raw in inputs:
            p = Path(raw)
            # Expande comodines tipo *.log
            if any(ch in raw for ch in ["*", "?", "["]):
                parent = p.parent if str(p.parent) != "" else Path(".")
                pattern = p.name
                paths.extend(sorted(parent.glob(pattern)))
                continue
            if p.is_dir():
                paths.extend(sorted(p.glob(glob_pattern)))
                continue
            paths.append(p)

    if input_dir:
        d = Path(input_dir)
        if d.exists() and d.is_dir():
            paths.extend(sorted(d.glob(glob_pattern)))
        else:
            raise SystemExit(f"La carpeta de entrada no existe: {d}")

    # Normaliza y filtra duplicados preservando orden
    seen = set()
    uniq: list[Path] = []
    for p in paths:
        rp = p.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        uniq.append(p)

    if not uniq:
        raise SystemExit("No se encontraron archivos de entrada.")

    missing = [p for p in uniq if not p.exists() or not p.is_file()]
    if missing:
        raise SystemExit(
            "Algunos archivos de entrada no existen o no son archivos: "
            + ", ".join(str(p) for p in missing)
        )

    return uniq


def cmd_parse(args: argparse.Namespace) -> int:
    input_paths = _expand_inputs(
        args.input,
        input_dir=args.input_dir,
        glob_pattern=args.glob,
    )
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    chunk_size = int(args.chunk_size)
    encoding = str(args.encoding)

    # Procesamiento en streaming por chunks: evita cargar el archivo grande completo.
    total_events = 0
    total_faces = 0
    total_sources = 0
    tiempo_min = None
    tiempo_max = None

    events_json_path = out_dir / "events.json"
    faces_json_path = out_dir / "faces.json"
    sources_json_path = out_dir / "sources.json"

    if args.only_sqlite:
        args.no_json = True
        args.single_file = False
        args.also_csv = False
        args.also_jsonl = False

    export_json_arrays = not args.no_json

    csv_events_path = out_dir / "events.csv"
    csv_faces_path = out_dir / "faces.csv"
    csv_sources_path = out_dir / "sources.csv"

    jsonl_events_path = out_dir / "events.jsonl"
    jsonl_faces_path = out_dir / "faces.jsonl"
    jsonl_sources_path = out_dir / "sources.jsonl"

    wrote_events_csv_header = False
    wrote_faces_csv_header = False
    wrote_sources_csv_header = False

    # SQLite opcional
    sqlite_conn = None
    if args.also_sqlite:
        sqlite_path = Path(args.sqlite_path)
        if not sqlite_path.is_absolute():
            sqlite_path = out_dir / sqlite_path
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        if sqlite_path.exists() and args.sqlite_overwrite:
            sqlite_path.unlink()
        sqlite_conn = sqlite3.connect(str(sqlite_path))
        # Rendimiento decente para inserts grandes
        sqlite_conn.execute("PRAGMA journal_mode=WAL;")
        sqlite_conn.execute("PRAGMA synchronous=NORMAL;")
        sqlite_conn.execute("PRAGMA temp_store=MEMORY;")

    progress_every = int(args.progress_every)
    start_time = time.time()
    next_progress_at = progress_every if progress_every > 0 else None
    chunk_records: list[dict] = []

    jsonl_events_fp = None
    jsonl_faces_fp = None
    jsonl_sources_fp = None
    if args.also_jsonl:
        jsonl_events_fp = jsonl_events_path.open("w", encoding="utf-8")
        jsonl_faces_fp = jsonl_faces_path.open("w", encoding="utf-8")
        jsonl_sources_fp = jsonl_sources_path.open("w", encoding="utf-8")

    def flush_chunk(records_chunk: list[dict], *, writers) -> None:
        nonlocal total_events, total_faces, total_sources, tiempo_min, tiempo_max
        nonlocal wrote_events_csv_header, wrote_faces_csv_header, wrote_sources_csv_header
        nonlocal next_progress_at
        if not records_chunk:
            return

        events_df, faces_df, sources_df = normalize(records_chunk)

        # JSON (records) en streaming
        w_events, w_faces, w_sources = writers
        events_records = events_df.to_dict(orient="records")
        faces_records = faces_df.to_dict(orient="records")
        sources_records = sources_df.to_dict(orient="records")

        w_events.write_many(events_records)
        w_faces.write_many(faces_records)
        w_sources.write_many(sources_records)

        # JSONL opcional (1 JSON por línea)
        if jsonl_events_fp is not None:
            for row in events_records:
                jsonl_events_fp.write(json.dumps(row, ensure_ascii=False, default=json_default) + "\n")
            for row in faces_records:
                jsonl_faces_fp.write(json.dumps(row, ensure_ascii=False, default=json_default) + "\n")
            for row in sources_records:
                jsonl_sources_fp.write(json.dumps(row, ensure_ascii=False, default=json_default) + "\n")

        # CSV opcional en modo append
        if args.also_csv:
            mode_e = "a" if wrote_events_csv_header else "w"
            mode_f = "a" if wrote_faces_csv_header else "w"
            mode_s = "a" if wrote_sources_csv_header else "w"
            events_df.to_csv(csv_events_path, index=False, mode=mode_e, header=not wrote_events_csv_header)
            faces_df.to_csv(csv_faces_path, index=False, mode=mode_f, header=not wrote_faces_csv_header)
            sources_df.to_csv(csv_sources_path, index=False, mode=mode_s, header=not wrote_sources_csv_header)
            wrote_events_csv_header = True
            wrote_faces_csv_header = True
            wrote_sources_csv_header = True

        # SQLite opcional (append por chunks)
        if sqlite_conn is not None:
            # En el primer chunk creamos tablas con replace; luego append.
            if total_events == 0:
                if not events_df.empty:
                    events_df.to_sql("events", sqlite_conn, if_exists="replace", index=False)
                else:
                    pd.DataFrame(columns=events_df.columns).to_sql(
                        "events", sqlite_conn, if_exists="replace", index=False
                    )

                pd.DataFrame(columns=faces_df.columns).to_sql(
                    "faces", sqlite_conn, if_exists="replace", index=False
                )
                pd.DataFrame(columns=sources_df.columns).to_sql(
                    "sources", sqlite_conn, if_exists="replace", index=False
                )
            else:
                if not events_df.empty:
                    events_df.to_sql("events", sqlite_conn, if_exists="append", index=False)

            if not faces_df.empty:
                faces_df.to_sql("faces", sqlite_conn, if_exists="append", index=False)
            if not sources_df.empty:
                sources_df.to_sql("sources", sqlite_conn, if_exists="append", index=False)

            sqlite_conn.commit()

        total_events += int(len(events_df))
        total_faces += int(len(faces_df))
        total_sources += int(len(sources_df))

        if next_progress_at is not None and total_events >= next_progress_at:
            elapsed = time.time() - start_time
            rate = total_events / elapsed if elapsed > 0 else 0
            print(
                f"Procesados {total_events:,} events | {total_faces:,} faces | {total_sources:,} sources "
                f"({rate:,.0f} ev/s)",
                file=sys.stderr,
            )
            # avanza al siguiente hito
            while next_progress_at is not None and total_events >= next_progress_at:
                next_progress_at += progress_every

        if not events_df.empty and "tiempo" in events_df.columns:
            series = events_df["tiempo"]
            if series.notna().any():
                cmin = series.min()
                cmax = series.max()
                if tiempo_min is None or (not pd.isna(cmin) and cmin < tiempo_min):
                    tiempo_min = cmin
                if tiempo_max is None or (not pd.isna(cmax) and cmax > tiempo_max):
                    tiempo_max = cmax

    records_iter = chain.from_iterable(read_events(p, encoding=encoding) for p in input_paths)

    try:
        writer_cls = JsonArrayWriter if export_json_arrays else _NullArrayWriter

        with (
            writer_cls(events_json_path) as w_events,
            writer_cls(faces_json_path) as w_faces,
            writer_cls(sources_json_path) as w_sources,
        ):
            for rec in records_iter:
                if not isinstance(rec, dict):
                    continue
                chunk_records.append(rec)
                if len(chunk_records) >= chunk_size:
                    flush_chunk(chunk_records, writers=(w_events, w_faces, w_sources))
                    chunk_records.clear()

            flush_chunk(chunk_records, writers=(w_events, w_faces, w_sources))
    finally:
        if jsonl_events_fp is not None:
            jsonl_events_fp.close()
            jsonl_faces_fp.close()
            jsonl_sources_fp.close()
        if sqlite_conn is not None:
            sqlite_conn.close()

    summary = {
        "events": int(total_events),
        "faces": int(total_faces),
        "sources": int(total_sources),
        "tiempo_min": None if tiempo_min is None else tiempo_min.isoformat(),
        "tiempo_max": None if tiempo_max is None else tiempo_max.isoformat(),
        "input": str(input_paths[0]) if len(input_paths) == 1 else None,
        "inputs": [str(p) for p in input_paths],
        "chunk_size": int(chunk_size),
    }

    write_json(out_dir / "summary.json", summary)

    if args.single_file and export_json_arrays:
        # Construye dataset.json sin cargar arrays completos a memoria.
        dataset_path = out_dir / "dataset.json"
        with dataset_path.open("w", encoding="utf-8") as fp:
            fp.write('{"events":')
            copy_file_text(events_json_path, fp)
            fp.write(',"faces":')
            copy_file_text(faces_json_path, fp)
            fp.write(',"sources":')
            copy_file_text(sources_json_path, fp)
            fp.write(',"summary":')
            fp.write(json.dumps(summary, ensure_ascii=False, separators=(",", ":")))
            fp.write('}')

    # Índices opcionales para acelerar joins/consultas
    if args.also_sqlite and not args.no_sqlite_indexes:
        sqlite_path = Path(args.sqlite_path)
        if not sqlite_path.is_absolute():
            sqlite_path = out_dir / sqlite_path
        con = sqlite3.connect(str(sqlite_path))
        try:
            con.execute("CREATE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id);")
            con.execute("CREATE INDEX IF NOT EXISTS idx_faces_event_id ON faces(event_id);")
            con.execute("CREATE INDEX IF NOT EXISTS idx_sources_event_id ON sources(event_id);")
            con.commit()
        finally:
            con.close()

    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="logtool", description="Normaliza logs JSON/NDJSON a tablas")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_parse = sub.add_parser("parse", help="Parsea y normaliza un archivo de logs")
    p_parse.add_argument(
        "--input",
        nargs="+",
        default=None,
        help="Ruta(s) del archivo de entrada (puede repetir o usar comodines)",
    )
    p_parse.add_argument(
        "--input-dir",
        default=None,
        help="Carpeta con archivos a procesar (se usa con --glob)",
    )
    p_parse.add_argument(
        "--glob",
        default="*.log",
        help="Patrón glob para --input-dir o cuando --input apunta a carpeta (default: *.log)",
    )
    p_parse.add_argument("--out", default="out", help="Carpeta de salida")
    p_parse.add_argument("--encoding", default="utf-8", help="Encoding del archivo (default: utf-8)")
    p_parse.add_argument(
        "--chunk-size",
        default=10_000,
        type=int,
        help="Cantidad de eventos a procesar por chunk (default: 10000)",
    )
    p_parse.add_argument("--single-file", action="store_true", help="Genera un dataset.json único")
    p_parse.add_argument("--also-csv", action="store_true", help="También exporta CSV")
    p_parse.add_argument("--also-jsonl", action="store_true", help="También exporta JSONL (1 objeto por línea)")
    p_parse.add_argument("--no-json", action="store_true", help="No genera events.json/faces.json/sources.json")
    p_parse.add_argument("--only-sqlite", action="store_true", help="Exporta solo SQLite (implica --no-json)")
    p_parse.add_argument(
        "--progress-every",
        default=200000,
        type=int,
        help="Imprime progreso cada N events (0 para desactivar)",
    )
    p_parse.add_argument("--also-sqlite", action="store_true", help="También exporta SQLite (events/faces/sources)")
    p_parse.add_argument(
        "--sqlite-path",
        default="dataset.sqlite",
        help="Nombre o ruta del .sqlite (relativo a --out si no es absoluta)",
    )
    p_parse.add_argument(
        "--sqlite-overwrite",
        action="store_true",
        help="Si existe el .sqlite, lo borra y lo recrea",
    )
    p_parse.add_argument(
        "--no-sqlite-indexes",
        action="store_true",
        help="No crea índices en event_id al final",
    )
    p_parse.set_defaults(func=cmd_parse)

    p_stats = sub.add_parser("stats", help="Calcula estadísticas (media/var/mediana/moda/sesgo/kurtosis)")
    p_stats.add_argument(
        "--input-dir",
        default="out",
        help="Carpeta donde están events.jsonl/faces.jsonl/sources.jsonl (default: out)",
    )
    p_stats.add_argument("--out", default="out", help="Carpeta de salida")
    p_stats.add_argument(
        "--threads",
        default=None,
        type=int,
        help="Hilos para DuckDB (opcional). Si no se indica, usa default.",
    )
    p_stats.add_argument(
        "--emit-web",
        action="store_true",
        help="Además genera web/stats-data.js con los datos de stats.json",
    )
    p_stats.add_argument(
        "--web-dir",
        default=None,
        help="Carpeta web donde escribir stats-data.js (default: <out>/web)",
    )
    p_stats.set_defaults(func=cmd_stats)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


def cmd_stats(args: argparse.Namespace) -> int:
    input_dir = Path(args.input_dir)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    events = input_dir / "events.jsonl"
    faces = input_dir / "faces.jsonl"
    sources = input_dir / "sources.jsonl"

    missing = [p for p in [events, faces, sources] if not p.exists()]
    if missing:
        raise SystemExit(
            "Faltan archivos JSONL. Genera primero con: "
            "python -m logtool parse --input <archivo> --out <out> --also-jsonl --no-json\n"
            f"No encontrados: {', '.join(str(p) for p in missing)}"
        )

    tables = [
        TableSpec(name="events", jsonl_path=events),
        TableSpec(name="faces", jsonl_path=faces),
        TableSpec(name="sources", jsonl_path=sources),
    ]

    stats = compute_stats_from_jsonl(tables, read_threads=args.threads)
    stats_path = out_dir / "stats.json"
    write_json(stats_path, stats)

    if args.emit_web:
        web_dir = Path(args.web_dir) if args.web_dir else (out_dir / "web")
        web_dir.mkdir(parents=True, exist_ok=True)
        js_path = web_dir / "stats-data.js"
        text = stats_path.read_text(encoding="utf-8")
        js_path.write_text("window.__STATS__ = " + text + ";\n", encoding="utf-8")
    return 0
