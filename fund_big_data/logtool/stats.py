from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import duckdb


_NUMERIC_TYPES = {
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "HUGEINT",
    "UHUGEINT",
    "FLOAT",
    "REAL",
    "DOUBLE",
    "DECIMAL",
}

_EVENT_FOCUS_REQUESTED = ["time", "status", "metod", "request", "server", "uuid"]
_EVENT_FOCUS_ALIASES = {
    "time": "tiempo",
    "status": "estado",
    "metod": "metodo",
    "method": "metodo",
    "request": "pedido",
    "server": "server",
    "uuid": "event_id",
}


@dataclass(frozen=True)
class TableSpec:
    name: str
    jsonl_path: Path


def _sql_string_literal(value: str) -> str:
    # Escapa comillas simples para literales SQL: ' -> ''
    return "'" + value.replace("'", "''") + "'"


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _is_temporal_type(type_name: str) -> bool:
    t = type_name.upper().strip()
    return t.startswith("TIMESTAMP") or t in {
        "DATE",
        "TIME",
        "TIME WITH TIME ZONE",
        "TIME WITHOUT TIME ZONE",
    }


def _duckdb_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> List[tuple[str, str]]:
    rows = con.execute(f"DESCRIBE {table_name}").fetchall()
    out: List[tuple[str, str]] = []
    for col_name, col_type, *_ in rows:
        out.append((str(col_name), str(col_type).upper().split("(", 1)[0].strip()))
    return out


def _duckdb_numeric_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
    rows = _duckdb_columns(con, table_name)
    numeric_cols: List[str] = []
    for col_name, type_name in rows:
        if type_name in _NUMERIC_TYPES:
            numeric_cols.append(str(col_name))
    return numeric_cols


def _focused_profile(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    col: str,
    col_type: str,
    *,
    topn: int = 10,
) -> Dict[str, Any]:
    colq = _quote_ident(col)

    n_rows, n_nonnull, distinct_approx = con.execute(
        f"SELECT COUNT(*), COUNT({colq}), approx_count_distinct({colq}) FROM {table_name}"
    ).fetchone()

    top_rows = con.execute(
        f"SELECT CAST({colq} AS VARCHAR) AS value, COUNT(*) AS c "
        f"FROM {table_name} WHERE {colq} IS NOT NULL "
        f"GROUP BY {colq} ORDER BY c DESC LIMIT {int(topn)}"
    ).fetchall()
    top_values = [{"value": value, "count": int(count)} for value, count in top_rows]

    profile: Dict[str, Any] = {
        "column": col,
        "type": col_type,
        "n_rows": int(n_rows),
        "n_nonnull": int(n_nonnull),
        "n_null": int(n_rows) - int(n_nonnull),
        "distinct_approx": int(distinct_approx) if distinct_approx is not None else None,
        "top_values": top_values,
    }

    if _is_temporal_type(col_type):
        min_v, max_v = con.execute(
            f"SELECT MIN({colq}), MAX({colq}) FROM {table_name} WHERE {colq} IS NOT NULL"
        ).fetchone()
        profile["min"] = min_v
        profile["max"] = max_v
    elif col_type in _NUMERIC_TYPES:
        mean_v, median_v = con.execute(
            f"SELECT AVG({colq}), MEDIAN({colq}) FROM {table_name} WHERE {colq} IS NOT NULL"
        ).fetchone()
        profile["mean"] = mean_v
        profile["median"] = median_v
    else:
        len_mean, len_var, len_median, len_skew, len_kurt = con.execute(
            f"""
            SELECT
              AVG(length(CAST({colq} AS VARCHAR))),
              VAR_POP(length(CAST({colq} AS VARCHAR))),
              MEDIAN(length(CAST({colq} AS VARCHAR))),
              SKEWNESS(length(CAST({colq} AS VARCHAR))),
              KURTOSIS(length(CAST({colq} AS VARCHAR)))
            FROM {table_name}
            WHERE {colq} IS NOT NULL
            """
        ).fetchone()

        mode_len_row = con.execute(
            f"""
            SELECT length(CAST({colq} AS VARCHAR)) AS l, COUNT(*) AS c
            FROM {table_name}
            WHERE {colq} IS NOT NULL
            GROUP BY l
            ORDER BY c DESC, l DESC
            LIMIT 1
            """
        ).fetchone()

        profile["mean"] = len_mean
        profile["variance"] = len_var
        profile["median"] = len_median
        profile["skewness"] = len_skew
        profile["kurtosis"] = len_kurt
        profile["mode"] = top_values[0]["value"] if top_values else None
        profile["mode_count"] = top_values[0]["count"] if top_values else 0
        profile["mode_length"] = int(mode_len_row[0]) if mode_len_row else None
        profile["length_stats"] = {
            "mean": len_mean,
            "variance": len_var,
            "median": len_median,
            "mode": int(mode_len_row[0]) if mode_len_row else None,
            "skewness": len_skew,
            "kurtosis": len_kurt,
        }

    return profile


def _stat_row(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    col: str,
    *,
    topn_mode: int = 1,
) -> Dict[str, Any]:
    # Nota: mode() existe en DuckDB, pero para floats puede ser poco útil; lo hacemos igual.
    # Para evitar problemas con nombres raros, quoteamos.
    colq = _quote_ident(col)

    # Moda (top-1): hacemos group by explícito (más robusto y permite null handling)
    mode_sql = (
        f"SELECT {colq} AS value, COUNT(1) AS c "
        f"FROM {table_name} WHERE {colq} IS NOT NULL "
        f"GROUP BY {colq} ORDER BY c DESC LIMIT {int(topn_mode)}"
    )
    mode_rows = con.execute(mode_sql).fetchall()
    mode_value = mode_rows[0][0] if mode_rows else None
    mode_count = int(mode_rows[0][1]) if mode_rows else 0

    sql = f"""
        SELECT
          COUNT(*) AS n_rows,
          COUNT({colq}) AS n_nonnull,
          AVG({colq}) AS mean,
          VAR_POP({colq}) AS variance,
          MEDIAN({colq}) AS median,
          SKEWNESS({colq}) AS skewness,
          KURTOSIS({colq}) AS kurtosis
        FROM {table_name}
    """

    n_rows, n_nonnull, mean, variance, median, skewness, kurtosis = con.execute(sql).fetchone()

    return {
        "column": col,
        "n_rows": int(n_rows),
        "n_nonnull": int(n_nonnull),
        "n_null": int(n_rows) - int(n_nonnull),
        "mean": mean,
        "variance": variance,
        "median": median,
        "mode": mode_value,
        "mode_count": mode_count,
        "skewness": skewness,
        "kurtosis": kurtosis,
    }


def _corr_matrix(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    numeric_cols: List[str],
) -> Dict[str, Any]:
    n = len(numeric_cols)
    if n == 0:
        return {"columns": [], "matrix": [], "n_pairs": []}

    # Construye una sola query con todas las correlaciones y conteos para evitar
    # escanear la tabla N^2 veces.
    select_parts: List[str] = []
    for i in range(n):
        ci = _quote_ident(numeric_cols[i])
        for j in range(i, n):
            cj = _quote_ident(numeric_cols[j])
            corr_alias = f"corr__{i}__{j}"
            n_alias = f"n__{i}__{j}"
            select_parts.append(f"corr({ci}, {cj}) AS {corr_alias}")
            select_parts.append(
                f"count(*) FILTER (WHERE {ci} IS NOT NULL AND {cj} IS NOT NULL) AS {n_alias}"
            )

    sql = "SELECT\n  " + ",\n  ".join(select_parts) + f"\nFROM {table_name}"
    cur = con.execute(sql)
    row = cur.fetchone()
    colnames = [d[0] for d in cur.description]
    values = dict(zip(colnames, row))

    matrix: List[List[Optional[float]]] = [[None for _ in range(n)] for _ in range(n)]
    n_pairs: List[List[int]] = [[0 for _ in range(n)] for _ in range(n)]

    for i in range(n):
        for j in range(i, n):
            corr_alias = f"corr__{i}__{j}"
            n_alias = f"n__{i}__{j}"
            c = values.get(corr_alias)
            np = values.get(n_alias)

            # DuckDB puede retornar NaN; lo dejamos (JS lo soporta), pero JSON puro no.
            # Como esto se serializa vía json (Python), NaN se convertirá a NaN si allow_nan=True.
            # Para máxima compatibilidad, lo convertimos a None.
            try:
                if c is not None and isinstance(c, float) and c != c:
                    c = None
            except Exception:
                pass

            npi = int(np) if np is not None else 0
            matrix[i][j] = c
            matrix[j][i] = c
            n_pairs[i][j] = npi
            n_pairs[j][i] = npi

    return {
        "columns": numeric_cols,
        "matrix": matrix,
        "n_pairs": n_pairs,
    }


def compute_stats_from_jsonl(
    tables: Iterable[TableSpec],
    *,
    read_threads: Optional[int] = None,
) -> Dict[str, Any]:
    con = duckdb.connect(database=":memory:")
    try:
        if read_threads is not None:
            # DuckDB no permite parámetros preparados en SET
            con.execute(f"SET threads = {int(read_threads)}")

        result: Dict[str, Any] = {}

        for t in tables:
            # DuckDB detecta NDJSON/JSONL; explicitamos format para evitar ambigüedades
            jsonl_path_lit = _sql_string_literal(str(t.jsonl_path))
            # CREATE VIEW tampoco acepta parámetros preparados en DuckDB
            con.execute(
                f"CREATE OR REPLACE VIEW {t.name} AS "
                f"SELECT * FROM read_json_auto({jsonl_path_lit}, format='newline_delimited')"
            )

            numeric_cols = _duckdb_numeric_columns(con, t.name)
            table_cols = _duckdb_columns(con, t.name)
            table_cols_map = {name: typ for name, typ in table_cols}
            stats_rows: List[Dict[str, Any]] = []
            for col in numeric_cols:
                stats_rows.append(_stat_row(con, t.name, col))

            corr = _corr_matrix(con, t.name, numeric_cols)

            # Conteo total de filas
            n_total = int(con.execute(f"SELECT COUNT(*) FROM {t.name}").fetchone()[0])
            table_result: Dict[str, Any] = {
                "rows": n_total,
                "numeric_columns": numeric_cols,
                "stats": stats_rows,
                "correlation": corr,
            }

            if t.name == "events":
                mapping: List[Dict[str, Any]] = []
                seen_resolved = set()
                profiles: List[Dict[str, Any]] = []

                for requested in _EVENT_FOCUS_REQUESTED:
                    resolved = _EVENT_FOCUS_ALIASES.get(requested, requested)
                    exists = resolved in table_cols_map
                    mapping.append(
                        {
                            "requested": requested,
                            "resolved": resolved,
                            "exists": exists,
                        }
                    )
                    if not exists or resolved in seen_resolved:
                        continue
                    seen_resolved.add(resolved)
                    profiles.append(
                        _focused_profile(
                            con,
                            t.name,
                            resolved,
                            table_cols_map[resolved],
                            topn=10,
                        )
                    )

                table_result["focused_analysis"] = {
                    "requested": _EVENT_FOCUS_REQUESTED,
                    "mapping": mapping,
                    "profiles": profiles,
                }

            result[t.name] = table_result

        return result
    finally:
        con.close()
