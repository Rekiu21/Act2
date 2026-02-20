from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd


EVENT_FIELDS = [
    "tiempo",
    "estado",
    "metodo",
    "pedido",
    "server",
    "baliza",
    "acceso",
    "decode",
    "detect",
    "refine",
    "assess",
    "vector",
    "search",
    "verify",
    "commit",
    "entire",
    "envase",
    "nombre",
]


def stable_event_id(event: Dict[str, Any]) -> str:
    # Usamos un subconjunto para que sea estable y razonablemente único.
    key = {
        "tiempo": event.get("tiempo"),
        "baliza": event.get("baliza"),
        "pedido": event.get("pedido"),
        "nombre": event.get("nombre"),
    }
    raw = json.dumps(key, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def normalize(records: Iterable[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    events_rows: List[Dict[str, Any]] = []
    faces_rows: List[Dict[str, Any]] = []
    sources_rows: List[Dict[str, Any]] = []

    for rec in records:
        event_id = stable_event_id(rec)

        fuentes = rec.get("fuentes") or []
        rostros = rec.get("rostros") or []

        event_row: Dict[str, Any] = {"event_id": event_id}
        for field in EVENT_FIELDS:
            event_row[field] = rec.get(field)
        event_row["fuentes_count"] = len(fuentes) if isinstance(fuentes, list) else None
        event_row["rostros_count"] = len(rostros) if isinstance(rostros, list) else None
        events_rows.append(event_row)

        if isinstance(fuentes, list):
            for idx, fuente in enumerate(fuentes):
                sources_rows.append({"event_id": event_id, "source_index": idx, "fuente": fuente})

        if isinstance(rostros, list):
            for face_index, face in enumerate(rostros):
                eyes = (face or {}).get("eyes") or []
                eye1 = eyes[0] if len(eyes) > 0 else [None, None]
                eye2 = eyes[1] if len(eyes) > 1 else [None, None]

                faces_rows.append(
                    {
                        "event_id": event_id,
                        "face_index": face_index,
                        "l": (face or {}).get("l"),
                        "t": (face or {}).get("t"),
                        "r": (face or {}).get("r"),
                        "b": (face or {}).get("b"),
                        "face_score": (face or {}).get("face"),
                        "size": (face or {}).get("size"),
                        "eye1_x": eye1[0] if isinstance(eye1, list) and len(eye1) > 0 else None,
                        "eye1_y": eye1[1] if isinstance(eye1, list) and len(eye1) > 1 else None,
                        "eye2_x": eye2[0] if isinstance(eye2, list) and len(eye2) > 0 else None,
                        "eye2_y": eye2[1] if isinstance(eye2, list) and len(eye2) > 1 else None,
                    }
                )

    events_df = pd.DataFrame(events_rows)
    faces_df = pd.DataFrame(faces_rows)
    sources_df = pd.DataFrame(sources_rows)

    if not events_df.empty and "tiempo" in events_df.columns:
        # Formato típico del log: "YYYY-MM-DD HH:MM:SS.mmm". Si alguna fila difiere, cae al parser general.
        tiempo = pd.to_datetime(
            events_df["tiempo"],
            format="%Y-%m-%d %H:%M:%S.%f",
            errors="coerce",
        )
        missing = tiempo.isna()
        if missing.any():
            tiempo2 = pd.to_datetime(events_df.loc[missing, "tiempo"], errors="coerce")
            tiempo.loc[missing] = tiempo2

        events_df["tiempo"] = tiempo

        # Útil para web/cálculos (ms desde epoch). NaT -> <NA>
        tiempo_ns = tiempo.astype("int64")
        tiempo_ms = tiempo_ns // 1_000_000
        events_df["tiempo_epoch_ms"] = pd.Series(tiempo_ms, dtype="Int64").where(tiempo.notna(), pd.NA)

    return events_df, faces_df, sources_df
