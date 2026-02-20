from __future__ import annotations

import math
import json
from pathlib import Path
from typing import IO, Any, Dict, Iterable, Iterator, Optional, TextIO, Union


def json_default(obj: Any) -> Any:
    """Convierte tipos no-serializables (pandas/numpy) a JSON-friendly."""

    # pandas.NA
    if type(obj).__name__ == "NAType":
        return None

    # datetime-like (incluye pandas.Timestamp)
    iso = getattr(obj, "isoformat", None)
    if callable(iso):
        try:
            return iso()
        except TypeError:
            pass

    # numpy scalar
    item = getattr(obj, "item", None)
    if callable(item):
        try:
            return item()
        except Exception:
            pass

    return str(obj)


def _sanitize_non_finite(obj: Any) -> Any:
    """Convierte NaN/Inf a None para producir JSON estricto.

    Python por defecto serializa NaN/Infinity, pero eso no es JSON válido y
    rompe `fetch(...).json()` en el navegador.
    """

    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _sanitize_non_finite(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_non_finite(v) for v in obj]
    if isinstance(obj, tuple):
        return [_sanitize_non_finite(v) for v in obj]
    return obj


def _looks_like_ndjson(path: Path, encoding: str, sample_lines: int = 50) -> bool:
    """Detecta NDJSON sin consumir el iterador principal.

    Regla simple: si las primeras N líneas no vacías parsean como JSON completo,
    asumimos NDJSON. Si alguna falla, usamos el parser de JSON concatenado.
    """

    with path.open("r", encoding=encoding, errors="replace") as fp:
        seen = 0
        for line in fp:
            line = line.lstrip("\ufeff").strip()
            if not line:
                continue
            # Si empieza con '[' es común que sea JSON array; tratamos como concatenado.
            if line.startswith("["):
                return False
            try:
                json.loads(line)
            except json.JSONDecodeError:
                return False
            seen += 1
            if seen >= sample_lines:
                break
    return seen > 0


def iter_ndjson(fp: IO[str]) -> Iterator[Dict[str, Any]]:
    for line_no, line in enumerate(fp, start=1):
        line = line.lstrip("\ufeff").strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"No se pudo parsear NDJSON en línea {line_no}: {e.msg}"
            ) from e
        if isinstance(obj, dict):
            yield obj
        else:
            yield {"_value": obj}


def iter_concatenated_json(fp: IO[str], chunk_size: int = 1_048_576) -> Iterator[Dict[str, Any]]:
    decoder = json.JSONDecoder()
    buffer = ""

    while True:
        chunk = fp.read(chunk_size)
        if not chunk:
            break
        buffer += chunk

        while True:
            # También tolera BOM al inicio (utf-8-sig)
            stripped = buffer.lstrip("\ufeff\t\r\n ")
            if not stripped:
                buffer = ""
                break

            ws_len = len(buffer) - len(stripped)
            buffer = stripped

            try:
                obj, end = decoder.raw_decode(buffer)
            except json.JSONDecodeError as e:
                # Necesita más datos (probablemente cortado al final del buffer)
                # Si el error no está cerca del final, el archivo no es JSON concatenado válido.
                if e.pos < max(0, len(buffer) - 256):
                    raise ValueError(
                        "El archivo no parece NDJSON ni JSON concatenado válido. "
                        "Sugerencia: revisa si hay texto extra/no-JSON entre objetos."
                    ) from e
                # Leer más
                buffer = (" " * ws_len) + buffer
                break

            buffer = buffer[end:]
            if isinstance(obj, dict):
                yield obj
            else:
                yield {"_value": obj}


def read_events(path: str | Path, encoding: str = "utf-8") -> Iterator[Dict[str, Any]]:
    """Lee eventos desde un archivo grande en formato NDJSON o JSON concatenado.

    Nota: evitamos el enfoque "yield y si falla hago fallback" porque, si el error
    ocurre después de haber emitido registros, ya no es posible retroceder.
    """

    path = Path(path)
    use_ndjson = _looks_like_ndjson(path, encoding=encoding)

    with path.open("r", encoding=encoding, errors="replace") as fp:
        if use_ndjson:
            yield from iter_ndjson(fp)
        else:
            yield from iter_concatenated_json(fp)


def write_json(path: str | Path, data: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(
            _sanitize_non_finite(data),
            fp,
            ensure_ascii=False,
            indent=2,
            default=json_default,
            allow_nan=False,
        )


def write_jsonl(path: str | Path, rows: Iterable[Dict[str, Any]]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(
                json.dumps(
                    _sanitize_non_finite(row),
                    ensure_ascii=False,
                    default=json_default,
                    allow_nan=False,
                )
                + "\n"
            )


class JsonArrayWriter:
    """Escribe un array JSON de forma incremental (sin cargar todo en memoria)."""

    def __init__(
        self,
        target: Union[str, Path, TextIO],
        *,
        ensure_ascii: bool = False,
        separators: tuple[str, str] = (",", ":"),
    ) -> None:
        self._target = target
        self._ensure_ascii = ensure_ascii
        self._separators = separators
        self._fp: Optional[TextIO] = None
        self._owns_fp = False
        self._first = True

    def __enter__(self) -> "JsonArrayWriter":
        if isinstance(self._target, (str, Path)):
            path = Path(self._target)
            path.parent.mkdir(parents=True, exist_ok=True)
            self._fp = path.open("w", encoding="utf-8")
            self._owns_fp = True
        else:
            self._fp = self._target
            self._owns_fp = False

        self._fp.write("[")
        return self

    def write(self, obj: Any) -> None:
        if self._fp is None:
            raise RuntimeError("JsonArrayWriter no está abierto (usa 'with').")

        if not self._first:
            self._fp.write(",")
        self._first = False
        self._fp.write(
            json.dumps(
                _sanitize_non_finite(obj),
                ensure_ascii=self._ensure_ascii,
                separators=self._separators,
                default=json_default,
                allow_nan=False,
            )
        )

    def write_many(self, objs: Iterable[Any]) -> None:
        for obj in objs:
            self.write(obj)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._fp is None:
            return
        self._fp.write("]")
        if self._owns_fp:
            self._fp.close()
        self._fp = None


def copy_file_text(src: str | Path, dst_fp: TextIO, chunk_size: int = 1_048_576) -> None:
    """Copia el contenido de un archivo a un fp destino sin cargar todo a memoria."""

    src = Path(src)
    with src.open("r", encoding="utf-8", errors="replace") as fp:
        while True:
            chunk = fp.read(chunk_size)
            if not chunk:
                break
            dst_fp.write(chunk)
