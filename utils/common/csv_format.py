"""CSV encode/decode helpers shared by clients and servers."""

from __future__ import annotations

import csv
import io
from pathlib import Path


def read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file():
        raise FileNotFoundError(f"Input CSV not found: {path}")

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"Input CSV has no header: {path}")
        rows = list(reader)

    if not rows:
        raise ValueError(f"Input CSV has no data rows: {path}")
    return list(reader.fieldnames), rows


def encode_record(
    row: dict[str, str],
    fieldnames: list[str],
    extra: dict[str, str] | None = None,
) -> str:
    extra = extra or {}
    columns = [*fieldnames, *extra.keys()]
    payload = {**row, **extra}
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns)
    writer.writeheader()
    writer.writerow(payload)
    return buf.getvalue().rstrip("\r\n")


def decode_record(raw: str) -> tuple[list[str], dict[str, str]]:
    reader = csv.DictReader(io.StringIO(raw))
    if reader.fieldnames is None:
        raise ValueError("CSV record has no header")
    row = next(reader, None)
    if row is None:
        raise ValueError("CSV record has no data row")
    return list(reader.fieldnames), row


def append_csv_message(handle, raw: str, header_written: bool) -> bool:
    rows = list(csv.reader(io.StringIO(raw)))
    if not rows:
        return header_written
    writer = csv.writer(handle)
    if header_written:
        writer.writerow(rows[-1])
    else:
        writer.writerows(rows)
        header_written = True
    handle.flush()
    return header_written
