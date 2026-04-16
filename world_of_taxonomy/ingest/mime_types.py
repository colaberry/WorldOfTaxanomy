"""Ingest IANA Media Type Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("mime_types", "MIME Types", "IANA Media Type Categories", "2024", "Global", "IANA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("MM", "MIME Top-Level Types", 1, None),
    ("MM.01", "application/*", 2, 'MM'),
    ("MM.02", "audio/*", 2, 'MM'),
    ("MM.03", "font/*", 2, 'MM'),
    ("MM.04", "image/*", 2, 'MM'),
    ("MM.05", "message/*", 2, 'MM'),
    ("MM.06", "model/*", 2, 'MM'),
    ("MM.07", "multipart/*", 2, 'MM'),
    ("MM.08", "text/*", 2, 'MM'),
    ("MM.09", "video/*", 2, 'MM'),
    ("MM.10", "application/json", 2, 'MM'),
    ("MM.11", "application/xml", 2, 'MM'),
    ("MM.12", "application/pdf", 2, 'MM'),
    ("MM.13", "application/octet-stream", 2, 'MM'),
    ("MM.14", "text/html", 2, 'MM'),
    ("MM.15", "text/plain", 2, 'MM'),
]

async def ingest_mime_types(conn) -> int:
    sid, short, full, ver, region, authority = _SYSTEM_ROW
    await conn.execute(
        """INSERT INTO classification_system (id, name, full_name, version, region, authority,
                  source_url, source_date, data_provenance, license)
           VALUES ($1,$2,$3,$4,$5,$6,$7,CURRENT_DATE,$8,$9)
           ON CONFLICT (id) DO UPDATE SET name=$2,full_name=$3,version=$4,region=$5,
                  authority=$6,source_url=$7,source_date=CURRENT_DATE,
                  data_provenance=$8,license=$9""",
        sid, short, full, ver, region, authority,
        _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute("DELETE FROM classification_node WHERE system_id = $1", sid)
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1,$2,$3,$4,$5)""",
            sid, code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, sid)
    return count
