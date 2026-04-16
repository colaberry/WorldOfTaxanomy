"""Ingest HTTP Response Status Code Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("http_status", "HTTP Status Codes", "HTTP Response Status Code Categories", "2024", "Global", "IETF")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("HS", "HTTP Status Code Classes", 1, None),
    ("HS.1xx", "1xx Informational", 2, 'HS'),
    ("HS.2xx", "2xx Success", 2, 'HS'),
    ("HS.3xx", "3xx Redirection", 2, 'HS'),
    ("HS.4xx", "4xx Client Error", 2, 'HS'),
    ("HS.5xx", "5xx Server Error", 2, 'HS'),
    ("HS.200", "200 OK", 2, 'HS'),
    ("HS.201", "201 Created", 2, 'HS'),
    ("HS.301", "301 Moved Permanently", 2, 'HS'),
    ("HS.302", "302 Found", 2, 'HS'),
    ("HS.400", "400 Bad Request", 2, 'HS'),
    ("HS.401", "401 Unauthorized", 2, 'HS'),
    ("HS.403", "403 Forbidden", 2, 'HS'),
    ("HS.404", "404 Not Found", 2, 'HS'),
    ("HS.500", "500 Internal Server Error", 2, 'HS'),
    ("HS.502", "502 Bad Gateway", 2, 'HS'),
    ("HS.503", "503 Service Unavailable", 2, 'HS'),
]

async def ingest_http_status(conn) -> int:
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
