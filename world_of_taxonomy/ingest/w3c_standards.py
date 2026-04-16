"""Ingest W3C Web Standards."""
from __future__ import annotations

_SYSTEM_ROW = ("w3c_standards", "W3C Standards", "W3C Web Standards", "2024", "Global", "W3C")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "W3C License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("W3C", "W3C Standard Groups", 1, None),
    ("W3C.HTML", "HTML and CSS", 2, 'W3C'),
    ("W3C.DOM", "DOM and Events", 2, 'W3C'),
    ("W3C.SVG", "Graphics (SVG, Canvas)", 2, 'W3C'),
    ("W3C.A11Y", "Accessibility (WCAG, WAI-ARIA)", 2, 'W3C'),
    ("W3C.XML", "XML Technologies", 2, 'W3C'),
    ("W3C.SEC", "Web Security", 2, 'W3C'),
    ("W3C.API", "Web APIs", 2, 'W3C'),
    ("W3C.WEB", "WebRTC and Communications", 2, 'W3C'),
    ("W3C.SEM", "Semantic Web (RDF, OWL)", 2, 'W3C'),
    ("W3C.WS", "Web Services", 2, 'W3C'),
    ("W3C.PAY", "Web Payments", 2, 'W3C'),
    ("W3C.VC", "Verifiable Credentials", 2, 'W3C'),
    ("W3C.DID", "Decentralized Identifiers", 2, 'W3C'),
    ("W3C.PER", "Web Performance", 2, 'W3C'),
    ("W3C.PWA", "Progressive Web Apps", 2, 'W3C'),
]

async def ingest_w3c_standards(conn) -> int:
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
