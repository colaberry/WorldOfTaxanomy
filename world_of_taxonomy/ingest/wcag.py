"""Ingest Web Content Accessibility Guidelines."""
from __future__ import annotations

_SYSTEM_ROW = ("wcag", "WCAG", "Web Content Accessibility Guidelines", "2.2", "Global", "W3C/WAI")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "W3C Software License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("WC", "WCAG Principles", 1, None),
    ("WC.01", "Perceivable: Text Alternatives", 2, 'WC'),
    ("WC.02", "Perceivable: Time-Based Media", 2, 'WC'),
    ("WC.03", "Perceivable: Adaptable", 2, 'WC'),
    ("WC.04", "Perceivable: Distinguishable", 2, 'WC'),
    ("WC.05", "Operable: Keyboard Accessible", 2, 'WC'),
    ("WC.06", "Operable: Enough Time", 2, 'WC'),
    ("WC.07", "Operable: Seizures and Physical Reactions", 2, 'WC'),
    ("WC.08", "Operable: Navigable", 2, 'WC'),
    ("WC.09", "Operable: Input Modalities", 2, 'WC'),
    ("WC.10", "Understandable: Readable", 2, 'WC'),
    ("WC.11", "Understandable: Predictable", 2, 'WC'),
    ("WC.12", "Understandable: Input Assistance", 2, 'WC'),
    ("WC.13", "Robust: Compatible", 2, 'WC'),
    ("WC.14", "Conformance Level A", 2, 'WC'),
    ("WC.15", "Conformance Level AA", 2, 'WC'),
    ("WC.16", "Conformance Level AAA", 2, 'WC'),
]

async def ingest_wcag(conn) -> int:
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
