"""Ingest Periodic Table Element Groups."""
from __future__ import annotations

_SYSTEM_ROW = ("periodic_table", "Periodic Table", "Periodic Table Element Groups", "2024", "Global", "IUPAC")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PT", "Element Groups", 1, None),
    ("PT.01", "Alkali Metals", 2, 'PT'),
    ("PT.02", "Alkaline Earth Metals", 2, 'PT'),
    ("PT.03", "Transition Metals", 2, 'PT'),
    ("PT.04", "Post-Transition Metals", 2, 'PT'),
    ("PT.05", "Metalloids", 2, 'PT'),
    ("PT.06", "Reactive Nonmetals", 2, 'PT'),
    ("PT.07", "Noble Gases", 2, 'PT'),
    ("PT.08", "Lanthanides", 2, 'PT'),
    ("PT.09", "Actinides", 2, 'PT'),
    ("PT.10", "Unknown Properties (Super-heavy)", 2, 'PT'),
    ("PT.11", "Period 1 (H, He)", 2, 'PT'),
    ("PT.12", "Period 2 (Li-Ne)", 2, 'PT'),
    ("PT.13", "Period 3 (Na-Ar)", 2, 'PT'),
    ("PT.14", "Period 4 (K-Kr)", 2, 'PT'),
    ("PT.15", "Period 5 (Rb-Xe)", 2, 'PT'),
    ("PT.16", "Period 6 (Cs-Rn)", 2, 'PT'),
    ("PT.17", "Period 7 (Fr-Og)", 2, 'PT'),
]

async def ingest_periodic_table(conn) -> int:
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
