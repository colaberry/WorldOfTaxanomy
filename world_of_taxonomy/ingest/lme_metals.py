"""Ingest London Metal Exchange Traded Metals."""
from __future__ import annotations

_SYSTEM_ROW = ("lme_metals", "LME Metals", "London Metal Exchange Traded Metals", "2024", "Global", "LME")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("LM", "LME Categories", 1, None),
    ("LM.01", "Copper (Grade A)", 2, 'LM'),
    ("LM.02", "Aluminium (Primary)", 2, 'LM'),
    ("LM.03", "Zinc (SHG)", 2, 'LM'),
    ("LM.04", "Nickel (Primary)", 2, 'LM'),
    ("LM.05", "Lead (Standard)", 2, 'LM'),
    ("LM.06", "Tin", 2, 'LM'),
    ("LM.07", "Aluminium Alloy", 2, 'LM'),
    ("LM.08", "NASAAC", 2, 'LM'),
    ("LM.09", "Cobalt", 2, 'LM'),
    ("LM.10", "Molybdenum", 2, 'LM'),
    ("LM.11", "Steel Scrap", 2, 'LM'),
    ("LM.12", "Steel Rebar", 2, 'LM'),
    ("LM.13", "Steel HRC", 2, 'LM'),
    ("LM.14", "Lithium Hydroxide", 2, 'LM'),
]

async def ingest_lme_metals(conn) -> int:
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
