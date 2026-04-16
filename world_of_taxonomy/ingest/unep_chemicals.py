"""Ingest UNEP Chemical Classification Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("unep_chemicals", "UNEP Chemicals", "UNEP Chemical Classification Categories", "2024", "Global", "UNEP")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("UC", "UNEP Chemical Categories", 1, None),
    ("UC.01", "Persistent Organic Pollutants (POPs)", 2, 'UC'),
    ("UC.02", "Heavy Metals", 2, 'UC'),
    ("UC.03", "Ozone-Depleting Substances", 2, 'UC'),
    ("UC.04", "Greenhouse Gases", 2, 'UC'),
    ("UC.05", "Endocrine Disruptors", 2, 'UC'),
    ("UC.06", "PFAS (Forever Chemicals)", 2, 'UC'),
    ("UC.07", "Pesticides", 2, 'UC'),
    ("UC.08", "Industrial Chemicals", 2, 'UC'),
    ("UC.09", "Pharmaceutical Residues", 2, 'UC'),
    ("UC.10", "Microplastics", 2, 'UC'),
    ("UC.11", "Hazardous Waste Chemicals", 2, 'UC'),
    ("UC.12", "Nanomaterials", 2, 'UC'),
    ("UC.13", "Radioactive Materials", 2, 'UC'),
    ("UC.14", "Strategic Approach (SAICM)", 2, 'UC'),
]

async def ingest_unep_chemicals(conn) -> int:
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
