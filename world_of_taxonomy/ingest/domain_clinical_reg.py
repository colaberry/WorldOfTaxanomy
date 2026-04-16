"""Ingest Clinical Registry Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_clinical_reg", "Clinical Registry", "Clinical Registry Types", "1.0", "Global", "AHRQ")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("RG", "Clinical Registry Types", 1, None),
    ("RG.01", "Disease Registry", 2, 'RG'),
    ("RG.02", "Procedure Registry", 2, 'RG'),
    ("RG.03", "Device Registry", 2, 'RG'),
    ("RG.04", "Implant Registry", 2, 'RG'),
    ("RG.05", "Cancer Registry", 2, 'RG'),
    ("RG.06", "Trauma Registry", 2, 'RG'),
    ("RG.07", "Birth Defects Registry", 2, 'RG'),
    ("RG.08", "Transplant Registry", 2, 'RG'),
    ("RG.09", "Rare Disease Registry", 2, 'RG'),
    ("RG.10", "Quality Improvement Registry", 2, 'RG'),
    ("RG.11", "Patient-Powered Registry", 2, 'RG'),
    ("RG.12", "Pharmacovigilance Registry", 2, 'RG'),
    ("RG.13", "Post-Market Surveillance Registry", 2, 'RG'),
]

async def ingest_domain_clinical_reg(conn) -> int:
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
