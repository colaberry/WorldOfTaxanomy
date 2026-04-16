"""Ingest Implant Classification Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_implant", "Implant Class", "Implant Classification Types", "1.0", "Global", "FDA")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IC", "Implant Classification Types", 1, None),
    ("IC.01", "Orthopedic Implant (Joint)", 2, 'IC'),
    ("IC.02", "Spinal Implant", 2, 'IC'),
    ("IC.03", "Cardiac Implant (Pacemaker)", 2, 'IC'),
    ("IC.04", "Cardiac Stent", 2, 'IC'),
    ("IC.05", "Cochlear Implant", 2, 'IC'),
    ("IC.06", "Dental Implant", 2, 'IC'),
    ("IC.07", "Breast Implant", 2, 'IC'),
    ("IC.08", "Intraocular Lens", 2, 'IC'),
    ("IC.09", "Neurostimulator", 2, 'IC'),
    ("IC.10", "Vascular Graft", 2, 'IC'),
    ("IC.11", "Bone Graft Substitute", 2, 'IC'),
    ("IC.12", "Hernia Mesh", 2, 'IC'),
    ("IC.13", "Drug-Eluting Implant", 2, 'IC'),
    ("IC.14", "Bioresorbable Implant", 2, 'IC'),
]

async def ingest_domain_implant(conn) -> int:
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
