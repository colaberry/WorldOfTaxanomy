"""Ingest Biobank Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_biobank", "Biobank Type", "Biobank Types", "1.0", "Global", "ISBER")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BB", "Biobank Types", 1, None),
    ("BB.01", "Population Biobank", 2, 'BB'),
    ("BB.02", "Disease-Specific Biobank", 2, 'BB'),
    ("BB.03", "Clinical Trial Biobank", 2, 'BB'),
    ("BB.04", "Tissue Bank", 2, 'BB'),
    ("BB.05", "Blood/Plasma Bank", 2, 'BB'),
    ("BB.06", "Cord Blood Bank", 2, 'BB'),
    ("BB.07", "DNA/RNA Biobank", 2, 'BB'),
    ("BB.08", "Microbiome Bank", 2, 'BB'),
    ("BB.09", "Virtual Biobank", 2, 'BB'),
    ("BB.10", "Agricultural Biobank (Seed)", 2, 'BB'),
    ("BB.11", "Biobank Quality Management", 2, 'BB'),
    ("BB.12", "Biobank Informatics (LIMS)", 2, 'BB'),
    ("BB.13", "Consent and Ethics Framework", 2, 'BB'),
]

async def ingest_domain_biobank(conn) -> int:
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
