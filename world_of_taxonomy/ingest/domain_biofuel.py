"""Ingest Biofuel Generation Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_biofuel", "Biofuel Generation", "Biofuel Generation Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BF", "Biofuel Generation Types", 1, None),
    ("BF.01", "First-Generation (Food Crop)", 2, 'BF'),
    ("BF.02", "Second-Generation (Cellulosic)", 2, 'BF'),
    ("BF.03", "Third-Generation (Algae)", 2, 'BF'),
    ("BF.04", "Fourth-Generation (Synthetic)", 2, 'BF'),
    ("BF.05", "Bioethanol", 2, 'BF'),
    ("BF.06", "Biodiesel (FAME)", 2, 'BF'),
    ("BF.07", "Renewable Diesel (HVO)", 2, 'BF'),
    ("BF.08", "Sustainable Aviation Fuel (SAF)", 2, 'BF'),
    ("BF.09", "Biogas and Biomethane", 2, 'BF'),
    ("BF.10", "Bio-Butanol", 2, 'BF'),
    ("BF.11", "Fischer-Tropsch Fuel", 2, 'BF'),
    ("BF.12", "Pyrolysis Oil", 2, 'BF'),
    ("BF.13", "Waste-to-Fuel", 2, 'BF'),
]

async def ingest_domain_biofuel(conn) -> int:
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
