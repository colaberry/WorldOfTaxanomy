"""Ingest Medical Gas Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_med_gas", "Medical Gas", "Medical Gas Types", "1.0", "Global", "FDA/ISO")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("MG", "Medical Gas Types", 1, None),
    ("MG.01", "Medical Oxygen", 2, 'MG'),
    ("MG.02", "Nitrous Oxide", 2, 'MG'),
    ("MG.03", "Medical Air", 2, 'MG'),
    ("MG.04", "Carbon Dioxide (Medical)", 2, 'MG'),
    ("MG.05", "Helium-Oxygen Mixture", 2, 'MG'),
    ("MG.06", "Nitrogen (Medical)", 2, 'MG'),
    ("MG.07", "Nitric Oxide (Therapeutic)", 2, 'MG'),
    ("MG.08", "Xenon (Anesthetic)", 2, 'MG'),
    ("MG.09", "Medical Vacuum", 2, 'MG'),
    ("MG.10", "Surgical Instrument Air", 2, 'MG'),
    ("MG.11", "Waste Anesthetic Gas", 2, 'MG'),
    ("MG.12", "Gas Cylinder Management", 2, 'MG'),
    ("MG.13", "Pipeline Distribution System", 2, 'MG'),
]

async def ingest_domain_med_gas(conn) -> int:
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
