"""Ingest EPA RCRA Hazardous Waste Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("epa_rcra_waste", "EPA RCRA Waste", "EPA RCRA Hazardous Waste Categories", "2024", "United States", "US EPA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("RW", "RCRA Waste Categories", 1, None),
    ("RW.01", "F-Listed Wastes (Non-Specific Source)", 2, 'RW'),
    ("RW.02", "K-Listed Wastes (Specific Source)", 2, 'RW'),
    ("RW.03", "P-Listed Wastes (Acutely Hazardous)", 2, 'RW'),
    ("RW.04", "U-Listed Wastes (Toxic)", 2, 'RW'),
    ("RW.05", "D-Characteristic Ignitability (D001)", 2, 'RW'),
    ("RW.06", "D-Characteristic Corrosivity (D002)", 2, 'RW'),
    ("RW.07", "D-Characteristic Reactivity (D003)", 2, 'RW'),
    ("RW.08", "D-Characteristic Toxicity (D004-D043)", 2, 'RW'),
    ("RW.09", "Mixed Waste (Radioactive + Hazardous)", 2, 'RW'),
    ("RW.10", "Universal Waste", 2, 'RW'),
    ("RW.11", "Used Oil", 2, 'RW'),
    ("RW.12", "E-Waste", 2, 'RW'),
    ("RW.13", "Pharmaceutical Waste", 2, 'RW'),
    ("RW.14", "Conditionally Exempt SQG Waste", 2, 'RW'),
]

async def ingest_epa_rcra_waste(conn) -> int:
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
