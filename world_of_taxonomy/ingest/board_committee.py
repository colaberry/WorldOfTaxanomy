"""Ingest Corporate Board Committee Types."""
from __future__ import annotations

_SYSTEM_ROW = ("board_committee", "Board Committees", "Corporate Board Committee Types", "2024", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BC", "Board Committees", 1, None),
    ("BC.01", "Audit Committee", 2, 'BC'),
    ("BC.02", "Compensation/Remuneration Committee", 2, 'BC'),
    ("BC.03", "Nominating/Governance Committee", 2, 'BC'),
    ("BC.04", "Risk Committee", 2, 'BC'),
    ("BC.05", "Technology/Cyber Committee", 2, 'BC'),
    ("BC.06", "ESG/Sustainability Committee", 2, 'BC'),
    ("BC.07", "Finance Committee", 2, 'BC'),
    ("BC.08", "Strategy Committee", 2, 'BC'),
    ("BC.09", "Compliance/Ethics Committee", 2, 'BC'),
    ("BC.10", "Human Capital Committee", 2, 'BC'),
    ("BC.11", "Executive Committee", 2, 'BC'),
    ("BC.12", "Investment Committee", 2, 'BC'),
    ("BC.13", "Special/Ad Hoc Committee", 2, 'BC'),
]

async def ingest_board_committee(conn) -> int:
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
