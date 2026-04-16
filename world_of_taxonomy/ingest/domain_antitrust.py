"""Ingest Antitrust Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_antitrust", "Antitrust Type", "Antitrust Types", "1.0", "Global", "DOJ/FTC/EU")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AT", "Antitrust Types", 1, None),
    ("AT.01", "Horizontal Price Fixing", 2, 'AT'),
    ("AT.02", "Bid Rigging", 2, 'AT'),
    ("AT.03", "Market Allocation", 2, 'AT'),
    ("AT.04", "Monopolization (Sherman Act)", 2, 'AT'),
    ("AT.05", "Attempted Monopolization", 2, 'AT'),
    ("AT.06", "Tying Arrangement", 2, 'AT'),
    ("AT.07", "Exclusive Dealing", 2, 'AT'),
    ("AT.08", "Merger Review (Hart-Scott-Rodino)", 2, 'AT'),
    ("AT.09", "Vertical Restraint", 2, 'AT'),
    ("AT.10", "Refusal to Deal", 2, 'AT'),
    ("AT.11", "Abuse of Dominance (EU Art 102)", 2, 'AT'),
    ("AT.12", "Cartel Leniency Program", 2, 'AT'),
    ("AT.13", "Digital Platform Antitrust", 2, 'AT'),
]

async def ingest_domain_antitrust(conn) -> int:
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
