"""Ingest Decentralized Finance Protocol Types."""
from __future__ import annotations

_SYSTEM_ROW = ("defi_protocol", "DeFi Protocols", "Decentralized Finance Protocol Types", "2024", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DF", "DeFi Protocol Types", 1, None),
    ("DF.01", "Decentralized Exchange (DEX)", 2, 'DF'),
    ("DF.02", "Lending/Borrowing Protocol", 2, 'DF'),
    ("DF.03", "Yield Aggregator", 2, 'DF'),
    ("DF.04", "Stablecoin Protocol", 2, 'DF'),
    ("DF.05", "Liquid Staking", 2, 'DF'),
    ("DF.06", "Derivatives Protocol", 2, 'DF'),
    ("DF.07", "Insurance Protocol", 2, 'DF'),
    ("DF.08", "Oracle Network", 2, 'DF'),
    ("DF.09", "Bridge/Cross-Chain", 2, 'DF'),
    ("DF.10", "DAO Governance", 2, 'DF'),
    ("DF.11", "Launchpad/IDO", 2, 'DF'),
    ("DF.12", "Real World Assets (RWA)", 2, 'DF'),
    ("DF.13", "Perpetual Protocol", 2, 'DF'),
    ("DF.14", "Restaking Protocol", 2, 'DF'),
]

async def ingest_defi_protocol(conn) -> int:
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
