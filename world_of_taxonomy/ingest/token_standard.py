"""Ingest Blockchain Token Standard Types."""
from __future__ import annotations

_SYSTEM_ROW = ("token_standard", "Token Standards", "Blockchain Token Standard Types", "2024", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("TS", "Token Standards", 1, None),
    ("TS.01", "ERC-20 (Fungible Token)", 2, 'TS'),
    ("TS.02", "ERC-721 (NFT)", 2, 'TS'),
    ("TS.03", "ERC-1155 (Multi-Token)", 2, 'TS'),
    ("TS.04", "ERC-4626 (Tokenized Vault)", 2, 'TS'),
    ("TS.05", "BEP-20 (BNB Chain)", 2, 'TS'),
    ("TS.06", "SPL Token (Solana)", 2, 'TS'),
    ("TS.07", "TRC-20 (Tron)", 2, 'TS'),
    ("TS.08", "FA2 (Tezos)", 2, 'TS'),
    ("TS.09", "ASA (Algorand)", 2, 'TS'),
    ("TS.10", "CW-20 (CosmWasm)", 2, 'TS'),
    ("TS.11", "Security Token (STO)", 2, 'TS'),
    ("TS.12", "Soulbound Token (SBT)", 2, 'TS'),
    ("TS.13", "Wrapped Token", 2, 'TS'),
    ("TS.14", "Stablecoin Standard", 2, 'TS'),
]

async def ingest_token_standard(conn) -> int:
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
