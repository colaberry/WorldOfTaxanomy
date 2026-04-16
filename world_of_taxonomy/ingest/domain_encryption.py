"""Ingest Encryption Standard Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_encryption", "Encryption Standard", "Encryption Standard Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EN", "Encryption Standard Types", 1, None),
    ("EN.01", "AES (Symmetric)", 2, 'EN'),
    ("EN.02", "RSA (Asymmetric)", 2, 'EN'),
    ("EN.03", "Elliptic Curve (ECC)", 2, 'EN'),
    ("EN.04", "ChaCha20-Poly1305", 2, 'EN'),
    ("EN.05", "TLS 1.3", 2, 'EN'),
    ("EN.06", "Post-Quantum Cryptography", 2, 'EN'),
    ("EN.07", "Format-Preserving Encryption", 2, 'EN'),
    ("EN.08", "Tokenization", 2, 'EN'),
    ("EN.09", "Envelope Encryption", 2, 'EN'),
    ("EN.10", "Database Encryption (TDE)", 2, 'EN'),
    ("EN.11", "File-Level Encryption", 2, 'EN'),
    ("EN.12", "Full Disk Encryption", 2, 'EN'),
    ("EN.13", "End-to-End Encryption", 2, 'EN'),
    ("EN.14", "Searchable Encryption", 2, 'EN'),
    ("EN.15", "Attribute-Based Encryption", 2, 'EN'),
]

async def ingest_domain_encryption(conn) -> int:
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
