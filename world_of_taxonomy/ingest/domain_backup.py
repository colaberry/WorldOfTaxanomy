"""Ingest Backup Strategy Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_backup", "Backup Strategy", "Backup Strategy Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BK", "Backup Strategy Types", 1, None),
    ("BK.01", "Full Backup", 2, 'BK'),
    ("BK.02", "Incremental Backup", 2, 'BK'),
    ("BK.03", "Differential Backup", 2, 'BK'),
    ("BK.04", "Continuous Data Protection", 2, 'BK'),
    ("BK.05", "Snapshot-Based Backup", 2, 'BK'),
    ("BK.06", "Cloud Backup (BaaS)", 2, 'BK'),
    ("BK.07", "Air-Gapped Backup", 2, 'BK'),
    ("BK.08", "Immutable Backup", 2, 'BK'),
    ("BK.09", "3-2-1 Backup Rule", 2, 'BK'),
    ("BK.10", "Deduplication", 2, 'BK'),
    ("BK.11", "Backup Encryption", 2, 'BK'),
    ("BK.12", "Backup Verification", 2, 'BK'),
    ("BK.13", "Retention Policy", 2, 'BK'),
    ("BK.14", "Bare Metal Recovery", 2, 'BK'),
]

async def ingest_domain_backup(conn) -> int:
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
