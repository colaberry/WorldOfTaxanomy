"""Ingest Version Control Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_version_control", "Version Control", "Version Control Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("VC", "Version Control Types", 1, None),
    ("VC.01", "Git (Distributed)", 2, 'VC'),
    ("VC.02", "Subversion (Centralized)", 2, 'VC'),
    ("VC.03", "Mercurial (Distributed)", 2, 'VC'),
    ("VC.04", "Monorepo Strategy", 2, 'VC'),
    ("VC.05", "Polyrepo Strategy", 2, 'VC'),
    ("VC.06", "Git Flow Branching", 2, 'VC'),
    ("VC.07", "Trunk-Based Development", 2, 'VC'),
    ("VC.08", "GitHub Flow", 2, 'VC'),
    ("VC.09", "GitLab Flow", 2, 'VC'),
    ("VC.10", "Release Branching", 2, 'VC'),
    ("VC.11", "Feature Flag Strategy", 2, 'VC'),
    ("VC.12", "Semantic Versioning", 2, 'VC'),
    ("VC.13", "Calendar Versioning", 2, 'VC'),
    ("VC.14", "Large File Storage (LFS)", 2, 'VC'),
]

async def ingest_domain_version_control(conn) -> int:
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
