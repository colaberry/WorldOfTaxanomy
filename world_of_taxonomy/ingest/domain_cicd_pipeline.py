"""Ingest CI/CD Pipeline Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_cicd_pipeline", "CI/CD Pipeline", "CI/CD Pipeline Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CI", "CI/CD Pipeline Types", 1, None),
    ("CI.01", "Continuous Integration", 2, 'CI'),
    ("CI.02", "Continuous Delivery", 2, 'CI'),
    ("CI.03", "Continuous Deployment", 2, 'CI'),
    ("CI.04", "Build Pipeline", 2, 'CI'),
    ("CI.05", "Test Pipeline (Unit/Integration)", 2, 'CI'),
    ("CI.06", "Security Pipeline (SAST/DAST)", 2, 'CI'),
    ("CI.07", "Artifact Management", 2, 'CI'),
    ("CI.08", "Container Image Pipeline", 2, 'CI'),
    ("CI.09", "Infrastructure Pipeline (IaC)", 2, 'CI'),
    ("CI.10", "GitOps Pipeline", 2, 'CI'),
    ("CI.11", "Canary Deployment Pipeline", 2, 'CI'),
    ("CI.12", "Blue-Green Deployment", 2, 'CI'),
    ("CI.13", "Rolling Update Pipeline", 2, 'CI'),
    ("CI.14", "Multi-Environment Pipeline", 2, 'CI'),
    ("CI.15", "Compliance Gate Pipeline", 2, 'CI'),
    ("CI.16", "Monorepo Pipeline", 2, 'CI'),
]

async def ingest_domain_cicd_pipeline(conn) -> int:
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
