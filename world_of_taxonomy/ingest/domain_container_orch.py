"""Ingest Container Orchestration Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_container_orch", "Container Orchestration", "Container Orchestration Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CO", "Container Orchestration Types", 1, None),
    ("CO.01", "Kubernetes (K8s)", 2, 'CO'),
    ("CO.02", "Docker Swarm", 2, 'CO'),
    ("CO.03", "Amazon ECS", 2, 'CO'),
    ("CO.04", "Azure Container Instances", 2, 'CO'),
    ("CO.05", "Google Cloud Run", 2, 'CO'),
    ("CO.06", "Nomad (HashiCorp)", 2, 'CO'),
    ("CO.07", "OpenShift (Red Hat)", 2, 'CO'),
    ("CO.08", "Rancher", 2, 'CO'),
    ("CO.09", "Helm Charts", 2, 'CO'),
    ("CO.10", "Service Mesh (Istio)", 2, 'CO'),
    ("CO.11", "Service Mesh (Linkerd)", 2, 'CO'),
    ("CO.12", "Sidecar Pattern", 2, 'CO'),
    ("CO.13", "Pod Security", 2, 'CO'),
    ("CO.14", "Container Registry", 2, 'CO'),
    ("CO.15", "Container Runtime (containerd)", 2, 'CO'),
]

async def ingest_domain_container_orch(conn) -> int:
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
