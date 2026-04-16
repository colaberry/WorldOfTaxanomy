"""Ingest Cloud Computing Service and Deployment Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_cloud_service",
    "Cloud Service Model Types",
    "Cloud Computing Service and Deployment Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cld_model", "Service Models", 1, None),
    ("cld_deploy", "Deployment Models", 1, None),
    ("cld_cat", "Service Categories", 1, None),
    ("cld_model_iaas", "IaaS (Infrastructure as a Service)", 2, "cld_model"),
    ("cld_model_paas", "PaaS (Platform as a Service)", 2, "cld_model"),
    ("cld_model_saas", "SaaS (Software as a Service)", 2, "cld_model"),
    ("cld_model_faas", "FaaS (Function as a Service / Serverless)", 2, "cld_model"),
    ("cld_model_caas", "CaaS (Container as a Service)", 2, "cld_model"),
    ("cld_deploy_pub", "Public cloud", 2, "cld_deploy"),
    ("cld_deploy_priv", "Private cloud (on-premises, hosted)", 2, "cld_deploy"),
    ("cld_deploy_hyb", "Hybrid cloud", 2, "cld_deploy"),
    ("cld_deploy_multi", "Multi-cloud", 2, "cld_deploy"),
    ("cld_deploy_edge", "Edge cloud", 2, "cld_deploy"),
    ("cld_cat_comp", "Compute services (VMs, containers, serverless)", 2, "cld_cat"),
    ("cld_cat_stor", "Storage services (object, block, file)", 2, "cld_cat"),
    ("cld_cat_net", "Networking services (CDN, DNS, VPC)", 2, "cld_cat"),
    ("cld_cat_db", "Database services (managed, serverless)", 2, "cld_cat"),
    ("cld_cat_ai", "AI/ML services (training, inference)", 2, "cld_cat"),
    ("cld_cat_sec", "Security services (IAM, KMS, WAF)", 2, "cld_cat"),
]


async def ingest_domain_cloud_service(conn) -> int:
    """Insert or update Cloud Service Model Types system and its nodes. Returns node count."""
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count,
                source_url, data_provenance, license)
           VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)
           ON CONFLICT (id) DO UPDATE
               SET node_count = 0, source_url = EXCLUDED.source_url,
                   data_provenance = EXCLUDED.data_provenance, license = EXCLUDED.license""",
        *_SYSTEM_ROW, _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute(
        "DELETE FROM classification_node WHERE system_id = $1", "domain_cloud_service"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_cloud_service", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_cloud_service",
    )
    return count
