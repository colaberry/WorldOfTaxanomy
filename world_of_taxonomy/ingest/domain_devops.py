"""Ingest DevOps Practice and Methodology Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_devops",
    "DevOps Practice Types",
    "DevOps Practice and Methodology Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("dop_ci", "Continuous Integration", 1, None),
    ("dop_cd", "Continuous Delivery/Deployment", 1, None),
    ("dop_ops", "Operations Practices", 1, None),
    ("dop_culture", "Culture and Methodology", 1, None),
    ("dop_ci_build", "Build automation", 2, "dop_ci"),
    ("dop_ci_test", "Automated testing (unit, integration, e2e)", 2, "dop_ci"),
    ("dop_ci_lint", "Code quality and linting", 2, "dop_ci"),
    ("dop_ci_scan", "Security scanning (SAST, SCA)", 2, "dop_ci"),
    ("dop_cd_pipe", "Deployment pipelines", 2, "dop_cd"),
    ("dop_cd_blue", "Blue-green deployment", 2, "dop_cd"),
    ("dop_cd_canary", "Canary releases", 2, "dop_cd"),
    ("dop_cd_feature", "Feature flags and toggles", 2, "dop_cd"),
    ("dop_cd_gitops", "GitOps", 2, "dop_cd"),
    ("dop_ops_iac", "Infrastructure as Code (IaC)", 2, "dop_ops"),
    ("dop_ops_monitor", "Monitoring and observability", 2, "dop_ops"),
    ("dop_ops_incident", "Incident management and on-call", 2, "dop_ops"),
    ("dop_ops_chaos", "Chaos engineering", 2, "dop_ops"),
    ("dop_culture_sre", "Site reliability engineering (SRE)", 2, "dop_culture"),
    ("dop_culture_plat", "Platform engineering", 2, "dop_culture"),
    ("dop_culture_devsecops", "DevSecOps", 2, "dop_culture"),
]


async def ingest_domain_devops(conn) -> int:
    """Insert or update DevOps Practice Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_devops"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_devops", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_devops",
    )
    return count
