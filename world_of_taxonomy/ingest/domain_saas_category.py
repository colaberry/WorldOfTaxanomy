"""Ingest Software as a Service Category Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_saas_category",
    "SaaS Category Types",
    "Software as a Service Category Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("saas_biz", "Business SaaS", 1, None),
    ("saas_dev", "Developer SaaS", 1, None),
    ("saas_vert", "Vertical SaaS", 1, None),
    ("saas_biz_crm", "CRM (Salesforce, HubSpot)", 2, "saas_biz"),
    ("saas_biz_erp", "ERP (NetSuite, SAP)", 2, "saas_biz"),
    ("saas_biz_hr", "HCM/HRIS (Workday, BambooHR)", 2, "saas_biz"),
    ("saas_biz_pm", "Project management (Asana, Monday)", 2, "saas_biz"),
    ("saas_biz_collab", "Collaboration (Slack, Teams)", 2, "saas_biz"),
    ("saas_biz_bi", "BI and analytics (Tableau, Looker)", 2, "saas_biz"),
    ("saas_dev_ci", "CI/CD platforms (GitHub Actions, CircleCI)", 2, "saas_dev"),
    ("saas_dev_monitor", "APM and monitoring (Datadog, New Relic)", 2, "saas_dev"),
    ("saas_dev_log", "Log management (Splunk, Elastic)", 2, "saas_dev"),
    ("saas_dev_sec", "Security SaaS (Snyk, CrowdStrike)", 2, "saas_dev"),
    ("saas_vert_health", "Healthcare SaaS (Epic, athenahealth)", 2, "saas_vert"),
    ("saas_vert_legal", "Legal SaaS (Clio, LegalZoom)", 2, "saas_vert"),
    ("saas_vert_realestate", "Real estate SaaS (Yardi, AppFolio)", 2, "saas_vert"),
    ("saas_vert_constr", "Construction SaaS (Procore, PlanGrid)", 2, "saas_vert"),
    ("saas_vert_edtech", "EdTech SaaS (Canvas, Blackboard)", 2, "saas_vert"),
]


async def ingest_domain_saas_category(conn) -> int:
    """Insert or update SaaS Category Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_saas_category"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_saas_category", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_saas_category",
    )
    return count
