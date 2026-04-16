"""Ingest Wealth Management Service and Strategy Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_wealth_mgmt",
    "Wealth Management Types",
    "Wealth Management Service and Strategy Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("wm_svc", "Service Tiers", 1, None),
    ("wm_strat", "Investment Strategies", 1, None),
    ("wm_plan", "Planning Services", 1, None),
    ("wm_svc_mass", "Mass affluent (digital advisory)", 2, "wm_svc"),
    ("wm_svc_hnw", "High net worth (HNW) advisory", 2, "wm_svc"),
    ("wm_svc_uhnw", "Ultra-high net worth (UHNW) / family office", 2, "wm_svc"),
    ("wm_svc_inst", "Institutional advisory", 2, "wm_svc"),
    ("wm_strat_passive", "Passive / index investing", 2, "wm_strat"),
    ("wm_strat_active", "Active management", 2, "wm_strat"),
    ("wm_strat_alt", "Alternative investments", 2, "wm_strat"),
    ("wm_strat_esg", "ESG / impact investing", 2, "wm_strat"),
    ("wm_strat_quant", "Quantitative / systematic strategies", 2, "wm_strat"),
    ("wm_plan_estate", "Estate planning", 2, "wm_plan"),
    ("wm_plan_tax", "Tax optimization", 2, "wm_plan"),
    ("wm_plan_retire", "Retirement planning", 2, "wm_plan"),
    ("wm_plan_phil", "Philanthropic advisory", 2, "wm_plan"),
    ("wm_plan_trust", "Trust services", 2, "wm_plan"),
]


async def ingest_domain_wealth_mgmt(conn) -> int:
    """Insert or update Wealth Management Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_wealth_mgmt"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_wealth_mgmt", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_wealth_mgmt",
    )
    return count
