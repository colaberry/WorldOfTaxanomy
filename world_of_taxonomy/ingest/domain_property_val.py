"""Ingest Real Estate Property Valuation Method Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_property_val",
    "Property Valuation Types",
    "Real Estate Property Valuation Method Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pv_approach", "Valuation Approaches", 1, None),
    ("pv_purpose", "Valuation Purposes", 1, None),
    ("pv_tech", "Valuation Technology", 1, None),
    ("pv_approach_sales", "Sales comparison approach", 2, "pv_approach"),
    ("pv_approach_cost", "Cost approach (replacement, reproduction)", 2, "pv_approach"),
    ("pv_approach_income", "Income capitalization approach", 2, "pv_approach"),
    ("pv_approach_dcf", "Discounted cash flow (DCF)", 2, "pv_approach"),
    ("pv_purpose_market", "Market value appraisal", 2, "pv_purpose"),
    ("pv_purpose_tax", "Tax assessment valuation", 2, "pv_purpose"),
    ("pv_purpose_insur", "Insurance replacement value", 2, "pv_purpose"),
    ("pv_purpose_invest", "Investment value analysis", 2, "pv_purpose"),
    ("pv_purpose_liqu", "Liquidation value", 2, "pv_purpose"),
    ("pv_tech_avm", "Automated valuation model (AVM)", 2, "pv_tech"),
    ("pv_tech_bpo", "Broker price opinion (BPO)", 2, "pv_tech"),
    ("pv_tech_desktop", "Desktop appraisal", 2, "pv_tech"),
    ("pv_tech_hybrid", "Hybrid appraisal (desktop + inspection)", 2, "pv_tech"),
]


async def ingest_domain_property_val(conn) -> int:
    """Insert or update Property Valuation Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_property_val"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_property_val", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_property_val",
    )
    return count
