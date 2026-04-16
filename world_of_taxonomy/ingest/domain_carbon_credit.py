"""Ingest Carbon Credit and Offset Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_carbon_credit",
    "Carbon Credit Types",
    "Carbon Credit and Offset Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cc_type", "Credit Types", 1, None),
    ("cc_mkt", "Market Types", 1, None),
    ("cc_proj", "Project Categories", 1, None),
    ("cc_type_ver", "Verified carbon units (VCU)", 2, "cc_type"),
    ("cc_type_cer", "Certified emission reductions (CER)", 2, "cc_type"),
    ("cc_type_eru", "Emission reduction units (ERU)", 2, "cc_type"),
    ("cc_type_redd", "REDD+ credits", 2, "cc_type"),
    ("cc_type_rem", "Carbon removal credits (CDR)", 2, "cc_type"),
    ("cc_mkt_comp", "Compliance market (EU ETS, CA cap-and-trade)", 2, "cc_mkt"),
    ("cc_mkt_vol", "Voluntary carbon market", 2, "cc_mkt"),
    ("cc_mkt_art6", "Article 6 (Paris Agreement) market", 2, "cc_mkt"),
    ("cc_proj_forest", "Forestry and land use (REDD+, A/R)", 2, "cc_proj"),
    ("cc_proj_renew", "Renewable energy projects", 2, "cc_proj"),
    ("cc_proj_cook", "Clean cookstoves", 2, "cc_proj"),
    ("cc_proj_meth", "Methane capture and avoidance", 2, "cc_proj"),
    ("cc_proj_dac", "Direct air capture (DAC)", 2, "cc_proj"),
    ("cc_proj_bio", "Biochar and enhanced weathering", 2, "cc_proj"),
    ("cc_proj_blue", "Blue carbon (mangroves, seagrass)", 2, "cc_proj"),
]


async def ingest_domain_carbon_credit(conn) -> int:
    """Insert or update Carbon Credit Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_carbon_credit"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_carbon_credit", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_carbon_credit",
    )
    return count
