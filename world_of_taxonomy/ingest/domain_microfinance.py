"""Ingest Microfinance Product and Institution Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_microfinance",
    "Microfinance Types",
    "Microfinance Product and Institution Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("mf_prod", "Microfinance Products", 1, None),
    ("mf_inst", "Institution Types", 1, None),
    ("mf_meth", "Lending Methodologies", 1, None),
    ("mf_prod_micro", "Microcredit (individual and group loans)", 2, "mf_prod"),
    ("mf_prod_save", "Microsavings", 2, "mf_prod"),
    ("mf_prod_insur", "Microinsurance", 2, "mf_prod"),
    ("mf_prod_pay", "Mobile money and micropayments", 2, "mf_prod"),
    ("mf_prod_lease", "Micro-leasing and asset finance", 2, "mf_prod"),
    ("mf_inst_ngo", "NGO microfinance institution", 2, "mf_inst"),
    ("mf_inst_nbfi", "Non-bank financial institution", 2, "mf_inst"),
    ("mf_inst_coop", "Credit union and cooperative", 2, "mf_inst"),
    ("mf_inst_bank", "Microfinance bank (licensed)", 2, "mf_inst"),
    ("mf_inst_fin", "Fintech microfinance platform", 2, "mf_inst"),
    ("mf_meth_group", "Group lending (Grameen model)", 2, "mf_meth"),
    ("mf_meth_indiv", "Individual lending", 2, "mf_meth"),
    ("mf_meth_village", "Village banking", 2, "mf_meth"),
    ("mf_meth_self", "Self-help group lending", 2, "mf_meth"),
    ("mf_meth_agent", "Agent banking", 2, "mf_meth"),
]


async def ingest_domain_microfinance(conn) -> int:
    """Insert or update Microfinance Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_microfinance"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_microfinance", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_microfinance",
    )
    return count
