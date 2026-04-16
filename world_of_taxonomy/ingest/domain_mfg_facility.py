"""Manufacturing Facility and Production Configuration Types domain taxonomy ingester.

Classifies manufacturing facilities by their production configuration.
Orthogonal to industry vertical, supply chain model and quality standard.
Based on APICS/ASCM manufacturing strategy framework and
Schroeder Operations Management typology.
Used by facility planners, industrial engineers and real estate developers
to match facility type to production requirements.

Code prefix: dmfgfac_
System ID: domain_mfg_facility
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
MFG_FACILITY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dmfgfac_jobshop", "Job Shop Production", 1, None),
    ("dmfgfac_jobshop_general", "General job shop: low volume, high variety, custom orders", 2, "dmfgfac_jobshop"),
    ("dmfgfac_jobshop_special", "Specialized job shop: niche, high-complexity parts (aerospace, defense)", 2, "dmfgfac_jobshop"),
    ("dmfgfac_batch", "Batch Production", 1, None),
    ("dmfgfac_batch_small", "Small batch: 10-500 units, high setup, moderate variety", 2, "dmfgfac_batch"),
    ("dmfgfac_batch_large", "Large batch: 500-5000 units, lower setup cost, reduced variety", 2, "dmfgfac_batch"),
    ("dmfgfac_flow", "Flow and Assembly Line Production", 1, None),
    ("dmfgfac_flow_straight", "Straight-line assembly (automotive, appliances, electronics)", 2, "dmfgfac_flow"),
    ("dmfgfac_flow_uflex", "U-shaped flexible line for lean single-piece flow", 2, "dmfgfac_flow"),
    ("dmfgfac_flow_mixed", "Mixed-model assembly line for variant production", 2, "dmfgfac_flow"),
    ("dmfgfac_continuous", "Continuous Process Production", 1, None),
    ("dmfgfac_continuous_chem", "Continuous chemical and refinery process plants", 2, "dmfgfac_continuous"),
    ("dmfgfac_continuous_food", "Continuous food and beverage processing lines", 2, "dmfgfac_continuous"),
    ("dmfgfac_flex", "Flexible and Reconfigurable Manufacturing", 1, None),
    ("dmfgfac_flex_fmc", "Flexible manufacturing cell (FMC) with machining centers and robots", 2, "dmfgfac_flex"),
    ("dmfgfac_flex_fms", "Flexible manufacturing system (FMS) with automated material handling", 2, "dmfgfac_flex"),
    ("dmfgfac_flex_micro", "Micro-factory and distributed manufacturing node", 2, "dmfgfac_flex"),
]

_DOMAIN_ROW = (
    "domain_mfg_facility",
    "Manufacturing Facility and Production Configuration Types",
    "Manufacturing facility layout and production system configuration classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['31', '32', '33']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_mfg_facility(conn) -> int:
    """Ingest Manufacturing Facility and Production Configuration Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_mfg_facility'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_mfg_facility",
        "Manufacturing Facility and Production Configuration Types",
        "Manufacturing facility layout and production system configuration classification",
        "1.0",
        "Global",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in MFG_FACILITY_NODES if parent is not None}

    rows = [
        (
            "domain_mfg_facility",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in MFG_FACILITY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(MFG_FACILITY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_mfg_facility'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_mfg_facility'",
        count,
    )

    naics_codes = [
        row["code"]
        for prefix in _NAICS_PREFIXES
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE $1",
            prefix + "%",
        )
    ]

    if naics_codes:
        await conn.executemany(
            """INSERT INTO node_taxonomy_link
                   (system_id, node_code, taxonomy_id, relevance)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
            [("naics_2022", code, "domain_mfg_facility", "primary") for code in naics_codes],
        )

    return count
