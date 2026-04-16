"""Ingest Actuarial Science Method and Model Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_actuarial_method",
    "Actuarial Method Types",
    "Actuarial Science Method and Model Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("am_reserve", "Reserving Methods", 1, None),
    ("am_price", "Pricing Methods", 1, None),
    ("am_risk", "Risk Models", 1, None),
    ("am_reserve_cl", "Chain ladder (development triangle)", 2, "am_reserve"),
    ("am_reserve_bf", "Bornhuetter-Ferguson method", 2, "am_reserve"),
    ("am_reserve_cc", "Cape Cod method", 2, "am_reserve"),
    ("am_reserve_freq", "Frequency-severity method", 2, "am_reserve"),
    ("am_reserve_mcmc", "Bayesian / MCMC reserving", 2, "am_reserve"),
    ("am_price_glm", "Generalized linear models (GLM)", 2, "am_price"),
    ("am_price_ml", "Machine learning pricing models", 2, "am_price"),
    ("am_price_exp", "Experience rating", 2, "am_price"),
    ("am_price_burn", "Burning cost method", 2, "am_price"),
    ("am_risk_cat", "Catastrophe models (RMS, AIR, CoreLogic)", 2, "am_risk"),
    ("am_risk_dfa", "Dynamic financial analysis (DFA)", 2, "am_risk"),
    ("am_risk_solvency", "Solvency capital models (SCR)", 2, "am_risk"),
    ("am_risk_erm", "Enterprise risk management models", 2, "am_risk"),
]


async def ingest_domain_actuarial_method(conn) -> int:
    """Insert or update Actuarial Method Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_actuarial_method"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_actuarial_method", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_actuarial_method",
    )
    return count
