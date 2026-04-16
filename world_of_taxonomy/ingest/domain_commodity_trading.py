"""Ingest Commodity Trading Market and Instrument Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_commodity_trading",
    "Commodity Trading Types",
    "Commodity Trading Market and Instrument Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ct_energy", "Energy Commodities", 1, None),
    ("ct_metal", "Metal Commodities", 1, None),
    ("ct_agri", "Agricultural Commodities", 1, None),
    ("ct_mech", "Trading Mechanisms", 1, None),
    ("ct_energy_crude", "Crude oil (WTI, Brent, Dubai)", 2, "ct_energy"),
    ("ct_energy_gas", "Natural gas (Henry Hub, TTF, JKM)", 2, "ct_energy"),
    ("ct_energy_coal", "Coal (thermal, coking)", 2, "ct_energy"),
    ("ct_energy_power", "Electricity (day-ahead, intraday)", 2, "ct_energy"),
    ("ct_metal_prec", "Precious metals (gold, silver, platinum)", 2, "ct_metal"),
    ("ct_metal_base", "Base metals (copper, aluminum, zinc)", 2, "ct_metal"),
    ("ct_metal_rare", "Rare earths and critical minerals", 2, "ct_metal"),
    ("ct_agri_grain", "Grains (corn, wheat, soybeans)", 2, "ct_agri"),
    ("ct_agri_soft", "Softs (coffee, cocoa, sugar, cotton)", 2, "ct_agri"),
    ("ct_agri_live", "Livestock (cattle, hogs)", 2, "ct_agri"),
    ("ct_mech_phys", "Physical trading (spot, forward)", 2, "ct_mech"),
    ("ct_mech_paper", "Paper trading (futures, options, swaps)", 2, "ct_mech"),
]


async def ingest_domain_commodity_trading(conn) -> int:
    """Insert or update Commodity Trading Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_commodity_trading"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_commodity_trading", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_commodity_trading",
    )
    return count
