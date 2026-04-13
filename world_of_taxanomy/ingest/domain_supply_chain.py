"""Supply Chain cross-cutting domain taxonomy ingester.

Supply chain taxonomy organizes trade and logistics concepts (cross-cutting):
  Incoterms 2020  (dsc_incoterm*) - EXW, FCA, CPT, CIP, DAP, DPU, DDP, FAS, FOB, CFR, CIF
  Trade Lane      (dsc_lane*)     - domestic, cross-border, transatlantic, transpacific, nearshore
  Customs Process (dsc_customs*)  - entry, FTZ, bonded warehouse, duty drawback, ATA carnet
  Logistics Tier  (dsc_tier*)     - 1PL through 5PL provider tiers

Source: ICC (International Chamber of Commerce) Incoterms 2020 + CSCMP definitions.
Public domain. Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
SUPPLY_CHAIN_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Incoterms 2020 category --
    ("dsc_incoterm",      "Incoterms 2020 Trade Terms",                         1, None),
    ("dsc_incoterm_exw",  "EXW - Ex Works (seller minimum obligation)",        2, "dsc_incoterm"),
    ("dsc_incoterm_fca",  "FCA - Free Carrier (named place of delivery)",      2, "dsc_incoterm"),
    ("dsc_incoterm_dap",  "DAP - Delivered at Place (buyer unloads)",          2, "dsc_incoterm"),
    ("dsc_incoterm_ddp",  "DDP - Delivered Duty Paid (seller maximum oblig.)", 2, "dsc_incoterm"),
    ("dsc_incoterm_fob",  "FOB - Free on Board (vessel, sea/inland waterway)", 2, "dsc_incoterm"),
    ("dsc_incoterm_cif",  "CIF - Cost, Insurance and Freight (sea only)",      2, "dsc_incoterm"),

    # -- Trade Lane category --
    ("dsc_lane",            "Trade Lane",                                        1, None),
    ("dsc_lane_domestic",   "Domestic (within same country)",                  2, "dsc_lane"),
    ("dsc_lane_nearshore",  "Nearshore (adjacent country, short-haul)",        2, "dsc_lane"),
    ("dsc_lane_transatl",   "Transatlantic (US/EU/UK trade corridor)",         2, "dsc_lane"),
    ("dsc_lane_transpacif", "Transpacific (US/Asia trade corridor)",            2, "dsc_lane"),
    ("dsc_lane_global",     "Global and Multi-Modal (hub-and-spoke)",           2, "dsc_lane"),

    # -- Customs and Trade Compliance category --
    ("dsc_customs",         "Customs and Trade Compliance",                     1, None),
    ("dsc_customs_entry",   "Formal Customs Entry and Clearance",              2, "dsc_customs"),
    ("dsc_customs_ftz",     "Foreign Trade Zone (FTZ) Operations",             2, "dsc_customs"),
    ("dsc_customs_bonded",  "Bonded Warehouse Storage",                        2, "dsc_customs"),
    ("dsc_customs_drawback","Duty Drawback and Refund Programs",               2, "dsc_customs"),

    # -- Logistics Provider Tier category --
    ("dsc_tier",       "Logistics Provider Tier",                               1, None),
    ("dsc_tier_1pl",   "1PL - First-Party (own assets and operations)",        2, "dsc_tier"),
    ("dsc_tier_2pl",   "2PL - Second-Party (asset-based carrier)",             2, "dsc_tier"),
    ("dsc_tier_3pl",   "3PL - Third-Party (outsourced warehousing + transport)",2, "dsc_tier"),
    ("dsc_tier_4pl",   "4PL - Fourth-Party (lead logistics orchestrator)",     2, "dsc_tier"),
    ("dsc_tier_5pl",   "5PL - Fifth-Party (e-logistics and digital platforms)", 2, "dsc_tier"),
]

_DOMAIN_ROW = (
    "domain_supply_chain",
    "Supply Chain and Trade Terms",
    "Incoterms 2020, trade lanes, customs process and logistics tier taxonomy (cross-cutting)",
    "WorldOfTaxanomy",
    None,
)


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific supply chain types."""
    parts = code.split("_")
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_supply_chain(conn) -> int:
    """Ingest Supply Chain cross-cutting domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_supply_chain'). Cross-cutting: links to NAICS 42
    (wholesale), 48-49 (transport), and 31-33 (manufacturing) broadly.

    Returns total supply chain node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_supply_chain",
        "Supply Chain and Trade Terms",
        "Incoterms 2020, trade lanes, customs process and logistics tier taxonomy (cross-cutting)",
        "1.0",
        "Global",
        "WorldOfTaxanomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in SUPPLY_CHAIN_NODES if parent is not None}

    rows = [
        (
            "domain_supply_chain",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in SUPPLY_CHAIN_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(SUPPLY_CHAIN_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_supply_chain'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_supply_chain'",
        count,
    )

    # Cross-cutting: link to wholesale, transport, and manufacturing NAICS sectors
    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' "
            "AND (code LIKE '42%' OR code LIKE '48%' OR code LIKE '49%' "
            "     OR code LIKE '31%' OR code LIKE '32%' OR code LIKE '33%')"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_supply_chain", "secondary") for code in naics_codes],
    )

    return count
