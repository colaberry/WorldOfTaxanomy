"""Mining Reserve Classification domain taxonomy ingester.

Reserve classification taxonomy uses SPE-PRMS (Society of Petroleum Engineers
Petroleum Resources Management System):
  Reserves         (dmr_res*)   - proved (1P), probable (2P), possible (3P)
  Contingent       (dmr_cont*)  - 1C/2C/3C (discovered but not yet commercial)
  Prospective      (dmr_prosp*) - low/best/high estimate (undiscovered)

Source: SPE-PRMS 2018 framework. Open standard.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
RESERVE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Reserves category (commercially recoverable) --
    ("dmr_res",          "Reserves (Commercially Recoverable)",               1, None),
    ("dmr_res_proved",   "Proved Reserves (1P - high certainty)",            2, "dmr_res"),
    ("dmr_res_probable", "Probable Reserves (2P - at least 50% confidence)", 2, "dmr_res"),
    ("dmr_res_possible", "Possible Reserves (3P - at least 10% confidence)", 2, "dmr_res"),

    # -- Contingent Resources category (discovered, not yet commercial) --
    ("dmr_cont",      "Contingent Resources (Discovered, Sub-commercial)",    1, None),
    ("dmr_cont_1c",   "Contingent Resources 1C (Low Estimate)",              2, "dmr_cont"),
    ("dmr_cont_2c",   "Contingent Resources 2C (Best Estimate)",             2, "dmr_cont"),
    ("dmr_cont_3c",   "Contingent Resources 3C (High Estimate)",             2, "dmr_cont"),

    # -- Prospective Resources category (undiscovered) --
    ("dmr_prosp",      "Prospective Resources (Undiscovered)",                1, None),
    ("dmr_prosp_low",  "Prospective Resources Low Estimate",                 2, "dmr_prosp"),
    ("dmr_prosp_best", "Prospective Resources Best Estimate",                2, "dmr_prosp"),
    ("dmr_prosp_high", "Prospective Resources High Estimate",                2, "dmr_prosp"),
]

_DOMAIN_ROW = (
    "domain_mining_reserve",
    "Mining Reserve Classification",
    "SPE-PRMS petroleum and mineral reserve classification: proved/probable/possible reserves and contingent/prospective resources",
    "WorldOfTaxanomy",
    None,
)

_NAICS_PREFIXES = ["21"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific reserve classes."""
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


async def ingest_domain_mining_reserve(conn) -> int:
    """Ingest Mining Reserve Classification domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_mining_reserve'), and links NAICS 21xxx nodes
    via node_taxonomy_link.

    Returns total reserve node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_mining_reserve",
        "Mining Reserve Classification",
        "SPE-PRMS petroleum and mineral reserve classification taxonomy",
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

    parent_codes = {parent for _, _, _, parent in RESERVE_NODES if parent is not None}

    rows = [
        (
            "domain_mining_reserve",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in RESERVE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(RESERVE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_mining_reserve'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_mining_reserve'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '21%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_mining_reserve", "primary") for code in naics_codes],
    )

    return count
