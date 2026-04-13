"""Utility Grid Region domain taxonomy ingester.

Grid taxonomy organizes electrical grid infrastructure:
  Voltage Level (dug_voltage*) - transmission, sub-transmission, distribution, LV
  Grid Region   (dug_region*)  - NERC reliability regions (ERCOT, PJM, MISO, CAISO, etc.)

Source: NERC (North American Electric Reliability Corporation). Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
GRID_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Voltage Level category --
    ("dug_voltage",        "Voltage Level",                                    1, None),
    ("dug_voltage_trans",  "Transmission (115 kV and above)",                2, "dug_voltage"),
    ("dug_voltage_sub",    "Sub-Transmission (26-115 kV)",                   2, "dug_voltage"),
    ("dug_voltage_dist",   "Distribution (under 26 kV)",                     2, "dug_voltage"),
    ("dug_voltage_low",    "Low Voltage / Customer Service (120V-480V)",     2, "dug_voltage"),

    # -- Grid Region (NERC) category --
    ("dug_region",        "Grid Region (NERC Reliability)",                   1, None),
    ("dug_region_ercot",  "ERCOT (Electric Reliability Council of Texas)",   2, "dug_region"),
    ("dug_region_pjm",    "PJM Interconnection (Mid-Atlantic, Midwest)",     2, "dug_region"),
    ("dug_region_miso",   "MISO (Midcontinent ISO - Midwest)",               2, "dug_region"),
    ("dug_region_caiso",  "CAISO (California Independent System Operator)",  2, "dug_region"),
    ("dug_region_spp",    "SPP (Southwest Power Pool - South-Central)",      2, "dug_region"),
    ("dug_region_nyiso",  "NYISO (New York Independent System Operator)",    2, "dug_region"),
    ("dug_region_isone",  "ISO-NE (ISO New England)",                        2, "dug_region"),
    ("dug_region_serc",   "SERC Reliability (Southeast)",                    2, "dug_region"),
    ("dug_region_wecc",   "WECC (Western Electricity Coordinating Council)", 2, "dug_region"),
]

_DOMAIN_ROW = (
    "domain_util_grid",
    "Utility Grid Regions",
    "Electrical grid voltage levels and NERC reliability region taxonomy for utilities",
    "WorldOfTaxanomy",
    None,
)

_NAICS_PREFIXES = ["221"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific grid types."""
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


async def ingest_domain_util_grid(conn) -> int:
    """Ingest Utility Grid Region domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_util_grid'), and links NAICS 221xxx nodes
    via node_taxonomy_link.

    Returns total grid node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_util_grid",
        "Utility Grid Regions",
        "Electrical grid voltage levels and NERC reliability region taxonomy for utilities",
        "1.0",
        "United States",
        "WorldOfTaxanomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in GRID_NODES if parent is not None}

    rows = [
        (
            "domain_util_grid",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in GRID_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(GRID_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_util_grid'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_util_grid'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '221%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_util_grid", "primary") for code in naics_codes],
    )

    return count
