"""Nuclear Energy domain taxonomy ingester.

Organizes nuclear energy industry types into categories aligned with
NAICS 2211 (Electric Power Generation, Transmission and Distribution).

Code prefix: dne_
Categories: nuclear power generation, fuel cycle, reactor technology,
decommissioning, nuclear medicine.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
NUCLEAR_ENERGY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Nuclear Power Generation --
    ("dne_power",              "Nuclear Power Generation",                           1, None),
    ("dne_power_lwr",          "Light Water Reactor Power Plants (PWR, BWR)",        2, "dne_power"),
    ("dne_power_heavy",        "Heavy Water Reactor Power Plants (PHWR, CANDU)",     2, "dne_power"),
    ("dne_power_smr",          "Small Modular Reactors (SMR)",                       2, "dne_power"),
    ("dne_power_grid",         "Nuclear Grid Integration and Load Following",        2, "dne_power"),

    # -- Fuel Cycle --
    ("dne_fuel",               "Fuel Cycle",                                         1, None),
    ("dne_fuel_mining",        "Uranium Mining and Milling",                          2, "dne_fuel"),
    ("dne_fuel_enrich",        "Uranium Enrichment (centrifuge, gaseous diffusion)",  2, "dne_fuel"),
    ("dne_fuel_fabricate",     "Fuel Rod and Assembly Fabrication",                   2, "dne_fuel"),
    ("dne_fuel_reprocess",     "Spent Fuel Reprocessing and Recycling",               2, "dne_fuel"),
    ("dne_fuel_storage",       "Spent Fuel Dry Cask and Pool Storage",                2, "dne_fuel"),

    # -- Reactor Technology --
    ("dne_reactor",            "Reactor Technology",                                 1, None),
    ("dne_reactor_gen4",       "Generation IV Reactor Designs (MSR, HTGR, SFR)",     2, "dne_reactor"),
    ("dne_reactor_fusion",     "Fusion Energy Research and Development",              2, "dne_reactor"),
    ("dne_reactor_control",    "Reactor Control Systems and Instrumentation",         2, "dne_reactor"),
    ("dne_reactor_safety",     "Nuclear Safety Systems and Containment",              2, "dne_reactor"),

    # -- Decommissioning --
    ("dne_decom",              "Decommissioning",                                    1, None),
    ("dne_decom_dismantle",    "Reactor Dismantling and Site Remediation",            2, "dne_decom"),
    ("dne_decom_waste",        "Radioactive Waste Management and Disposal",           2, "dne_decom"),
    ("dne_decom_monitor",      "Long-Term Environmental Monitoring",                  2, "dne_decom"),

    # -- Nuclear Medicine --
    ("dne_med",                "Nuclear Medicine",                                   1, None),
    ("dne_med_isotope",        "Medical Isotope Production (Mo-99, I-131, F-18)",    2, "dne_med"),
    ("dne_med_therapy",        "Radiation Therapy and Oncology Applications",         2, "dne_med"),
    ("dne_med_imaging",        "Nuclear Diagnostic Imaging (PET, SPECT)",             2, "dne_med"),
    ("dne_med_steril",         "Radiation Sterilization of Medical Devices",          2, "dne_med"),
]

_DOMAIN_ROW = (
    "domain_nuclear_energy",
    "Nuclear Energy Types",
    "Nuclear power generation, fuel cycle, reactor technology, "
    "decommissioning, and nuclear medicine domain taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link: 2211 (Electric Power Generation)
_NAICS_PREFIXES = ["2211"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_nuclear_energy(conn) -> int:
    """Ingest Nuclear Energy domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_nuclear_energy'), and links NAICS 2211 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_nuclear_energy",
        "Nuclear Energy Types",
        "Nuclear power generation, fuel cycle, reactor technology, "
        "decommissioning, and nuclear medicine domain taxonomy",
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

    parent_codes = {parent for _, _, _, parent in NUCLEAR_ENERGY_NODES if parent is not None}

    rows = [
        (
            "domain_nuclear_energy",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in NUCLEAR_ENERGY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(NUCLEAR_ENERGY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_nuclear_energy'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_nuclear_energy'",
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
            [("naics_2022", code, "domain_nuclear_energy", "primary") for code in naics_codes],
        )

    return count
