"""Defence Acquisition Programme Types domain taxonomy ingester.

Classifies defence procurement by programme category, contract instrument and
acquisition phase. Orthogonal to equipment/capability type (domain_defence_type).
Aligns with US DoD DODI 5000.02 ACAT framework, NATO acquisition principles,
and UK CADMID lifecycle. Used by programme managers, defence contractors,
and government acquisition officials.

Code prefix: ddfacq_
System ID: domain_defence_acquisition
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
DEFENCE_ACQUISITION_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ddfacq_acat", "US DoD ACAT Programme Categories", 1, None),
    ("ddfacq_acat_i", "ACAT I - Major Defence Acquisition Programs (MDAP)", 2, "ddfacq_acat"),
    ("ddfacq_acat_ii", "ACAT II - Major Automated Information Systems (MAIS)", 2, "ddfacq_acat"),
    ("ddfacq_acat_iii", "ACAT III - Less than ACAT II or MDAP thresholds", 2, "ddfacq_acat"),
    ("ddfacq_acat_iv", "ACAT IV - Component or lower command acquisitions", 2, "ddfacq_acat"),
    ("ddfacq_contract", "Contract Instrument Types", 1, None),
    ("ddfacq_contract_ffp", "Firm-Fixed-Price (FFP) contracts", 2, "ddfacq_contract"),
    ("ddfacq_contract_fpif", "Fixed-Price Incentive Firm (FPIF) contracts", 2, "ddfacq_contract"),
    ("ddfacq_contract_cpaf", "Cost-Plus-Award-Fee (CPAF) contracts", 2, "ddfacq_contract"),
    ("ddfacq_contract_cpff", "Cost-Plus-Fixed-Fee (CPFF) contracts", 2, "ddfacq_contract"),
    ("ddfacq_contract_idiq", "Indefinite Delivery / Indefinite Quantity (IDIQ)", 2, "ddfacq_contract"),
    ("ddfacq_phase", "Acquisition Lifecycle Phases", 1, None),
    ("ddfacq_phase_matdev", "Material Development Decision (MDD) / Concept", 2, "ddfacq_phase"),
    ("ddfacq_phase_msb", "Milestone B - Engineering and Manufacturing Development", 2, "ddfacq_phase"),
    ("ddfacq_phase_msc", "Milestone C - Production and Deployment", 2, "ddfacq_phase"),
    ("ddfacq_phase_frr", "Full-Rate Production Decision Review", 2, "ddfacq_phase"),
    ("ddfacq_phase_ops", "Operations and Support (sustainment phase)", 2, "ddfacq_phase"),
    ("ddfacq_intl", "International and Non-US Acquisition Frameworks", 1, None),
    ("ddfacq_intl_nato", "NATO Capability Development (NDPP, NCI Agency)", 2, "ddfacq_intl"),
    ("ddfacq_intl_fms", "US Foreign Military Sales (FMS) programme", 2, "ddfacq_intl"),
    ("ddfacq_intl_dcs", "Direct Commercial Sales (DCS) and export licensing", 2, "ddfacq_intl"),
]

_DOMAIN_ROW = (
    "domain_defence_acquisition",
    "Defence Acquisition Programme Types",
    "Defence procurement and acquisition programme classification: ACAT, contract types, lifecycle phases",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3364', '5413', '9281']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_defence_acquisition(conn) -> int:
    """Ingest Defence Acquisition Programme Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_defence_acquisition'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_defence_acquisition",
        "Defence Acquisition Programme Types",
        "Defence procurement and acquisition programme classification: ACAT, contract types, lifecycle phases",
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

    parent_codes = {parent for _, _, _, parent in DEFENCE_ACQUISITION_NODES if parent is not None}

    rows = [
        (
            "domain_defence_acquisition",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in DEFENCE_ACQUISITION_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(DEFENCE_ACQUISITION_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_defence_acquisition'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_defence_acquisition'",
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
            [("naics_2022", code, "domain_defence_acquisition", "primary") for code in naics_codes],
        )

    return count
