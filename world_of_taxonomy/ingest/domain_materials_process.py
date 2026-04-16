"""Advanced Materials Manufacturing Process Types domain taxonomy ingester.

Classifies the manufacturing and synthesis processes used to produce advanced materials.
Orthogonal to material type and application sector.
Used by process engineers, equipment OEMs, IP attorneys, and R&D lab managers
to categorize process IP, equipment procurement and scale-up pathways.

Code prefix: damproc_
System ID: domain_materials_process
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
MATERIALS_PROCESS_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("damproc_additive", "Additive Manufacturing (3D Printing)", 1, None),
    ("damproc_additive_slm", "Selective Laser Melting/Sintering (SLM/SLS metals and ceramics)", 2, "damproc_additive"),
    ("damproc_additive_fdm", "FDM/FFF filament extrusion (polymers, composite)", 2, "damproc_additive"),
    ("damproc_additive_dlp", "Stereolithography and DLP (photopolymer resin)", 2, "damproc_additive"),
    ("damproc_additive_dw", "Direct Ink Writing and binder jetting", 2, "damproc_additive"),
    ("damproc_vapor", "Chemical and Physical Vapor Deposition", 1, None),
    ("damproc_vapor_cvd", "CVD (MOCVD, LPCVD, PECVD) thin film growth", 2, "damproc_vapor"),
    ("damproc_vapor_pvd", "PVD sputtering and evaporation (magnetron, e-beam)", 2, "damproc_vapor"),
    ("damproc_vapor_ald", "Atomic Layer Deposition (ALD) for conformal coatings", 2, "damproc_vapor"),
    ("damproc_forming", "Thermomechanical Forming and Shaping", 1, None),
    ("damproc_forming_roll", "Hot/cold rolling and strip production (metals)", 2, "damproc_forming"),
    ("damproc_forming_forge", "Forging and hot pressing (titanium, superalloys)", 2, "damproc_forming"),
    ("damproc_forming_sinter", "Powder metallurgy sintering (HIP, CIP, SPS)", 2, "damproc_forming"),
    ("damproc_nano", "Nanofabrication and Nano-Scale Processing", 1, None),
    ("damproc_nano_litho", "Photolithography and EUV patterning (semiconductor)", 2, "damproc_nano"),
    ("damproc_nano_etching", "Dry and wet etching (RIE, ICP, KOH)", 2, "damproc_nano"),
    ("damproc_nano_self", "Self-assembly and bottom-up synthesis (DNA origami, templated growth)", 2, "damproc_nano"),
    ("damproc_composite", "Composite Fabrication and Assembly", 1, None),
    ("damproc_composite_layup", "Manual and automated fiber layup (CFRP hand/ATL/AFP)", 2, "damproc_composite"),
    ("damproc_composite_rtm", "Resin transfer moulding (RTM, VARTM, resin infusion)", 2, "damproc_composite"),
]

_DOMAIN_ROW = (
    "domain_materials_process",
    "Advanced Materials Manufacturing Process Types",
    "Manufacturing process classification for advanced materials: additive, CVD, thin film, forming, nano",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3354', '3391', '3364', '5417']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_materials_process(conn) -> int:
    """Ingest Advanced Materials Manufacturing Process Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_materials_process'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_materials_process",
        "Advanced Materials Manufacturing Process Types",
        "Manufacturing process classification for advanced materials: additive, CVD, thin film, forming, nano",
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

    parent_codes = {parent for _, _, _, parent in MATERIALS_PROCESS_NODES if parent is not None}

    rows = [
        (
            "domain_materials_process",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in MATERIALS_PROCESS_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(MATERIALS_PROCESS_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_materials_process'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_materials_process'",
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
            [("naics_2022", code, "domain_materials_process", "primary") for code in naics_codes],
        )

    return count
