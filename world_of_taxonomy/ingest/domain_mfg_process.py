"""Manufacturing Process Type domain taxonomy ingester.

Manufacturing process taxonomy organizes production methods:
  Discrete Mfg  (dfp_discrete*) - machining, forming, assembly, fabrication
  Process Mfg   (dfp_process*)  - chemical, refining, mixing, continuous flow
  Additive Mfg  (dfp_add*)      - FDM/FFF, SLA/DLP, SLS/MJF, DMLS/EBM
  Casting/Mold  (dfp_form*)     - casting, forging, molding, extrusion

Source: NIST manufacturing process classifications. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
MFG_PROCESS_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Discrete Manufacturing category --
    ("dfp_discrete",         "Discrete Manufacturing",                        1, None),
    ("dfp_discrete_machine", "Machining (CNC, turning, milling, grinding)",  2, "dfp_discrete"),
    ("dfp_discrete_form",    "Metal Forming (stamping, rolling, bending)",   2, "dfp_discrete"),
    ("dfp_discrete_assem",   "Assembly (manual, robotic, SMT)",              2, "dfp_discrete"),
    ("dfp_discrete_fab",     "Fabrication (welding, cutting, joining)",      2, "dfp_discrete"),

    # -- Process Manufacturing category --
    ("dfp_process",       "Process Manufacturing",                            1, None),
    ("dfp_process_chem",  "Chemical Processing (reaction, synthesis)",       2, "dfp_process"),
    ("dfp_process_ref",   "Refining and Distillation",                       2, "dfp_process"),
    ("dfp_process_mix",   "Mixing, Blending, and Compounding",               2, "dfp_process"),
    ("dfp_process_cont",  "Continuous Flow Processing",                      2, "dfp_process"),
    ("dfp_process_batch", "Batch Processing",                                 2, "dfp_process"),

    # -- Additive Manufacturing category --
    ("dfp_add",       "Additive Manufacturing (3D Printing)",                1, None),
    ("dfp_add_fdm",   "FDM/FFF (Fused Deposition Modeling - plastics)",     2, "dfp_add"),
    ("dfp_add_sla",   "SLA/DLP (Stereolithography - photopolymers)",        2, "dfp_add"),
    ("dfp_add_sls",   "SLS/MJF (Selective Laser Sintering - powders)",      2, "dfp_add"),
    ("dfp_add_dmls",  "DMLS/EBM (Metal Additive - direct metal laser)",     2, "dfp_add"),

    # -- Forming and Casting category --
    ("dfp_form",          "Casting, Molding, and Forming",                   1, None),
    ("dfp_form_cast",     "Casting (sand, die, investment, centrifugal)",   2, "dfp_form"),
    ("dfp_form_forge",    "Forging (open-die, closed-die, ring rolling)",   2, "dfp_form"),
    ("dfp_form_mold",     "Injection Molding and Compression Molding",      2, "dfp_form"),
    ("dfp_form_extrude",  "Extrusion and Pultrusion",                       2, "dfp_form"),
]

_DOMAIN_ROW = (
    "domain_mfg_process",
    "Manufacturing Process Types",
    "Discrete, process, additive manufacturing and forming/casting process taxonomy",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["31", "32", "33"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific process types."""
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


async def ingest_domain_mfg_process(conn) -> int:
    """Ingest Manufacturing Process Type domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_mfg_process'), and links NAICS 31-33xxx nodes
    via node_taxonomy_link.

    Returns total process node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_mfg_process",
        "Manufacturing Process Types",
        "Discrete, process, additive manufacturing and forming/casting process taxonomy",
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

    parent_codes = {parent for _, _, _, parent in MFG_PROCESS_NODES if parent is not None}

    rows = [
        (
            "domain_mfg_process",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in MFG_PROCESS_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(MFG_PROCESS_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_mfg_process'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_mfg_process'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' "
            "AND (code LIKE '31%' OR code LIKE '32%' OR code LIKE '33%')"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_mfg_process", "primary") for code in naics_codes],
    )

    return count
