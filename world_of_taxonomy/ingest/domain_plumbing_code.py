"""Ingest Plumbing Code and System Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_plumbing_code",
    "Plumbing Code Types",
    "Plumbing Code and System Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pl_sys", "Plumbing Systems", 1, None),
    ("pl_code", "Code Standards", 1, None),
    ("pl_mat", "Piping Materials", 1, None),
    ("pl_sys_supply", "Water supply and distribution", 2, "pl_sys"),
    ("pl_sys_dws", "Drain, waste and vent (DWV)", 2, "pl_sys"),
    ("pl_sys_storm", "Stormwater drainage", 2, "pl_sys"),
    ("pl_sys_hot", "Hot water systems (storage, tankless, recirculating)", 2, "pl_sys"),
    ("pl_sys_fire", "Fire protection piping (wet, dry standpipe)", 2, "pl_sys"),
    ("pl_code_ipc", "International Plumbing Code (IPC)", 2, "pl_code"),
    ("pl_code_upc", "Uniform Plumbing Code (UPC)", 2, "pl_code"),
    ("pl_code_nsf", "NSF/ANSI standards (water treatment)", 2, "pl_code"),
    ("pl_mat_copper", "Copper (Type K, L, M)", 2, "pl_mat"),
    ("pl_mat_pex", "PEX (cross-linked polyethylene)", 2, "pl_mat"),
    ("pl_mat_cpvc", "CPVC (chlorinated PVC)", 2, "pl_mat"),
    ("pl_mat_pvc", "PVC (DWV applications)", 2, "pl_mat"),
    ("pl_mat_cast", "Cast iron (hub, hubless)", 2, "pl_mat"),
]


async def ingest_domain_plumbing_code(conn) -> int:
    """Insert or update Plumbing Code Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_plumbing_code"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_plumbing_code", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_plumbing_code",
    )
    return count
