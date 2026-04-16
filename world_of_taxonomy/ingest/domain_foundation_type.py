"""Ingest Building Foundation System Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_foundation_type",
    "Foundation Types",
    "Building Foundation System Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("fd_shallow", "Shallow Foundations", 1, None),
    ("fd_deep", "Deep Foundations", 1, None),
    ("fd_special", "Special Foundations", 1, None),
    ("fd_shallow_spread", "Spread footing (isolated, combined)", 2, "fd_shallow"),
    ("fd_shallow_mat", "Mat / raft foundation", 2, "fd_shallow"),
    ("fd_shallow_strip", "Strip footing (continuous)", 2, "fd_shallow"),
    ("fd_shallow_slab", "Slab-on-grade", 2, "fd_shallow"),
    ("fd_deep_driven", "Driven piles (steel, concrete, timber)", 2, "fd_deep"),
    ("fd_deep_drilled", "Drilled shafts (caissons)", 2, "fd_deep"),
    ("fd_deep_micro", "Micropiles / mini-piles", 2, "fd_deep"),
    ("fd_deep_helical", "Helical piles (screw piles)", 2, "fd_deep"),
    ("fd_special_under", "Underpinning systems", 2, "fd_special"),
    ("fd_special_retain", "Retaining walls (gravity, cantilever, anchored)", 2, "fd_special"),
    ("fd_special_soil", "Soil improvement (compaction, grouting)", 2, "fd_special"),
    ("fd_special_seismic", "Seismic isolation / base isolation", 2, "fd_special"),
]


async def ingest_domain_foundation_type(conn) -> int:
    """Insert or update Foundation Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_foundation_type"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_foundation_type", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_foundation_type",
    )
    return count
