"""Ingest Prefabricated Building Component Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_prefab",
    "Prefab Types",
    "Prefabricated Building Component Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pf_struct", "Structural Prefab", 1, None),
    ("pf_envelope", "Envelope Prefab", 1, None),
    ("pf_mep", "MEP Prefab", 1, None),
    ("pf_struct_beam", "Precast beams and columns", 2, "pf_struct"),
    ("pf_struct_slab", "Precast floor and roof slabs", 2, "pf_struct"),
    ("pf_struct_wall", "Precast wall panels", 2, "pf_struct"),
    ("pf_struct_stairs", "Precast stairs and landings", 2, "pf_struct"),
    ("pf_envelope_curtain", "Prefabricated curtain wall units", 2, "pf_envelope"),
    ("pf_envelope_rain", "Pre-assembled rainscreen panels", 2, "pf_envelope"),
    ("pf_envelope_roof", "Pre-assembled roof cassettes", 2, "pf_envelope"),
    ("pf_mep_rack", "Multi-trade rack assemblies (M/E/P)", 2, "pf_mep"),
    ("pf_mep_plant", "Prefabricated plant rooms", 2, "pf_mep"),
    ("pf_mep_riser", "Prefabricated riser modules", 2, "pf_mep"),
    ("pf_mep_head", "Prefabricated headwall units", 2, "pf_mep"),
]


async def ingest_domain_prefab(conn) -> int:
    """Insert or update Prefab Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_prefab"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_prefab", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_prefab",
    )
    return count
