"""Ingest Crop Protection Product and Method Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_crop_protection",
    "Crop Protection Types",
    "Crop Protection Product and Method Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cp_chem", "Chemical Crop Protection", 1, None),
    ("cp_bio", "Biological Crop Protection", 1, None),
    ("cp_int", "Integrated Pest Management", 1, None),
    ("cp_chem_herb", "Herbicides (pre-emergent, post-emergent)", 2, "cp_chem"),
    ("cp_chem_insect", "Insecticides (contact, systemic)", 2, "cp_chem"),
    ("cp_chem_fung", "Fungicides (protectant, systemic)", 2, "cp_chem"),
    ("cp_chem_nem", "Nematicides", 2, "cp_chem"),
    ("cp_chem_pgr", "Plant growth regulators", 2, "cp_chem"),
    ("cp_bio_micro", "Microbial biocontrol agents", 2, "cp_bio"),
    ("cp_bio_macro", "Macrobial agents (predatory insects)", 2, "cp_bio"),
    ("cp_bio_botan", "Botanical pesticides (neem, pyrethrin)", 2, "cp_bio"),
    ("cp_bio_phero", "Pheromone traps and mating disruption", 2, "cp_bio"),
    ("cp_bio_rna", "RNA interference (RNAi) biopesticides", 2, "cp_bio"),
    ("cp_int_scout", "Scouting and monitoring", 2, "cp_int"),
    ("cp_int_thresh", "Economic threshold decision-making", 2, "cp_int"),
    ("cp_int_resist", "Resistance management", 2, "cp_int"),
    ("cp_int_cover", "Cover crops and cultural controls", 2, "cp_int"),
]


async def ingest_domain_crop_protection(conn) -> int:
    """Insert or update Crop Protection Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_crop_protection"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_crop_protection", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_crop_protection",
    )
    return count
