"""Ingest Veterinary Service and Specialty Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_veterinary",
    "Veterinary Service Types",
    "Veterinary Service and Specialty Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("vet_comp", "Companion Animal Services", 1, None),
    ("vet_large", "Large Animal and Equine", 1, None),
    ("vet_spec", "Veterinary Specialties", 1, None),
    ("vet_comp_well", "Wellness exams and vaccination", 2, "vet_comp"),
    ("vet_comp_surg", "Small animal surgery", 2, "vet_comp"),
    ("vet_comp_dent", "Veterinary dentistry", 2, "vet_comp"),
    ("vet_comp_emerg", "Emergency and critical care", 2, "vet_comp"),
    ("vet_comp_diag", "Diagnostic imaging and laboratory", 2, "vet_comp"),
    ("vet_large_food", "Food animal medicine (cattle, swine, poultry)", 2, "vet_large"),
    ("vet_large_equine", "Equine medicine and surgery", 2, "vet_large"),
    ("vet_large_repro", "Reproductive services and breeding", 2, "vet_large"),
    ("vet_large_herd", "Herd health management", 2, "vet_large"),
    ("vet_spec_onco", "Veterinary oncology", 2, "vet_spec"),
    ("vet_spec_derm", "Veterinary dermatology", 2, "vet_spec"),
    ("vet_spec_neuro", "Veterinary neurology", 2, "vet_spec"),
    ("vet_spec_cardio", "Veterinary cardiology", 2, "vet_spec"),
    ("vet_spec_ophth", "Veterinary ophthalmology", 2, "vet_spec"),
    ("vet_spec_exotic", "Exotic and zoo animal medicine", 2, "vet_spec"),
    ("vet_spec_behav", "Animal behavior", 2, "vet_spec"),
]


async def ingest_domain_veterinary(conn) -> int:
    """Insert or update Veterinary Service Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_veterinary"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_veterinary", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_veterinary",
    )
    return count
