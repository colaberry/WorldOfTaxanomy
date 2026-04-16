"""Ingest Dental Service and Specialty Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_dental",
    "Dental Service Types",
    "Dental Service and Specialty Types",
    "WorldOfTaxonomy",
    "United States",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("dnt_prev", "Preventive Dentistry", 1, None),
    ("dnt_rest", "Restorative Dentistry", 1, None),
    ("dnt_spec", "Dental Specialties", 1, None),
    ("dnt_prev_clean", "Prophylaxis and cleaning", 2, "dnt_prev"),
    ("dnt_prev_xray", "Diagnostic imaging (X-ray, CBCT)", 2, "dnt_prev"),
    ("dnt_prev_seal", "Sealants and fluoride treatment", 2, "dnt_prev"),
    ("dnt_prev_screen", "Oral cancer screening", 2, "dnt_prev"),
    ("dnt_rest_fill", "Fillings (amalgam, composite, ceramic)", 2, "dnt_rest"),
    ("dnt_rest_crown", "Crowns and bridges", 2, "dnt_rest"),
    ("dnt_rest_root", "Root canal therapy (endodontics)", 2, "dnt_rest"),
    ("dnt_rest_impl", "Dental implants", 2, "dnt_rest"),
    ("dnt_rest_denture", "Dentures and partials", 2, "dnt_rest"),
    ("dnt_rest_veneer", "Veneers and cosmetic bonding", 2, "dnt_rest"),
    ("dnt_spec_ortho", "Orthodontics (braces, aligners)", 2, "dnt_spec"),
    ("dnt_spec_perio", "Periodontics (gum disease treatment)", 2, "dnt_spec"),
    ("dnt_spec_pedo", "Pediatric dentistry", 2, "dnt_spec"),
    ("dnt_spec_oral", "Oral and maxillofacial surgery", 2, "dnt_spec"),
    ("dnt_spec_prosth", "Prosthodontics", 2, "dnt_spec"),
]


async def ingest_domain_dental(conn) -> int:
    """Insert or update Dental Service Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_dental"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_dental", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_dental",
    )
    return count
