"""Ingest Commercial Lease Abstraction and Data Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_lease_abstract",
    "Lease Abstraction Types",
    "Commercial Lease Abstraction and Data Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("la_key", "Key Lease Terms", 1, None),
    ("la_fin", "Financial Terms", 1, None),
    ("la_ops", "Operational Terms", 1, None),
    ("la_key_parties", "Parties (landlord, tenant, guarantor)", 2, "la_key"),
    ("la_key_premises", "Premises description and measurements", 2, "la_key"),
    ("la_key_term", "Lease term (commencement, expiration)", 2, "la_key"),
    ("la_key_options", "Options (renewal, expansion, termination)", 2, "la_key"),
    ("la_fin_rent", "Base rent and escalation schedule", 2, "la_fin"),
    ("la_fin_opex", "Operating expense pass-throughs (NNN, modified gross)", 2, "la_fin"),
    ("la_fin_cam", "CAM charges and reconciliation", 2, "la_fin"),
    ("la_fin_ti", "Tenant improvement allowance", 2, "la_fin"),
    ("la_fin_free", "Free rent and concessions", 2, "la_fin"),
    ("la_fin_security", "Security deposit and letter of credit", 2, "la_fin"),
    ("la_ops_use", "Permitted use clause", 2, "la_ops"),
    ("la_ops_assign", "Assignment and subletting rights", 2, "la_ops"),
    ("la_ops_excl", "Exclusive use and radius restrictions", 2, "la_ops"),
    ("la_ops_maint", "Maintenance and repair responsibilities", 2, "la_ops"),
]


async def ingest_domain_lease_abstract(conn) -> int:
    """Insert or update Lease Abstraction Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_lease_abstract"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_lease_abstract", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_lease_abstract",
    )
    return count
