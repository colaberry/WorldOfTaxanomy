"""Ingest Construction and Building Permit Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_construction_permit",
    "Construction Permit Types",
    "Construction and Building Permit Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cp_build", "Building Permits", 1, None),
    ("cp_trade", "Trade Permits", 1, None),
    ("cp_special", "Special Permits", 1, None),
    ("cp_build_new", "New construction permit", 2, "cp_build"),
    ("cp_build_reno", "Renovation and alteration permit", 2, "cp_build"),
    ("cp_build_demo", "Demolition permit", 2, "cp_build"),
    ("cp_build_add", "Addition permit", 2, "cp_build"),
    ("cp_build_temp", "Temporary structure permit", 2, "cp_build"),
    ("cp_trade_elec", "Electrical permit", 2, "cp_trade"),
    ("cp_trade_plumb", "Plumbing permit", 2, "cp_trade"),
    ("cp_trade_mech", "Mechanical / HVAC permit", 2, "cp_trade"),
    ("cp_trade_fire", "Fire protection permit", 2, "cp_trade"),
    ("cp_special_grade", "Grading and excavation permit", 2, "cp_special"),
    ("cp_special_sign", "Sign permit", 2, "cp_special"),
    ("cp_special_env", "Environmental compliance permit", 2, "cp_special"),
    ("cp_special_occ", "Certificate of occupancy", 2, "cp_special"),
]


async def ingest_domain_construction_permit(conn) -> int:
    """Insert or update Construction Permit Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_construction_permit"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_construction_permit", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_construction_permit",
    )
    return count
