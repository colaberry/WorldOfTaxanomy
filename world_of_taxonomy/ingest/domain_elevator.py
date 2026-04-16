"""Ingest Elevator and Vertical Transport Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_elevator",
    "Elevator Inspection Types",
    "Elevator and Vertical Transport Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("el_type", "Elevator Types", 1, None),
    ("el_inspect", "Inspection Categories", 1, None),
    ("el_code", "Code Requirements", 1, None),
    ("el_type_trac", "Traction elevators (geared, gearless)", 2, "el_type"),
    ("el_type_hydr", "Hydraulic elevators", 2, "el_type"),
    ("el_type_mrl", "Machine-room-less (MRL) elevators", 2, "el_type"),
    ("el_type_freight", "Freight elevators", 2, "el_type"),
    ("el_type_escal", "Escalators and moving walkways", 2, "el_type"),
    ("el_inspect_accept", "Acceptance inspection (new installation)", 2, "el_inspect"),
    ("el_inspect_period", "Periodic inspection (annual)", 2, "el_inspect"),
    ("el_inspect_witness", "Witnessed test (Category 1, 3, 5)", 2, "el_inspect"),
    ("el_code_a17", "ASME A17.1 / CSA B44 (Safety Code)", 2, "el_code"),
    ("el_code_a17_3", "ASME A17.3 (Existing Elevators)", 2, "el_code"),
    ("el_code_en81", "EN 81 (European elevator standard)", 2, "el_code"),
]


async def ingest_domain_elevator(conn) -> int:
    """Insert or update Elevator Inspection Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_elevator"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_elevator", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_elevator",
    )
    return count
