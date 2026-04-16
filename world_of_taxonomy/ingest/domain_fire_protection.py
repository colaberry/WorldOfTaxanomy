"""Ingest Fire Protection System Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_fire_protection",
    "Fire Protection Types",
    "Fire Protection System Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("fp_detect", "Detection Systems", 1, None),
    ("fp_suppress", "Suppression Systems", 1, None),
    ("fp_passive", "Passive Protection", 1, None),
    ("fp_detect_smoke", "Smoke detectors (ionization, photoelectric)", 2, "fp_detect"),
    ("fp_detect_heat", "Heat detectors (fixed temp, rate-of-rise)", 2, "fp_detect"),
    ("fp_detect_flame", "Flame detectors (UV, IR)", 2, "fp_detect"),
    ("fp_detect_gas", "Gas detection systems", 2, "fp_detect"),
    ("fp_detect_alarm", "Fire alarm control panel (FACP)", 2, "fp_detect"),
    ("fp_suppress_sprink", "Automatic sprinkler systems (wet, dry, deluge)", 2, "fp_suppress"),
    ("fp_suppress_foam", "Foam suppression systems", 2, "fp_suppress"),
    ("fp_suppress_clean", "Clean agent systems (FM-200, Novec)", 2, "fp_suppress"),
    ("fp_suppress_co2", "CO2 suppression systems", 2, "fp_suppress"),
    ("fp_suppress_standpipe", "Standpipe and hose systems", 2, "fp_suppress"),
    ("fp_passive_rated", "Fire-rated assemblies (walls, floors, doors)", 2, "fp_passive"),
    ("fp_passive_firestop", "Firestopping and penetration seals", 2, "fp_passive"),
    ("fp_passive_smoke", "Smoke barriers and partitions", 2, "fp_passive"),
]


async def ingest_domain_fire_protection(conn) -> int:
    """Insert or update Fire Protection Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_fire_protection"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_fire_protection", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_fire_protection",
    )
    return count
