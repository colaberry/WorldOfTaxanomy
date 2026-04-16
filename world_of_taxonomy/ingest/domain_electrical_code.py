"""Ingest Electrical Code and System Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_electrical_code",
    "Electrical Code Types",
    "Electrical Code and System Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ec_sys", "Electrical Systems", 1, None),
    ("ec_code", "Code Standards", 1, None),
    ("ec_safe", "Safety Requirements", 1, None),
    ("ec_sys_power", "Power distribution (switchgear, panelboards)", 2, "ec_sys"),
    ("ec_sys_light", "Lighting systems (LED, fluorescent, controls)", 2, "ec_sys"),
    ("ec_sys_emerg", "Emergency and standby power", 2, "ec_sys"),
    ("ec_sys_low", "Low-voltage systems (data, telecom)", 2, "ec_sys"),
    ("ec_code_nec", "National Electrical Code (NEC / NFPA 70)", 2, "ec_code"),
    ("ec_code_nfpa79", "NFPA 79 (Industrial Machinery)", 2, "ec_code"),
    ("ec_code_nfpa70e", "NFPA 70E (Electrical Safety in Workplace)", 2, "ec_code"),
    ("ec_code_ieee", "IEEE standards (grounding, power quality)", 2, "ec_code"),
    ("ec_safe_gfci", "GFCI protection requirements", 2, "ec_safe"),
    ("ec_safe_afci", "AFCI protection requirements", 2, "ec_safe"),
    ("ec_safe_surge", "Surge protection (SPD)", 2, "ec_safe"),
    ("ec_safe_arc", "Arc flash analysis and labeling", 2, "ec_safe"),
    ("ec_safe_lockout", "Lockout/tagout (LOTO) procedures", 2, "ec_safe"),
]


async def ingest_domain_electrical_code(conn) -> int:
    """Insert or update Electrical Code Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_electrical_code"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_electrical_code", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_electrical_code",
    )
    return count
