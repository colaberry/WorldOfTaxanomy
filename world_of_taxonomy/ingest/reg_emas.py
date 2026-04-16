"""EMAS regulatory taxonomy ingester.

EU Eco-Management and Audit Scheme (EC) No 1221/2009.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2009/1221

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 25 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_emas"
_SYSTEM_NAME = "EMAS"
_FULL_NAME = "EU Eco-Management and Audit Scheme (EC) No 1221/2009"
_VERSION = "2009"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2009/1221"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_EMAS_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("registration", "Registration Process", 1, None),
    ("env_review", "Environmental Review", 1, None),
    ("ems", "Environmental Management System", 1, None),
    ("statement", "Environmental Statement", 1, None),
    ("verification", "Verification and Validation", 1, None),
    ("indicators", "Core Indicators", 1, None),
    ("initial_review", "Initial Environmental Review (Annex I)", 2, "env_review"),
    ("legal_compliance", "Legal Compliance Assessment", 2, "env_review"),
    ("aspects", "Identification of Environmental Aspects", 2, "env_review"),
    ("iso_14001", "ISO 14001 Requirements (Annex II)", 2, "ems"),
    ("policy", "Environmental Policy", 2, "ems"),
    ("objectives", "Objectives and Targets", 2, "ems"),
    ("operational_control", "Operational Control", 2, "ems"),
    ("emergency_prep", "Emergency Preparedness", 2, "ems"),
    ("annual_statement", "Annual Environmental Statement (Art 7, Annex IV)", 2, "statement"),
    ("core_data", "Core Indicator Data Reporting", 2, "statement"),
    ("validated_statement", "Validated Statement Publication", 2, "statement"),
    ("verifier", "EMAS Environmental Verifier (Art 18-27)", 2, "verification"),
    ("accreditation", "Verifier Accreditation", 2, "verification"),
    ("energy_eff", "Energy Efficiency Indicator", 2, "indicators"),
    ("material_eff", "Material Efficiency Indicator", 2, "indicators"),
    ("water_use", "Water Use Indicator", 2, "indicators"),
    ("waste_gen", "Waste Generation Indicator", 2, "indicators"),
    ("land_biodiv", "Land Use and Biodiversity Indicator", 2, "indicators"),
    ("emissions", "Emissions Indicator", 2, "indicators"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_emas(conn) -> int:
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
    leaf_codes, parent_codes = set(), set()
    for c, t, l, p in REG_EMAS_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_EMAS_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_EMAS_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_EMAS_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
