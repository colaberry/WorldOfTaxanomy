"""EU Batteries Regulation regulatory taxonomy ingester.

Regulation (EU) 2023/1542 concerning batteries and waste batteries.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2023/1542

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 18 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_eu_batteries"
_SYSTEM_NAME = "EU Batteries Regulation"
_FULL_NAME = "Regulation (EU) 2023/1542 concerning batteries and waste batteries"
_VERSION = "2023"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2023/1542"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_EU_BATTERIES_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("sustainability", "Sustainability and Safety Requirements", 1, None),
    ("labelling", "Labelling and Information", 1, None),
    ("due_diligence", "Due Diligence and Supply Chain", 1, None),
    ("end_of_life", "End-of-Life Management", 1, None),
    ("passport", "Battery Passport", 1, None),
    ("carbon_fp", "Carbon Footprint Declaration and Thresholds", 2, "sustainability"),
    ("recycled_content", "Recycled Content Requirements", 2, "sustainability"),
    ("performance", "Performance and Durability Requirements", 2, "sustainability"),
    ("hazardous_restrict", "Hazardous Substance Restrictions", 2, "sustainability"),
    ("qr_label", "QR Code and Labelling Requirements", 2, "labelling"),
    ("capacity_label", "Capacity and Performance Labels", 2, "labelling"),
    ("dd_policy", "Due Diligence Policy (Art 48)", 2, "due_diligence"),
    ("risk_management", "Supply Chain Risk Management", 2, "due_diligence"),
    ("collection", "Collection Targets", 2, "end_of_life"),
    ("recycling", "Recycling Efficiency Targets", 2, "end_of_life"),
    ("material_recovery", "Material Recovery Targets (Co, Li, Ni, Cu)", 2, "end_of_life"),
    ("digital_passport", "Digital Battery Passport Requirements", 2, "passport"),
    ("data_access", "Data Access and Interoperability", 2, "passport"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_eu_batteries(conn) -> int:
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
    for c, t, l, p in REG_EU_BATTERIES_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_EU_BATTERIES_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_EU_BATTERIES_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_EU_BATTERIES_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
