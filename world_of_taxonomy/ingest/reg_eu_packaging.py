"""EU Packaging Regulation regulatory taxonomy ingester.

Regulation (EU) 2025/40 on packaging and packaging waste.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2025/40

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 19 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_eu_packaging"
_SYSTEM_NAME = "EU Packaging Regulation"
_FULL_NAME = "Regulation (EU) 2025/40 on packaging and packaging waste"
_VERSION = "2025"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2025/40"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_EU_PACKAGING_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("sustainability", "Sustainability Requirements", 1, None),
    ("labelling", "Labelling and Information", 1, None),
    ("targets", "Reduction and Recycling Targets", 1, None),
    ("epr", "Extended Producer Responsibility", 1, None),
    ("reuse", "Reuse and Refill", 1, None),
    ("recyclability", "Recyclability Requirements", 2, "sustainability"),
    ("recycled_content", "Minimum Recycled Content", 2, "sustainability"),
    ("minimisation", "Packaging Minimisation", 2, "sustainability"),
    ("hazardous", "Hazardous Substance Restrictions", 2, "sustainability"),
    ("sorting_label", "Sorting Labels and QR Codes", 2, "labelling"),
    ("material_id", "Material Identification", 2, "labelling"),
    ("reusability_label", "Reusability Information", 2, "labelling"),
    ("reduction_targets", "Overall Packaging Waste Reduction Targets", 2, "targets"),
    ("recycling_targets", "Recycling Targets by Material", 2, "targets"),
    ("plastic_targets", "Plastic Recycling Targets", 2, "targets"),
    ("epr_scheme", "EPR Scheme Requirements", 2, "epr"),
    ("eco_modulation", "Eco-Modulated Fees", 2, "epr"),
    ("reuse_targets", "Reuse Targets by Packaging Type", 2, "reuse"),
    ("refill_obligations", "Refill Obligations for Beverage Containers", 2, "reuse"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_eu_packaging(conn) -> int:
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
    for c, t, l, p in REG_EU_PACKAGING_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_EU_PACKAGING_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_EU_PACKAGING_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_EU_PACKAGING_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
