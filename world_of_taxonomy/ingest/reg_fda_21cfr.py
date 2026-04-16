"""FDA 21 CFR Parts regulatory taxonomy ingester.

FDA Regulations - Title 21 Code of Federal Regulations Key Parts.
Authority: US Food and Drug Administration (FDA).
Source: https://www.ecfr.gov/current/title-21

Data provenance: manual_transcription
License: Public Domain

Total: 28 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_fda_21cfr"
_SYSTEM_NAME = "FDA 21 CFR Parts"
_FULL_NAME = "FDA Regulations - Title 21 Code of Federal Regulations Key Parts"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "US Food and Drug Administration (FDA)"
_SOURCE_URL = "https://www.ecfr.gov/current/title-21"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_FDA_21CFR_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("drugs", "Drug Regulations", 1, None),
    ("devices", "Medical Device Regulations", 1, None),
    ("biologics", "Biologics Regulations", 1, None),
    ("food", "Food Safety Regulations", 1, None),
    ("quality", "Quality System Regulations", 1, None),
    ("electronic", "Electronic Records and Signatures", 1, None),
    ("part_210", "Part 210 - Current Good Manufacturing Practice (cGMP) in Manufacturing", 2, "drugs"),
    ("part_211", "Part 211 - cGMP for Finished Pharmaceuticals", 2, "drugs"),
    ("part_312", "Part 312 - Investigational New Drug (IND) Application", 2, "drugs"),
    ("part_314", "Part 314 - New Drug Application (NDA)", 2, "drugs"),
    ("part_320", "Part 320 - Bioavailability and Bioequivalence", 2, "drugs"),
    ("part_807", "Part 807 - Establishment Registration and Device Listing", 2, "devices"),
    ("part_812", "Part 812 - Investigational Device Exemptions (IDE)", 2, "devices"),
    ("part_814", "Part 814 - Premarket Approval (PMA)", 2, "devices"),
    ("part_820", "Part 820 - Quality System Regulation (QSR)", 2, "devices"),
    ("part_860", "Part 860 - Medical Device Classification", 2, "devices"),
    ("part_890", "Part 890 - Physical Medicine Devices", 2, "devices"),
    ("part_600", "Part 600 - Biological Products: General", 2, "biologics"),
    ("part_601", "Part 601 - Licensing (BLA)", 2, "biologics"),
    ("part_610", "Part 610 - General Biological Products Standards", 2, "biologics"),
    ("part_110", "Part 110 - Current Good Manufacturing Practice (Food)", 2, "food"),
    ("part_117", "Part 117 - Current Good Manufacturing Practice (FSMA)", 2, "food"),
    ("part_120", "Part 120 - Hazard Analysis and Critical Control Points (HACCP) for Juice", 2, "food"),
    ("part_11", "Part 11 - Electronic Records; Electronic Signatures", 2, "electronic"),
    ("part_58", "Part 58 - Good Laboratory Practice (GLP)", 2, "quality"),
    ("part_11_scope", "Part 11 Subpart A - General Provisions", 2, "electronic"),
    ("part_11_records", "Part 11 Subpart B - Electronic Records", 2, "electronic"),
    ("part_11_sig", "Part 11 Subpart C - Electronic Signatures", 2, "electronic"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_fda_21cfr(conn) -> int:
    """Ingest FDA 21 CFR Parts regulatory taxonomy.

    Returns 28 nodes.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count,
                source_url, data_provenance, license)
           VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)
           ON CONFLICT (id) DO UPDATE
               SET node_count = 0,
                   source_url = EXCLUDED.source_url,
                   data_provenance = EXCLUDED.data_provenance,
                   license = EXCLUDED.license""",
        *_SYSTEM_ROW, _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )

    leaf_codes = set()
    parent_codes = set()
    for code, title, level, parent in REG_FDA_21CFR_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_FDA_21CFR_NODES:
        if code not in parent_codes:
            leaf_codes.add(code)

    rows = [
        (
            _SYSTEM_ID,
            code,
            title,
            level,
            parent,
            code.split("_")[0],
            code in leaf_codes,
        )
        for code, title, level, parent in REG_FDA_21CFR_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_FDA_21CFR_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
