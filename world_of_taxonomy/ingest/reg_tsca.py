"""TSCA regulatory taxonomy ingester.

Toxic Substances Control Act (15 USC 2601 et seq.).
Authority: US Congress / EPA.
Source: https://www.govinfo.gov/content/pkg/USCODE-2023-title15/html/USCODE-2023-title15-chap53.htm

Data provenance: manual_transcription
License: Public Domain

Total: 25 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_tsca"
_SYSTEM_NAME = "TSCA"
_FULL_NAME = "Toxic Substances Control Act (15 USC 2601 et seq.)"
_VERSION = "1976"
_REGION = "United States"
_AUTHORITY = "US Congress / EPA"
_SOURCE_URL = "https://www.govinfo.gov/content/pkg/USCODE-2023-title15/html/USCODE-2023-title15-chap53.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_TSCA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("title_1", "Title I - Control of Toxic Substances", 1, None),
    ("title_2", "Title II - Asbestos Hazard Emergency Response", 1, None),
    ("title_3", "Title III - Indoor Radon Abatement", 1, None),
    ("title_4", "Title IV - Lead Exposure Reduction", 1, None),
    ("title_5", "Title V - Healthy High-Performance Schools", 1, None),
    ("title_6", "Title VI - Formaldehyde Standards for Composite Wood Products", 1, None),
    ("sec_4", "Sec 4 - Testing of Chemical Substances and Mixtures", 2, "title_1"),
    ("sec_5", "Sec 5 - New Chemicals and Significant New Uses (PMN)", 2, "title_1"),
    ("sec_6", "Sec 6 - Prioritization and Risk Evaluation/Management", 2, "title_1"),
    ("sec_7", "Sec 7 - Imminent Hazards", 2, "title_1"),
    ("sec_8", "Sec 8 - Reporting and Retention of Information", 2, "title_1"),
    ("sec_8a", "Sec 8(a) - Chemical Data Reporting (CDR)", 2, "title_1"),
    ("sec_8b", "Sec 8(b) - TSCA Inventory", 2, "title_1"),
    ("sec_8e", "Sec 8(e) - Substantial Risk Notification", 2, "title_1"),
    ("sec_11", "Sec 11 - Inspections and Subpoenas", 2, "title_1"),
    ("sec_14", "Sec 14 - Confidential Business Information (CBI)", 2, "title_1"),
    ("sec_16", "Sec 16 - Penalties", 2, "title_1"),
    ("sec_18", "Sec 18 - Preemption of State Law", 2, "title_1"),
    ("sec_26", "Sec 26 - Risk Evaluation Procedures and Criteria", 2, "title_1"),
    ("ahera", "AHERA - Asbestos Hazard Emergency Response Act", 2, "title_2"),
    ("ahera_inspection", "Inspection and Management Plans for Schools", 2, "title_2"),
    ("lead_paint", "Residential Lead-Based Paint Hazard Reduction", 2, "title_4"),
    ("lead_rrp", "Renovation, Repair, and Painting (RRP) Rule", 2, "title_4"),
    ("formaldehyde", "Formaldehyde Emission Standards for Composite Wood Products", 2, "title_6"),
    ("lautenberg", "Frank R. Lautenberg Chemical Safety for the 21st Century Act (2016 Amendments)", 2, "title_1"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_tsca(conn) -> int:
    """Ingest TSCA regulatory taxonomy.

    Returns 25 nodes.
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
    for code, title, level, parent in REG_TSCA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_TSCA_NODES:
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
        for code, title, level, parent in REG_TSCA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_TSCA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
