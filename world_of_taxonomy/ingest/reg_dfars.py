"""DFARS regulatory taxonomy ingester.

Defense Federal Acquisition Regulation Supplement (48 CFR Chapter 2).
Authority: US Department of Defense (DoD).
Source: https://www.ecfr.gov/current/title-48/chapter-2

Data provenance: manual_transcription
License: Public Domain

Total: 25 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_dfars"
_SYSTEM_NAME = "DFARS"
_FULL_NAME = "Defense Federal Acquisition Regulation Supplement (48 CFR Chapter 2)"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "US Department of Defense (DoD)"
_SOURCE_URL = "https://www.ecfr.gov/current/title-48/chapter-2"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_DFARS_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("part_201", "Part 201 - Federal Acquisition Regulations System", 1, None),
    ("part_204", "Part 204 - Administrative and Information Matters", 1, None),
    ("part_207", "Part 207 - Acquisition Planning", 1, None),
    ("part_209", "Part 209 - Contractor Qualifications", 1, None),
    ("part_211", "Part 211 - Describing Agency Needs", 1, None),
    ("part_212", "Part 212 - Acquisition of Commercial Products and Services", 1, None),
    ("part_215", "Part 215 - Contracting by Negotiation", 1, None),
    ("part_217", "Part 217 - Special Contracting Methods", 1, None),
    ("part_219", "Part 219 - Small Business Programs", 1, None),
    ("part_225", "Part 225 - Foreign Acquisition (Berry Amendment, ITAR)", 1, None),
    ("part_227", "Part 227 - Patents, Data, and Copyrights", 1, None),
    ("part_231", "Part 231 - Contract Cost Principles and Procedures", 1, None),
    ("part_232", "Part 232 - Contract Financing", 1, None),
    ("part_234", "Part 234 - Major System Acquisition", 1, None),
    ("part_235", "Part 235 - Research and Development Contracting", 1, None),
    ("part_237", "Part 237 - Service Contracting", 1, None),
    ("part_239", "Part 239 - Acquisition of Information Technology", 1, None),
    ("part_242", "Part 242 - Contract Administration and Audit Services", 1, None),
    ("part_245", "Part 245 - Government Property", 1, None),
    ("part_246", "Part 246 - Quality Assurance", 1, None),
    ("part_252", "Part 252 - Solicitation Provisions and Contract Clauses", 1, None),
    ("clause_7012", "DFARS 252.204-7012 - Safeguarding Covered Defense Information (CDI)", 2, "part_204"),
    ("clause_7019", "DFARS 252.204-7019 - NIST SP 800-171 DoD Assessment", 2, "part_204"),
    ("clause_7020", "DFARS 252.204-7020 - NIST SP 800-171 DoD Assessment Requirements", 2, "part_204"),
    ("clause_7021", "DFARS 252.204-7021 - CMMC Requirements", 2, "part_204"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_dfars(conn) -> int:
    """Ingest DFARS regulatory taxonomy.

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
    for code, title, level, parent in REG_DFARS_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_DFARS_NODES:
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
        for code, title, level, parent in REG_DFARS_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_DFARS_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
