"""Ingest IEC 31010:2019 Risk Management - Risk Assessment Techniques."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_iso_31010",
    "ISO 31010:2019",
    "IEC 31010:2019 Risk Management - Risk Assessment Techniques",
    "2019",
    "Global",
    "International Organization for Standardization (ISO)",
)
_SOURCE_URL = "https://www.iso.org/standard/72140.html"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ISO copyright)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cl_4", "Clause 4 - Core Concepts", 1, None),
    ("cl_5", "Clause 5 - Risk Assessment Process", 1, None),
    ("cl_6", "Clause 6 - Selecting Risk Assessment Techniques", 1, None),
    ("annex_a", "Annex A - Comparison of Risk Assessment Techniques", 1, None),
    ("annex_b", "Annex B - Risk Assessment Techniques", 1, None),
    ("4_1", "4.1 Purpose and benefits of risk assessment", 2, "cl_4"),
    ("4_2", "4.2 Risk assessment and the risk management framework", 2, "cl_4"),
    ("4_3", "4.3 Risk assessment and the risk management process", 2, "cl_4"),
    ("5_1", "5.1 Overview", 2, "cl_5"),
    ("5_2", "5.2 Risk identification", 2, "cl_5"),
    ("5_3", "5.3 Risk analysis", 2, "cl_5"),
    ("5_4", "5.4 Risk evaluation", 2, "cl_5"),
    ("b1", "B.1 Brainstorming", 2, "annex_b"),
    ("b2", "B.2 Structured interviews", 2, "annex_b"),
    ("b3", "B.3 Delphi technique", 2, "annex_b"),
    ("b4", "B.4 Checklists", 2, "annex_b"),
    ("b5", "B.5 Preliminary hazard analysis", 2, "annex_b"),
    ("b6", "B.6 HAZOP study", 2, "annex_b"),
    ("b7", "B.7 HACCP", 2, "annex_b"),
    ("b8", "B.8 FMEA and FMECA", 2, "annex_b"),
    ("b9", "B.9 Fault tree analysis", 2, "annex_b"),
    ("b10", "B.10 Event tree analysis", 2, "annex_b"),
    ("b11", "B.11 Cause-and-consequence analysis", 2, "annex_b"),
    ("b12", "B.12 Bow tie analysis", 2, "annex_b"),
    ("b13", "B.13 Monte Carlo simulation", 2, "annex_b"),
    ("b14", "B.14 Bayesian analysis", 2, "annex_b"),
]


async def ingest_reg_iso_31010(conn) -> int:
    """Insert or update ISO 31010:2019 system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_iso_31010"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_iso_31010", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_iso_31010",
    )
    return count
