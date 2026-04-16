"""Ingest FATF International Standards on Combating Money Laundering and Terrorist Financing."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_fatf",
    "FATF 40 Recommendations",
    "FATF International Standards on Combating Money Laundering and Terrorist Financing",
    "2023",
    "Global",
    "Financial Action Task Force (FATF)",
)
_SOURCE_URL = "https://www.fatf-gafi.org/en/recommendations.html"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("aml", "A - AML/CFT Policies and Coordination", 1, None),
    ("ml", "B - Money Laundering and Confiscation", 1, None),
    ("tf", "C - Terrorist Financing and Proliferation", 1, None),
    ("prev", "D - Preventive Measures", 1, None),
    ("inst", "E - Transparency of Legal Persons", 1, None),
    ("auth", "F - Powers and Responsibilities of Authorities", 1, None),
    ("coop", "G - International Cooperation", 1, None),
    ("r1", "R.1 Assessing risks and applying a risk-based approach", 2, "aml"),
    ("r2", "R.2 National cooperation and coordination", 2, "aml"),
    ("r3", "R.3 Money laundering offence", 2, "ml"),
    ("r4", "R.4 Confiscation and provisional measures", 2, "ml"),
    ("r5", "R.5 Terrorist financing offence", 2, "tf"),
    ("r6", "R.6 Targeted financial sanctions (terrorism)", 2, "tf"),
    ("r7", "R.7 Targeted financial sanctions (proliferation)", 2, "tf"),
    ("r8", "R.8 Non-profit organisations", 2, "tf"),
    ("r10", "R.10 Customer due diligence", 2, "prev"),
    ("r11", "R.11 Record keeping", 2, "prev"),
    ("r12", "R.12 Politically exposed persons", 2, "prev"),
    ("r15", "R.15 New technologies (virtual assets)", 2, "prev"),
    ("r16", "R.16 Wire transfers", 2, "prev"),
    ("r20", "R.20 Reporting of suspicious transactions", 2, "prev"),
    ("r24", "R.24 Transparency of legal persons", 2, "inst"),
    ("r25", "R.25 Transparency of legal arrangements", 2, "inst"),
    ("r26", "R.26 Regulation and supervision of financial institutions", 2, "auth"),
    ("r29", "R.29 Financial intelligence units", 2, "auth"),
    ("r30", "R.30 Responsibilities of law enforcement", 2, "auth"),
    ("r36", "R.36 International instruments", 2, "coop"),
    ("r37", "R.37 Mutual legal assistance", 2, "coop"),
    ("r40", "R.40 Other forms of international cooperation", 2, "coop"),
]


async def ingest_reg_fatf(conn) -> int:
    """Insert or update FATF 40 Recommendations system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_fatf"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_fatf", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_fatf",
    )
    return count
