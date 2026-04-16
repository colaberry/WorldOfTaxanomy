"""Ingest Basel III/IV International Banking Regulatory Framework."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_basel3",
    "Basel III/IV",
    "Basel III/IV International Banking Regulatory Framework",
    "2023",
    "Global",
    "Basel Committee on Banking Supervision (BCBS)",
)
_SOURCE_URL = "https://www.bis.org/bcbs/basel3.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (BIS)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pillar_1", "Pillar 1 - Minimum Capital Requirements", 1, None),
    ("pillar_2", "Pillar 2 - Supervisory Review Process", 1, None),
    ("pillar_3", "Pillar 3 - Market Discipline", 1, None),
    ("liq", "Liquidity Standards", 1, None),
    ("lev", "Leverage Ratio", 1, None),
    ("1_1", "Credit Risk - Standardised Approach", 2, "pillar_1"),
    ("1_2", "Credit Risk - Internal Ratings-Based Approach", 2, "pillar_1"),
    ("1_3", "Market Risk - Fundamental Review of Trading Book", 2, "pillar_1"),
    ("1_4", "Operational Risk - Standardised Measurement Approach", 2, "pillar_1"),
    ("1_5", "Credit Valuation Adjustment (CVA) Risk", 2, "pillar_1"),
    ("1_6", "Output Floor", 2, "pillar_1"),
    ("1_7", "Capital Buffers (CCyB, G-SIB, D-SIB)", 2, "pillar_1"),
    ("2_1", "ICAAP - Internal Capital Adequacy Assessment", 2, "pillar_2"),
    ("2_2", "ILAAP - Internal Liquidity Adequacy Assessment", 2, "pillar_2"),
    ("2_3", "Supervisory Stress Testing", 2, "pillar_2"),
    ("2_4", "Interest Rate Risk in the Banking Book (IRRBB)", 2, "pillar_2"),
    ("3_1", "Regulatory Disclosure Requirements", 2, "pillar_3"),
    ("3_2", "Key Metrics and Overview", 2, "pillar_3"),
    ("3_3", "Composition of Capital Disclosures", 2, "pillar_3"),
    ("liq_1", "Liquidity Coverage Ratio (LCR)", 2, "liq"),
    ("liq_2", "Net Stable Funding Ratio (NSFR)", 2, "liq"),
    ("liq_3", "Liquidity Monitoring Tools", 2, "liq"),
    ("lev_1", "Leverage Ratio Requirements", 2, "lev"),
    ("lev_2", "G-SIB Leverage Ratio Buffer", 2, "lev"),
]


async def ingest_reg_basel3(conn) -> int:
    """Insert or update Basel III/IV system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_basel3"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_basel3", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_basel3",
    )
    return count
