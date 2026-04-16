"""Clean Air Act regulatory taxonomy ingester.

Clean Air Act (42 USC 7401 et seq.).
Authority: US Congress / EPA.
Source: https://www.govinfo.gov/content/pkg/USCODE-2023-title42/html/USCODE-2023-title42-chap85.htm

Data provenance: manual_transcription
License: Public Domain

Total: 28 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_clean_air"
_SYSTEM_NAME = "Clean Air Act"
_FULL_NAME = "Clean Air Act (42 USC 7401 et seq.)"
_VERSION = "1990"
_REGION = "United States"
_AUTHORITY = "US Congress / EPA"
_SOURCE_URL = "https://www.govinfo.gov/content/pkg/USCODE-2023-title42/html/USCODE-2023-title42-chap85.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_CLEAN_AIR_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("title_1", "Title I - Air Pollution Prevention and Control", 1, None),
    ("title_2", "Title II - Emission Standards for Moving Sources", 1, None),
    ("title_3", "Title III - Hazardous Air Pollutants", 1, None),
    ("title_4", "Title IV - Acid Deposition Control (Acid Rain Program)", 1, None),
    ("title_5", "Title V - Permits", 1, None),
    ("title_6", "Title VI - Stratospheric Ozone Protection", 1, None),
    ("naaqs", "National Ambient Air Quality Standards (NAAQS) (Sec 108-109)", 2, "title_1"),
    ("sip", "State Implementation Plans (SIPs) (Sec 110)", 2, "title_1"),
    ("nsps", "New Source Performance Standards (NSPS) (Sec 111)", 2, "title_1"),
    ("neshap", "National Emission Standards for HAPs (NESHAP) (Sec 112)", 2, "title_1"),
    ("psd", "Prevention of Significant Deterioration (PSD) (Sec 160-169B)", 2, "title_1"),
    ("nonattainment", "Nonattainment Area Requirements (Sec 171-193)", 2, "title_1"),
    ("visibility", "Visibility Protection (Regional Haze) (Sec 169A-169B)", 2, "title_1"),
    ("mobile_sources", "Motor Vehicle Emission Standards (Sec 202-219)", 2, "title_2"),
    ("fuel_standards", "Fuel and Fuel Additive Standards (Sec 211)", 2, "title_2"),
    ("cafe", "Clean Alternative Fuels (Sec 241-250)", 2, "title_2"),
    ("aircraft_emissions", "Aircraft Emission Standards (Sec 231-234)", 2, "title_2"),
    ("hap_list", "Hazardous Air Pollutant List (189 HAPs) (Sec 112(b))", 2, "title_3"),
    ("mact", "Maximum Achievable Control Technology (MACT) (Sec 112(d))", 2, "title_3"),
    ("residual_risk", "Residual Risk Standards (Sec 112(f))", 2, "title_3"),
    ("accidental_release", "Accidental Release Prevention (Sec 112(r))", 2, "title_3"),
    ("so2_allowances", "SO2 Allowance Trading Program", 2, "title_4"),
    ("nox_reduction", "NOx Reduction Program", 2, "title_4"),
    ("monitoring_reporting", "Continuous Emission Monitoring (CEM)", 2, "title_4"),
    ("operating_permits", "Operating Permit Requirements (Sec 501-507)", 2, "title_5"),
    ("permit_fees", "Permit Fees (Sec 502(b)(3))", 2, "title_5"),
    ("ozone_depleting", "Ozone-Depleting Substance Phaseout (Sec 601-618)", 2, "title_6"),
    ("hfc_phasedown", "HFC Phasedown (AIM Act Amendments)", 2, "title_6"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_clean_air(conn) -> int:
    """Ingest Clean Air Act regulatory taxonomy.

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
    for code, title, level, parent in REG_CLEAN_AIR_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_CLEAN_AIR_NODES:
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
        for code, title, level, parent in REG_CLEAN_AIR_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_CLEAN_AIR_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
