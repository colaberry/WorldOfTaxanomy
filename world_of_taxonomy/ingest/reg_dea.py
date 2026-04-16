"""DEA Schedules regulatory taxonomy ingester.

Drug Enforcement Administration Controlled Substance Schedules (21 CFR Parts 1301-1321).
Authority: US Drug Enforcement Administration (DEA).
Source: https://www.ecfr.gov/current/title-21/chapter-II

Data provenance: manual_transcription
License: Public Domain

Total: 25 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_dea"
_SYSTEM_NAME = "DEA Schedules"
_FULL_NAME = "Drug Enforcement Administration Controlled Substance Schedules (21 CFR Parts 1301-1321)"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "US Drug Enforcement Administration (DEA)"
_SOURCE_URL = "https://www.ecfr.gov/current/title-21/chapter-II"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_DEA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("schedule_i", "Schedule I - High Abuse Potential, No Accepted Medical Use", 1, None),
    ("schedule_ii", "Schedule II - High Abuse Potential, Accepted Medical Use", 1, None),
    ("schedule_iii", "Schedule III - Moderate Abuse Potential", 1, None),
    ("schedule_iv", "Schedule IV - Low Abuse Potential", 1, None),
    ("schedule_v", "Schedule V - Lowest Abuse Potential", 1, None),
    ("registration", "DEA Registration Requirements", 1, None),
    ("security", "Security and Control Requirements", 1, None),
    ("reporting", "Reporting and Recordkeeping", 1, None),
    ("si_opioids", "Schedule I Opioids (e.g., heroin)", 2, "schedule_i"),
    ("si_hallucinogens", "Schedule I Hallucinogens (e.g., LSD, psilocybin)", 2, "schedule_i"),
    ("si_stimulants", "Schedule I Stimulants (e.g., MDMA)", 2, "schedule_i"),
    ("si_depressants", "Schedule I Depressants (e.g., GHB analogs)", 2, "schedule_i"),
    ("si_cannabis", "Schedule I Cannabinoids (marijuana - federal)", 2, "schedule_i"),
    ("sii_opioids", "Schedule II Opioids (e.g., fentanyl, oxycodone, morphine)", 2, "schedule_ii"),
    ("sii_stimulants", "Schedule II Stimulants (e.g., amphetamine, methylphenidate)", 2, "schedule_ii"),
    ("sii_depressants", "Schedule II Depressants (e.g., pentobarbital)", 2, "schedule_ii"),
    ("siii_anabolic", "Schedule III Anabolic Steroids", 2, "schedule_iii"),
    ("siii_compounds", "Schedule III Compounds (e.g., buprenorphine, ketamine)", 2, "schedule_iii"),
    ("siv_benzo", "Schedule IV Benzodiazepines (e.g., alprazolam, diazepam)", 2, "schedule_iv"),
    ("sv_cough", "Schedule V Cough Preparations", 2, "schedule_v"),
    ("reg_practitioner", "Practitioner Registration (1301.11)", 2, "registration"),
    ("reg_pharmacy", "Pharmacy Registration", 2, "registration"),
    ("reg_manufacturer", "Manufacturer Registration", 2, "registration"),
    ("sec_physical", "Physical Security Requirements (1301.71-76)", 2, "security"),
    ("sec_employee", "Employee Screening (1301.90-93)", 2, "security"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_dea(conn) -> int:
    """Ingest DEA Schedules regulatory taxonomy.

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
    for code, title, level, parent in REG_DEA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_DEA_NODES:
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
        for code, title, level, parent in REG_DEA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_DEA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
