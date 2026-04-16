"""SOX regulatory taxonomy ingester.

Sarbanes-Oxley Act of 2002.
Authority: US Congress / SEC.
Source: https://www.govinfo.gov/content/pkg/PLAW-107publ204/html/PLAW-107publ204.htm

Data provenance: manual_transcription
License: Public Domain

Total: 58 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_sox"
_SYSTEM_NAME = "SOX"
_FULL_NAME = "Sarbanes-Oxley Act of 2002"
_VERSION = "2002"
_REGION = "United States"
_AUTHORITY = "US Congress / SEC"
_SOURCE_URL = "https://www.govinfo.gov/content/pkg/PLAW-107publ204/html/PLAW-107publ204.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_SOX_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("title_1", "Title I - Public Company Accounting Oversight Board", 1, None),
    ("title_2", "Title II - Auditor Independence", 1, None),
    ("title_3", "Title III - Corporate Responsibility", 1, None),
    ("title_4", "Title IV - Enhanced Financial Disclosures", 1, None),
    ("title_5", "Title V - Analyst Conflicts of Interest", 1, None),
    ("title_6", "Title VI - Commission Resources and Authority", 1, None),
    ("title_7", "Title VII - Studies and Reports", 1, None),
    ("title_8", "Title VIII - Corporate and Criminal Fraud Accountability", 1, None),
    ("title_9", "Title IX - White-Collar Crime Penalty Enhancements", 1, None),
    ("title_10", "Title X - Corporate Tax Returns", 1, None),
    ("title_11", "Title XI - Corporate Fraud and Accountability", 1, None),
    ("sec_101", "Sec 101 - Establishment; Administrative Provisions", 2, "title_1"),
    ("sec_102", "Sec 102 - Registration with the Board", 2, "title_1"),
    ("sec_103", "Sec 103 - Auditing, Quality Control, and Independence Standards", 2, "title_1"),
    ("sec_104", "Sec 104 - Inspections of Registered Public Accounting Firms", 2, "title_1"),
    ("sec_105", "Sec 105 - Investigations and Disciplinary Proceedings", 2, "title_1"),
    ("sec_106", "Sec 106 - Foreign Public Accounting Firms", 2, "title_1"),
    ("sec_109", "Sec 109 - Funding", 2, "title_1"),
    ("sec_201", "Sec 201 - Services Outside Scope of Practice of Auditors", 2, "title_2"),
    ("sec_202", "Sec 202 - Preapproval Requirements", 2, "title_2"),
    ("sec_203", "Sec 203 - Audit Partner Rotation", 2, "title_2"),
    ("sec_204", "Sec 204 - Auditor Reports to Audit Committees", 2, "title_2"),
    ("sec_206", "Sec 206 - Conflicts of Interest", 2, "title_2"),
    ("sec_301", "Sec 301 - Public Company Audit Committees", 2, "title_3"),
    ("sec_302", "Sec 302 - Corporate Responsibility for Financial Reports", 2, "title_3"),
    ("sec_303", "Sec 303 - Improper Influence on Conduct of Audits", 2, "title_3"),
    ("sec_304", "Sec 304 - Forfeiture of Certain Bonuses and Profits", 2, "title_3"),
    ("sec_305", "Sec 305 - Officer and Director Bars and Penalties", 2, "title_3"),
    ("sec_306", "Sec 306 - Insider Trades During Pension Fund Blackout Periods", 2, "title_3"),
    ("sec_401", "Sec 401 - Disclosures in Periodic Reports", 2, "title_4"),
    ("sec_402", "Sec 402 - Enhanced Conflict of Interest Provisions", 2, "title_4"),
    ("sec_403", "Sec 403 - Disclosures of Transactions Involving Management", 2, "title_4"),
    ("sec_404", "Sec 404 - Management Assessment of Internal Controls", 2, "title_4"),
    ("sec_406", "Sec 406 - Code of Ethics for Senior Financial Officers", 2, "title_4"),
    ("sec_407", "Sec 407 - Disclosure of Audit Committee Financial Expert", 2, "title_4"),
    ("sec_409", "Sec 409 - Real Time Issuer Disclosures", 2, "title_4"),
    ("sec_501", "Sec 501 - Treatment of Securities Analysts by Registered Securities Associations", 2, "title_5"),
    ("sec_601", "Sec 601 - Authorization of Appropriations", 2, "title_6"),
    ("sec_602", "Sec 602 - SEC Appearance and Practice Provisions", 2, "title_6"),
    ("sec_603", "Sec 603 - Federal Court Authority to Impose Penny Stock Bars", 2, "title_6"),
    ("sec_701", "Sec 701 - GAO Study and Report on Consolidation of Accounting Firms", 2, "title_7"),
    ("sec_702", "Sec 702 - Commission Study on Credit Rating Agencies", 2, "title_7"),
    ("sec_801", "Sec 801 - Short Title (Corporate and Criminal Fraud Accountability Act)", 2, "title_8"),
    ("sec_802", "Sec 802 - Criminal Penalties for Altering Documents", 2, "title_8"),
    ("sec_803", "Sec 803 - Debts Nondischargeable if Incurred in Violation of Securities Laws", 2, "title_8"),
    ("sec_804", "Sec 804 - Statute of Limitations for Securities Fraud", 2, "title_8"),
    ("sec_806", "Sec 806 - Whistleblower Protection", 2, "title_8"),
    ("sec_901", "Sec 901 - Short Title (White-Collar Crime Penalty Enhancements Act)", 2, "title_9"),
    ("sec_902", "Sec 902 - Attempts and Conspiracies to Commit Criminal Fraud", 2, "title_9"),
    ("sec_903", "Sec 903 - Criminal Penalties for Mail and Wire Fraud", 2, "title_9"),
    ("sec_906", "Sec 906 - Corporate Responsibility for Financial Reports (Criminal)", 2, "title_9"),
    ("sec_1001", "Sec 1001 - Sense of the Senate Regarding CEO Signing Corporate Tax Returns", 2, "title_10"),
    ("sec_1101", "Sec 1101 - Short Title (Corporate Fraud Accountability Act)", 2, "title_11"),
    ("sec_1102", "Sec 1102 - Tampering with Records or Impeding Official Proceedings", 2, "title_11"),
    ("sec_1103", "Sec 1103 - Temporary Freeze Authority for SEC", 2, "title_11"),
    ("sec_1105", "Sec 1105 - SEC Authority to Prohibit Persons from Serving as Officers or Directors", 2, "title_11"),
    ("sec_1106", "Sec 1106 - Increased Criminal Penalties under Securities Exchange Act", 2, "title_11"),
    ("sec_1107", "Sec 1107 - Retaliation Against Informants", 2, "title_11"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_sox(conn) -> int:
    """Ingest SOX regulatory taxonomy.

    Returns 58 nodes.
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
    for code, title, level, parent in REG_SOX_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_SOX_NODES:
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
        for code, title, level, parent in REG_SOX_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_SOX_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
