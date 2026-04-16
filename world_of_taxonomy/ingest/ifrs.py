"""Ingest IFRS Standards."""
from __future__ import annotations

_SYSTEM_ROW = ("ifrs", "IFRS Standards", "IFRS Standards", "2024", "Global", "IFRS Foundation")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "IFRS Foundation License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IFRS", "IFRS Standards", 1, None),
    ("IFRS1", "First-time Adoption of IFRS", 2, 'IFRS'),
    ("IFRS2", "Share-based Payment", 2, 'IFRS'),
    ("IFRS3", "Business Combinations", 2, 'IFRS'),
    ("IFRS5", "Non-current Assets Held for Sale", 2, 'IFRS'),
    ("IFRS7", "Financial Instruments: Disclosures", 2, 'IFRS'),
    ("IFRS8", "Operating Segments", 2, 'IFRS'),
    ("IFRS9", "Financial Instruments", 2, 'IFRS'),
    ("IFRS10", "Consolidated Financial Statements", 2, 'IFRS'),
    ("IFRS11", "Joint Arrangements", 2, 'IFRS'),
    ("IFRS12", "Disclosure of Interests", 2, 'IFRS'),
    ("IFRS13", "Fair Value Measurement", 2, 'IFRS'),
    ("IFRS15", "Revenue from Contracts", 2, 'IFRS'),
    ("IFRS16", "Leases", 2, 'IFRS'),
    ("IFRS17", "Insurance Contracts", 2, 'IFRS'),
    ("IAS1", "Presentation of Financial Statements", 2, 'IFRS'),
    ("IAS2", "Inventories", 2, 'IFRS'),
    ("IAS7", "Statement of Cash Flows", 2, 'IFRS'),
    ("IAS8", "Accounting Policies", 2, 'IFRS'),
    ("IAS10", "Events After Reporting Period", 2, 'IFRS'),
    ("IAS12", "Income Taxes", 2, 'IFRS'),
    ("IAS16", "Property, Plant and Equipment", 2, 'IFRS'),
    ("IAS19", "Employee Benefits", 2, 'IFRS'),
    ("IAS21", "Foreign Exchange Rates", 2, 'IFRS'),
    ("IAS23", "Borrowing Costs", 2, 'IFRS'),
    ("IAS24", "Related Party Disclosures", 2, 'IFRS'),
    ("IAS27", "Separate Financial Statements", 2, 'IFRS'),
    ("IAS28", "Investments in Associates", 2, 'IFRS'),
    ("IAS32", "Financial Instruments: Presentation", 2, 'IFRS'),
    ("IAS33", "Earnings Per Share", 2, 'IFRS'),
    ("IAS36", "Impairment of Assets", 2, 'IFRS'),
    ("IAS37", "Provisions and Contingencies", 2, 'IFRS'),
    ("IAS38", "Intangible Assets", 2, 'IFRS'),
    ("IAS40", "Investment Property", 2, 'IFRS'),
]

async def ingest_ifrs(conn) -> int:
    sid, short, full, ver, region, authority = _SYSTEM_ROW
    await conn.execute(
        """INSERT INTO classification_system (id, name, full_name, version, region, authority,
                  source_url, source_date, data_provenance, license)
           VALUES ($1,$2,$3,$4,$5,$6,$7,CURRENT_DATE,$8,$9)
           ON CONFLICT (id) DO UPDATE SET name=$2,full_name=$3,version=$4,region=$5,
                  authority=$6,source_url=$7,source_date=CURRENT_DATE,
                  data_provenance=$8,license=$9""",
        sid, short, full, ver, region, authority,
        _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute("DELETE FROM classification_node WHERE system_id = $1", sid)
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1,$2,$3,$4,$5)""",
            sid, code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, sid)
    return count
