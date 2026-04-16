"""Basel III/IV Exposure Category Classification ingester.

The Basel III/IV framework (BCBS 189/424) requires banks to categorise credit
exposures for risk-weighted asset calculation. These categories are used by
banks globally to classify counterparty exposures for capital adequacy
reporting to prudential regulators (Fed, PRA, BaFin, ECB/SSM, etc.).
"""
from __future__ import annotations

SYSTEM_ID = "basel_exposure"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # SA Exposure Classes (level 1 - Standardised Approach)
    ("SA",   "Standardised Approach Exposure Classes",        1, None),
    ("IRB",  "Internal Ratings-Based Approach Classes",       1, None),
    ("MKT",  "Market Risk Exposure Classes",                  1, None),
    ("OP",   "Operational Risk Approaches",                   1, None),
    # SA exposure classes (level 2)
    ("SA-SOV",  "Sovereigns and Central Banks",               2, "SA"),
    ("SA-MDB",  "Multilateral Development Banks",             2, "SA"),
    ("SA-BNK",  "Banks and Covered Bonds",                    2, "SA"),
    ("SA-SEC",  "Securities Firms",                           2, "SA"),
    ("SA-CORP", "Corporates",                                 2, "SA"),
    ("SA-SME",  "SME Corporates",                             2, "SA"),
    ("SA-SPEC", "Specialised Lending",                        2, "SA"),
    ("SA-RET",  "Retail Exposures",                           2, "SA"),
    ("SA-RRET", "Regulatory Retail",                          2, "SA"),
    ("SA-MORT", "Residential Real Estate",                    2, "SA"),
    ("SA-CRE",  "Commercial Real Estate",                     2, "SA"),
    ("SA-CRET", "Income-Producing Real Estate (IPRE)",        2, "SA"),
    ("SA-ADC",  "Land Acquisition, Development and Construction",2,"SA"),
    ("SA-EQ",   "Equity Exposures",                           2, "SA"),
    ("SA-SUB",  "Subordinated Debt",                          2, "SA"),
    ("SA-DEF",  "Defaulted Exposures",                        2, "SA"),
    ("SA-OTH",  "Other Assets",                               2, "SA"),
    # IRB classes (level 2)
    ("IRB-CORP","Corporate",                                  2, "IRB"),
    ("IRB-SPEC","Specialised Lending (Project, Object, Commodity)",2,"IRB"),
    ("IRB-SOV", "Sovereign",                                  2, "IRB"),
    ("IRB-BNK", "Bank",                                       2, "IRB"),
    ("IRB-RET", "Retail - Residential Mortgage",              2, "IRB"),
    ("IRB-QRRE","Qualifying Revolving Retail",                2, "IRB"),
    ("IRB-ORPE","Other Retail",                               2, "IRB"),
    ("IRB-EQ",  "Equity",                                     2, "IRB"),
    # Market risk (level 2)
    ("MKT-SENSI","Sensitivity-Based Method",                  2, "MKT"),
    ("MKT-DRC",  "Default Risk Charge",                       2, "MKT"),
    ("MKT-RNNM", "Residual Risk Add-On",                      2, "MKT"),
    ("MKT-IMA",  "Internal Model Approach",                   2, "MKT"),
    # Op risk (level 2)
    ("OP-BIA",  "Basic Indicator Approach",                   2, "OP"),
    ("OP-TSA",  "Standardised Approach (OpRisk)",             2, "OP"),
    ("OP-SA",   "Standardised Approach (Basel IV SMA)",       2, "OP"),
]


async def ingest_basel_exposure(conn) -> int:
    """Ingest Basel III/IV Exposure Category Classification."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "Basel Exposure",
        "Basel III/IV Exposure Category Classification (BCBS 189/424)",
        "Global",
        "Basel IV (CRR3)",
        "Bank for International Settlements (BIS/BCBS)",
        "#0891B2",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = parent_code if parent_code else code
        is_leaf = code not in codes_with_children
        await conn.execute(
            """
            INSERT INTO classification_node
                (system_id, code, title, level, parent_code,
                 sector_code, is_leaf, seq_order)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (system_id, code) DO UPDATE SET is_leaf = EXCLUDED.is_leaf
            """,
            SYSTEM_ID, code, title, level, parent_code, sector, is_leaf, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, SYSTEM_ID,
    )
    print(f"  Ingested {count} Basel exposure codes")
    return count
