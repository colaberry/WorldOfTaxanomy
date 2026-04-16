"""ISO 10962 Classification of Financial Instruments (CFI) ingester.

The CFI code is the ISO standard for classifying financial instruments
using a six-character alphabetical code. Published by ISO and maintained
by ANNA (Association of National Numbering Agencies).
First character = asset class; second character = group; remaining = attributes.
Used globally by exchanges, clearing houses, depositories, and regulators.
"""
from __future__ import annotations

SYSTEM_ID = "cfi_iso10962"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Asset classes (level 1)
    ("E",  "Equities",                                    1, None),
    ("D",  "Debt Instruments",                            1, None),
    ("R",  "Entitlements (Rights)",                       1, None),
    ("O",  "Listed Options",                              1, None),
    ("F",  "Futures",                                     1, None),
    ("S",  "Swaps",                                       1, None),
    ("H",  "Non-listed and Complex Options",              1, None),
    ("I",  "Spot",                                        1, None),
    ("J",  "Forwards",                                    1, None),
    ("K",  "Strategies",                                  1, None),
    ("L",  "Financing",                                   1, None),
    ("T",  "Referential Instruments",                     1, None),
    ("M",  "Others / Miscellaneous",                      1, None),
    # Equities sub-groups (level 2)
    ("ES", "Shares (Common / Ordinary)",                  2, "E"),
    ("EP", "Preferred / Preference Shares",               2, "E"),
    ("EC", "Common Convertible Shares",                   2, "E"),
    ("EF", "Preferred Convertible Shares",                2, "E"),
    ("EL", "Limited Partnership Units",                   2, "E"),
    ("ED", "Depositary Receipts on Equities",             2, "E"),
    ("EY", "Structured Instruments (Equities)",           2, "E"),
    ("EM", "Others (Equities)",                           2, "E"),
    # Debt sub-groups (level 2)
    ("DB", "Bonds",                                       2, "D"),
    ("DC", "Convertible Bonds",                           2, "D"),
    ("DW", "Medium-Term Notes",                           2, "D"),
    ("DT", "Money Market Instruments",                    2, "D"),
    ("DY", "Mortgage-Backed Securities",                  2, "D"),
    ("DA", "Asset-Backed Securities",                     2, "D"),
    ("DD", "Municipal Bonds",                             2, "D"),
    ("DM", "Others (Debt)",                               2, "D"),
    # Entitlements sub-groups (level 2)
    ("RA", "Allotment Rights",                            2, "R"),
    ("RS", "Subscription Rights",                         2, "R"),
    ("RP", "Purchase Rights",                             2, "R"),
    ("RW", "Warrants",                                    2, "R"),
    ("RM", "Others (Entitlements)",                       2, "R"),
    # Listed Options sub-groups (level 2)
    ("OC", "Call Options",                                2, "O"),
    ("OP", "Put Options",                                 2, "O"),
    ("OM", "Others (Listed Options)",                     2, "O"),
    # Futures sub-groups (level 2)
    ("FF", "Financial Futures",                           2, "F"),
    ("FC", "Commodity Futures",                           2, "F"),
    ("FM", "Others (Futures)",                            2, "F"),
    # Swaps sub-groups (level 2)
    ("SR", "Rates Swaps",                                 2, "S"),
    ("SC", "Currency Swaps",                              2, "S"),
    ("SE", "Equity Swaps",                                2, "S"),
    ("SD", "Commodity Swaps",                             2, "S"),
    ("SM", "Others (Swaps)",                              2, "S"),
    # Non-listed Options sub-groups (level 2)
    ("HC", "Non-listed Call Options",                     2, "H"),
    ("HP", "Non-listed Put Options",                      2, "H"),
    ("HM", "Others (Non-listed Options)",                 2, "H"),
    # Spot sub-groups (level 2)
    ("IC", "Currencies (Spot)",                           2, "I"),
    ("IM", "Others (Spot)",                               2, "I"),
    # Forwards sub-groups (level 2)
    ("JF", "Rates Forwards",                              2, "J"),
    ("JC", "Currency Forwards",                           2, "J"),
    ("JM", "Others (Forwards)",                           2, "J"),
    # Strategies sub-groups (level 2)
    ("KR", "Rates Strategies",                            2, "K"),
    ("KE", "Equity Strategies",                           2, "K"),
    ("KM", "Others (Strategies)",                         2, "K"),
    # Financing sub-groups (level 2)
    ("LR", "Repos",                                       2, "L"),
    ("LB", "Buy-Sell Back",                               2, "L"),
    ("LM", "Others (Financing)",                          2, "L"),
    # Referential sub-groups (level 2)
    ("TC", "Currencies (Referential)",                    2, "T"),
    ("TB", "Baskets",                                     2, "T"),
    ("TI", "Indices",                                     2, "T"),
    ("TM", "Others (Referential)",                        2, "T"),
]


async def ingest_cfi_iso10962(conn) -> int:
    """Ingest ISO 10962 Classification of Financial Instruments (CFI)."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "CFI ISO 10962",
        "Classification of Financial Instruments (ISO 10962:2015)",
        "Global (ISO)",
        "2015",
        "ISO / Association of National Numbering Agencies (ANNA)",
        "#6366F1",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = code[:1]
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
    print(f"  Ingested {count} CFI ISO 10962 codes")
    return count
