"""BEC Rev 5 (Broad Economic Categories, Revision 5) ingester.

UN Statistics Division, 2018. 7 top-level categories, 29 total nodes.
BEC bridges HS/SITC trade statistics with SNA end-use categories.

Source: UN Statistics Division. Hand-coded.
"""
from typing import Optional, List, Tuple

# (code, parent_code_or_None, title)
BEC_NODES: List[Tuple[str, Optional[str], str]] = [
    ("1", None, "Food and beverages"),
    ("1.1", '1', "Primary food and beverages"),
    ("1.1.1", '1.1', "Primary food and beverages - mainly for industry"),
    ("1.1.2", '1.1', "Primary food and beverages - mainly for household consumption"),
    ("1.2", '1', "Processed food and beverages"),
    ("1.2.1", '1.2', "Processed food and beverages - mainly for industry"),
    ("1.2.2", '1.2', "Processed food and beverages - mainly for household consumption"),
    ("2", None, "Industrial supplies not elsewhere specified"),
    ("2.1", '2', "Primary industrial supplies"),
    ("2.2", '2', "Processed industrial supplies"),
    ("3", None, "Fuels and lubricants"),
    ("3.1", '3', "Primary fuels and lubricants"),
    ("3.2", '3', "Processed fuels and lubricants"),
    ("3.2.1", '3.2', "Motor spirit"),
    ("3.2.2", '3.2', "Other processed fuels and lubricants"),
    ("4", None, "Capital goods (except transport equipment) and parts and accessories"),
    ("4.1", '4', "Capital goods (except transport equipment)"),
    ("4.2", '4', "Parts and accessories for capital goods"),
    ("5", None, "Transport equipment and parts and accessories"),
    ("5.1", '5', "Passenger motor cars"),
    ("5.2", '5', "Other transport equipment"),
    ("5.2.1", '5.2', "Industrial transport equipment"),
    ("5.2.2", '5.2', "Non-industrial transport equipment"),
    ("5.3", '5', "Parts and accessories for transport equipment"),
    ("6", None, "Consumer goods not elsewhere specified"),
    ("6.1", '6', "Durable consumer goods"),
    ("6.2", '6', "Semi-durable consumer goods"),
    ("6.3", '6', "Non-durable consumer goods"),
    ("7", None, "Goods not elsewhere specified"),
]


async def ingest_bec_rev5(conn) -> int:
    """Ingest BEC Rev 5 categories. Returns node count."""
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, region, version, authority, tint_color)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "bec_rev5", "BEC Rev 5",
        "Broad Economic Categories, Revision 5",
        "Global", "Revision 5 (2018)", "UN Statistics Division", "#14B8A6",
    )

    count = 0
    for seq, (code, parent, title) in enumerate(BEC_NODES, 1):
        level = len(code.replace(".", "")) - 1
        sector = code.split(".")[0]
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ('bec_rev5', $1, $2, $3, $4, $5, FALSE, $6)
               ON CONFLICT (system_id, code) DO NOTHING""",
            code, title, level, parent, sector, seq,
        )
        count += 1

    await conn.execute("UPDATE classification_system SET node_count=$1 WHERE id='bec_rev5'", count)
    print(f"  {count} BEC Rev 5 nodes ingested")
    return count
