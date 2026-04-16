"""OECD Fields of Research and Development (FORD) - Frascati Manual 2015.

The FORD classification is the international standard for classifying
R&D expenditure and personnel by scientific discipline. Defined in the
OECD Frascati Manual 2015. Used by national science ministries, research
councils, universities, and statistical offices worldwide.
Six main fields, 42 sub-fields (disciplines).
"""
from __future__ import annotations

SYSTEM_ID = "ford_frascati"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Main fields (level 1)
    ("1", "Natural Sciences",                         1, None),
    ("2", "Engineering and Technology",               1, None),
    ("3", "Medical and Health Sciences",              1, None),
    ("4", "Agricultural and Veterinary Sciences",     1, None),
    ("5", "Social Sciences",                          1, None),
    ("6", "Humanities and the Arts",                  1, None),
    # Natural Sciences sub-fields (level 2)
    ("1.1",  "Mathematics",                                                      2, "1"),
    ("1.2",  "Computer and Information Sciences",                                2, "1"),
    ("1.3",  "Physical Sciences",                                                2, "1"),
    ("1.4",  "Chemical Sciences",                                                2, "1"),
    ("1.5",  "Earth and Related Environmental Sciences",                         2, "1"),
    ("1.6",  "Biological Sciences",                                              2, "1"),
    ("1.7",  "Other Natural Sciences",                                           2, "1"),
    # Engineering and Technology sub-fields (level 2)
    ("2.1",  "Civil Engineering",                                                2, "2"),
    ("2.2",  "Electrical Engineering, Electronic Engineering, Information Engineering", 2, "2"),
    ("2.3",  "Mechanical Engineering",                                           2, "2"),
    ("2.4",  "Chemical Engineering",                                             2, "2"),
    ("2.5",  "Materials Engineering",                                            2, "2"),
    ("2.6",  "Medical Engineering",                                              2, "2"),
    ("2.7",  "Environmental Engineering",                                        2, "2"),
    ("2.8",  "Environmental Biotechnology",                                      2, "2"),
    ("2.9",  "Industrial Biotechnology",                                         2, "2"),
    ("2.10", "Nano-technology",                                                  2, "2"),
    ("2.11", "Other Engineering and Technologies",                               2, "2"),
    # Medical and Health Sciences sub-fields (level 2)
    ("3.1",  "Basic Medicine",                                                   2, "3"),
    ("3.2",  "Clinical Medicine",                                                2, "3"),
    ("3.3",  "Health Sciences",                                                  2, "3"),
    ("3.4",  "Medical Biotechnology",                                            2, "3"),
    ("3.5",  "Other Medical Sciences",                                           2, "3"),
    # Agricultural and Veterinary Sciences sub-fields (level 2)
    ("4.1",  "Agriculture, Forestry and Fisheries",                              2, "4"),
    ("4.2",  "Animal and Dairy Sciences",                                        2, "4"),
    ("4.3",  "Veterinary Sciences",                                              2, "4"),
    ("4.4",  "Agricultural Biotechnology",                                       2, "4"),
    ("4.5",  "Other Agricultural Sciences",                                      2, "4"),
    # Social Sciences sub-fields (level 2)
    ("5.1",  "Psychology and Cognitive Sciences",                                2, "5"),
    ("5.2",  "Economics and Business",                                           2, "5"),
    ("5.3",  "Educational Sciences",                                             2, "5"),
    ("5.4",  "Sociology",                                                        2, "5"),
    ("5.5",  "Law",                                                              2, "5"),
    ("5.6",  "Political Science",                                                2, "5"),
    ("5.7",  "Social and Economic Geography",                                    2, "5"),
    ("5.8",  "Media and Communications",                                         2, "5"),
    ("5.9",  "Other Social Sciences",                                            2, "5"),
    # Humanities and the Arts sub-fields (level 2)
    ("6.1",  "History and Archaeology",                                          2, "6"),
    ("6.2",  "Languages and Literature",                                         2, "6"),
    ("6.3",  "Philosophy, Ethics and Religion",                                  2, "6"),
    ("6.4",  "Arts (art history, performing arts, music)",                       2, "6"),
    ("6.5",  "Other Humanities and the Arts",                                    2, "6"),
]


async def ingest_ford_frascati(conn) -> int:
    """Ingest OECD FORD (Fields of Research and Development) - Frascati Manual 2015."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "FORD (Frascati 2015)",
        "OECD Fields of Research and Development - Frascati Manual 2015",
        "Global (OECD)",
        "2015",
        "Organisation for Economic Co-operation and Development (OECD)",
        "#D97706",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = code.split(".")[0]
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
    print(f"  Ingested {count} FORD Frascati codes")
    return count
