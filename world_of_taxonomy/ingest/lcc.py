"""Library of Congress Classification (LCC) skeleton ingester.

The Library of Congress Classification (LCC) is a system of library
classification developed by the Library of Congress. It uses alphabetic
main classes (A-Z), subdivided by a combination of letters and numerals.
This is a top-level skeleton covering the 21 main classes. Full subclass
enumeration requires parsing the official LCC schedules.
"""
from __future__ import annotations

SYSTEM_ID = "lcc"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Main classes (level 1)
    ("A",  "General Works",                                         1, None),
    ("B",  "Philosophy, Psychology, Religion",                      1, None),
    ("C",  "Auxiliary Sciences of History",                         1, None),
    ("D",  "World History",                                         1, None),
    ("E",  "History of the Americas (United States)",               1, None),
    ("F",  "History of the Americas (Other)",                       1, None),
    ("G",  "Geography, Anthropology, Recreation",                   1, None),
    ("H",  "Social Sciences",                                       1, None),
    ("J",  "Political Science",                                     1, None),
    ("K",  "Law",                                                   1, None),
    ("L",  "Education",                                             1, None),
    ("M",  "Music and Books on Music",                              1, None),
    ("N",  "Fine Arts",                                             1, None),
    ("P",  "Language and Literature",                               1, None),
    ("Q",  "Science",                                               1, None),
    ("R",  "Medicine",                                              1, None),
    ("S",  "Agriculture",                                           1, None),
    ("T",  "Technology",                                            1, None),
    ("U",  "Military Science",                                      1, None),
    ("V",  "Naval Science",                                         1, None),
    ("Z",  "Bibliography, Library Science, Information Resources",  1, None),
    # Subclasses (level 2) - selected key subclasses
    ("AC", "Collections, Series, Collected Works",                  2, "A"),
    ("AE", "Encyclopedias",                                         2, "A"),
    ("AG", "Dictionaries and Other General Reference Works",        2, "A"),
    ("BC", "Logic",                                                 2, "B"),
    ("BD", "Speculative Philosophy",                                2, "B"),
    ("BF", "Psychology",                                            2, "B"),
    ("BL", "Religions, Mythology, Rationalism",                     2, "B"),
    ("DG", "History of Italy and Surrounding Regions",               2, "D"),
    ("GE", "Environmental Sciences",                                2, "G"),
    ("GF", "Human Ecology, Anthropogeography",                      2, "G"),
    ("HA", "Statistics",                                            2, "H"),
    ("HB", "Economic Theory, Demography",                           2, "H"),
    ("HC", "Economic History and Conditions",                       2, "H"),
    ("HD", "Industries, Land Use, Labor",                           2, "H"),
    ("HE", "Transportation and Communications",                     2, "H"),
    ("HF", "Commerce",                                              2, "H"),
    ("HG", "Finance",                                               2, "H"),
    ("HJ", "Public Finance",                                        2, "H"),
    ("HM", "Sociology",                                             2, "H"),
    ("HN", "Social History and Conditions",                         2, "H"),
    ("HQ", "The Family, Marriage, Women",                           2, "H"),
    ("HS", "Societies - Secret, Benevolent, Etc.",                  2, "H"),
    ("HT", "Communities, Classes, Races",                           2, "H"),
    ("HV", "Social Pathology, Social and Public Welfare",           2, "H"),
    ("HX", "Socialism, Communism, Anarchism",                       2, "H"),
    ("KF", "Law of the United States",                              2, "K"),
    ("KJV","Law of France",                                         2, "K"),
    ("KK", "Law of Germany",                                        2, "K"),
    ("ML", "Literature on Music",                                   2, "M"),
    ("MT", "Musical Instruction and Study",                         2, "M"),
    ("NA", "Architecture",                                          2, "N"),
    ("NB", "Sculpture",                                             2, "N"),
    ("NC", "Drawing, Design, Illustration",                         2, "N"),
    ("ND", "Painting",                                              2, "N"),
    ("NE", "Print Media",                                           2, "N"),
    ("PA", "Greek Language and Literature",                         2, "P"),
    ("PB", "Modern Languages",                                      2, "P"),
    ("PQ", "French, Italian, Spanish, Portuguese Literature",       2, "P"),
    ("PR", "English Literature",                                    2, "P"),
    ("PS", "American Literature in English",                        2, "P"),
    ("PT", "German, Dutch, and Scandinavian Literatures",           2, "P"),
    ("QA", "Mathematics",                                           2, "Q"),
    ("QB", "Astronomy",                                             2, "Q"),
    ("QC", "Physics",                                               2, "Q"),
    ("QD", "Chemistry",                                             2, "Q"),
    ("QE", "Geology",                                               2, "Q"),
    ("QH", "Natural History (General), Biology",                    2, "Q"),
    ("QK", "Botany",                                                2, "Q"),
    ("QL", "Zoology",                                               2, "Q"),
    ("QM", "Human Anatomy",                                         2, "Q"),
    ("QP", "Physiology",                                            2, "Q"),
    ("QR", "Microbiology",                                          2, "Q"),
    ("RA", "Public Aspects of Medicine",                            2, "R"),
    ("RB", "Pathology",                                             2, "R"),
    ("RC", "Internal Medicine",                                     2, "R"),
    ("RD", "Surgery",                                               2, "R"),
    ("RE", "Ophthalmology",                                         2, "R"),
    ("RF", "Otorhinolaryngology",                                   2, "R"),
    ("RG", "Gynecology and Obstetrics",                             2, "R"),
    ("RJ", "Pediatrics",                                            2, "R"),
    ("RK", "Dentistry",                                             2, "R"),
    ("RL", "Dermatology",                                           2, "R"),
    ("RM", "Therapeutics, Pharmacology",                            2, "R"),
    ("RS", "Pharmacy and Materia Medica",                           2, "R"),
    ("RT", "Nursing",                                               2, "R"),
    ("RX", "Homeopathy",                                            2, "R"),
    ("RZ", "Other Systems of Medicine",                             2, "R"),
    ("SB", "Plant Culture",                                         2, "S"),
    ("SD", "Forestry",                                              2, "S"),
    ("SF", "Animal Culture",                                        2, "S"),
    ("SH", "Aquaculture, Fisheries, Angling",                       2, "S"),
    ("SK", "Hunting Sports",                                        2, "S"),
    ("TA", "Engineering (General), Civil Engineering",              2, "T"),
    ("TC", "Hydraulic Engineering and Ocean Engineering",           2, "T"),
    ("TD", "Environmental Technology, Sanitary Engineering",        2, "T"),
    ("TE", "Highway Engineering, Roads and Pavements",              2, "T"),
    ("TF", "Railroad Engineering and Operation",                    2, "T"),
    ("TG", "Bridges and Roofs",                                     2, "T"),
    ("TH", "Building Construction",                                 2, "T"),
    ("TJ", "Mechanical Engineering and Machinery",                  2, "T"),
    ("TK", "Electrical Engineering, Electronics, Nuclear Engineering",2,"T"),
    ("TL", "Motor Vehicles, Aeronautics, Astronautics",             2, "T"),
    ("TN", "Mining Engineering, Metallurgy",                        2, "T"),
    ("TP", "Chemical Technology",                                   2, "T"),
    ("TR", "Photography",                                           2, "T"),
    ("TS", "Manufactures",                                          2, "T"),
    ("TT", "Handicrafts, Arts and Crafts",                          2, "T"),
    ("TX", "Home Economics",                                        2, "T"),
    ("ZB", "Bibliographies",                                         2, "Z"),
    ("ZA", "Information Resources (General)",                       2, "Z"),
]


async def ingest_lcc(conn) -> int:
    """Ingest Library of Congress Classification skeleton."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "LCC",
        "Library of Congress Classification",
        "Global",
        "2023",
        "Library of Congress",
        "#92400E",
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
    print(f"  Ingested {count} LCC codes")
    return count
