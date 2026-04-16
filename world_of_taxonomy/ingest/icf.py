"""International Classification of Functioning, Disability and Health (ICF) ingester.

The ICF is a WHO framework for measuring health and disability at both individual
and population levels. It classifies body functions and structures, activities and
participation, and environmental factors. Used by healthcare providers, WHO, and
national health systems to code disability and functioning outcomes.
"""
from __future__ import annotations

SYSTEM_ID = "icf"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Components (level 1)
    ("b",  "Body Functions",                              1, None),
    ("s",  "Body Structures",                             1, None),
    ("d",  "Activities and Participation",                1, None),
    ("e",  "Environmental Factors",                       1, None),
    # Body Functions - chapters (level 2)
    ("b1", "Mental Functions",                            2, "b"),
    ("b2", "Sensory Functions and Pain",                  2, "b"),
    ("b3", "Voice and Speech Functions",                  2, "b"),
    ("b4", "Functions of the Cardiovascular, Haematological, Immunological and Respiratory Systems",2,"b"),
    ("b5", "Functions of the Digestive, Metabolic and Endocrine Systems",2,"b"),
    ("b6", "Genitourinary and Reproductive Functions",    2, "b"),
    ("b7", "Neuromusculoskeletal and Movement-Related Functions",2,"b"),
    ("b8", "Functions of the Skin and Related Structures",2,"b"),
    # Body Structures - chapters (level 2)
    ("s1", "Structures of the Nervous System",            2, "s"),
    ("s2", "The Eye, Ear and Related Structures",         2, "s"),
    ("s3", "Structures Involved in Voice and Speech",     2, "s"),
    ("s4", "Structures of the Cardiovascular, Immunological and Respiratory Systems",2,"s"),
    ("s5", "Structures Related to the Digestive, Metabolic and Endocrine Systems",2,"s"),
    ("s6", "Structures Related to the Genitourinary and Reproductive Systems",2,"s"),
    ("s7", "Structures Related to Movement",              2, "s"),
    ("s8", "Skin and Related Structures",                 2, "s"),
    # Activities and Participation - chapters (level 2)
    ("d1", "Learning and Applying Knowledge",             2, "d"),
    ("d2", "General Tasks and Demands",                   2, "d"),
    ("d3", "Communication",                               2, "d"),
    ("d4", "Mobility",                                    2, "d"),
    ("d5", "Self-Care",                                   2, "d"),
    ("d6", "Domestic Life",                               2, "d"),
    ("d7", "Interpersonal Interactions and Relationships",2, "d"),
    ("d8", "Major Life Areas",                            2, "d"),
    ("d9", "Community, Social and Civic Life",            2, "d"),
    # Environmental Factors - chapters (level 2)
    ("e1", "Products and Technology",                     2, "e"),
    ("e2", "Natural Environment and Human-Made Changes",  2, "e"),
    ("e3", "Support and Relationships",                   2, "e"),
    ("e4", "Attitudes",                                   2, "e"),
    ("e5", "Services, Systems and Policies",              2, "e"),
]


async def ingest_icf(conn) -> int:
    """Ingest ICF (International Classification of Functioning) skeleton."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "ICF",
        "International Classification of Functioning, Disability and Health",
        "Global (WHO)",
        "2001 (updated 2013)",
        "World Health Organization (WHO)",
        "#0D9488",
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
    print(f"  Ingested {count} ICF codes")
    return count
