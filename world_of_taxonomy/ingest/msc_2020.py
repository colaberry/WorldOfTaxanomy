"""Mathematics Subject Classification 2020 (MSC 2020) ingester.

The Mathematics Subject Classification (MSC) is the taxonomy used by
Mathematical Reviews (MathSciNet) and zbMATH to classify mathematical
literature. MSC 2020 has 63 two-digit primary classes and thousands of
five-digit subfields.
"""
from __future__ import annotations

SYSTEM_ID = "msc_2020"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Primary classes (level 1)
    ("00", "General and Overarching Topics",            1, None),
    ("01", "History and Biography",                     1, None),
    ("03", "Mathematical Logic and Foundations",        1, None),
    ("05", "Combinatorics",                             1, None),
    ("06", "Order, Lattices, Ordered Algebraic Structures",1,None),
    ("08", "General Algebraic Systems",                 1, None),
    ("11", "Number Theory",                             1, None),
    ("12", "Field Theory and Polynomials",              1, None),
    ("13", "Commutative Algebra",                       1, None),
    ("14", "Algebraic Geometry",                        1, None),
    ("15", "Linear and Multilinear Algebra; Matrix Theory",1,None),
    ("16", "Associative Rings and Algebras",            1, None),
    ("17", "Nonassociative Rings and Algebras",         1, None),
    ("18", "Category Theory; Homological Algebra",      1, None),
    ("19", "K-Theory",                                  1, None),
    ("20", "Group Theory and Generalizations",          1, None),
    ("22", "Topological Groups, Lie Groups",            1, None),
    ("26", "Real Functions",                            1, None),
    ("28", "Measure and Integration",                   1, None),
    ("30", "Functions of a Complex Variable",           1, None),
    ("31", "Potential Theory",                          1, None),
    ("32", "Several Complex Variables and Analytic Spaces",1,None),
    ("33", "Special Functions",                         1, None),
    ("34", "Ordinary Differential Equations",           1, None),
    ("35", "Partial Differential Equations",            1, None),
    ("37", "Dynamical Systems and Ergodic Theory",      1, None),
    ("39", "Difference and Functional Equations",       1, None),
    ("40", "Sequences, Series, Summability",            1, None),
    ("41", "Approximations and Expansions",             1, None),
    ("42", "Harmonic Analysis on Euclidean Spaces",     1, None),
    ("43", "Abstract Harmonic Analysis",                1, None),
    ("44", "Integral Transforms, Operational Calculus", 1, None),
    ("45", "Integral Equations",                        1, None),
    ("46", "Functional Analysis",                       1, None),
    ("47", "Operator Theory",                           1, None),
    ("49", "Calculus of Variations and Optimal Control",1, None),
    ("51", "Geometry",                                  1, None),
    ("52", "Convex and Discrete Geometry",              1, None),
    ("53", "Differential Geometry",                     1, None),
    ("54", "General Topology",                          1, None),
    ("55", "Algebraic Topology",                        1, None),
    ("57", "Manifolds and Cell Complexes",              1, None),
    ("58", "Global Analysis, Analysis on Manifolds",    1, None),
    ("60", "Probability Theory and Stochastic Processes",1,None),
    ("62", "Statistics",                                1, None),
    ("65", "Numerical Analysis",                        1, None),
    ("68", "Computer Science",                          1, None),
    ("70", "Mechanics of Particles and Systems",        1, None),
    ("74", "Mechanics of Deformable Solids",            1, None),
    ("76", "Fluid Mechanics",                           1, None),
    ("78", "Optics, Electromagnetic Theory",            1, None),
    ("80", "Classical Thermodynamics, Heat Transfer",   1, None),
    ("81", "Quantum Theory",                            1, None),
    ("82", "Statistical Mechanics, Structure of Matter",1, None),
    ("83", "Relativity and Gravitational Theory",       1, None),
    ("85", "Astronomy and Astrophysics",                1, None),
    ("86", "Geophysics",                                1, None),
    ("90", "Operations Research, Mathematical Programming",1,None),
    ("91", "Game Theory, Economics, Social Science",    1, None),
    ("92", "Biology and Other Natural Sciences",        1, None),
    ("93", "Systems Theory; Control",                   1, None),
    ("94", "Information and Communication Theory",      1, None),
    ("97", "Mathematics Education",                     1, None),
    # Selected sub-classifications (level 2)
    ("03B",   "General Logic",                          2, "03"),
    ("03C",   "Model Theory",                           2, "03"),
    ("03D",   "Computability and Recursion Theory",     2, "03"),
    ("03E",   "Set Theory",                             2, "03"),
    ("05A",   "Enumerative Combinatorics",              2, "05"),
    ("05C",   "Graph Theory",                           2, "05"),
    ("05D",   "Extremal Combinatorics",                 2, "05"),
    ("11A",   "Elementary Number Theory",               2, "11"),
    ("11N",   "Multiplicative Number Theory",           2, "11"),
    ("11P",   "Additive Number Theory",                 2, "11"),
    ("14A",   "Foundations of Algebraic Geometry",      2, "14"),
    ("14H",   "Curves",                                 2, "14"),
    ("35A",   "General Topics for PDEs",                2, "35"),
    ("35B",   "Qualitative Properties of PDE Solutions",2, "35"),
    ("35K",   "Parabolic Equations and Systems",        2, "35"),
    ("35Q",   "PDEs of Mathematical Physics",           2, "35"),
    ("60A",   "Foundations of Probability",             2, "60"),
    ("60G",   "Stochastic Processes",                   2, "60"),
    ("60H",   "Stochastic Analysis",                    2, "60"),
    ("62A",   "Foundational and Philosophical Topics in Statistics",2,"62"),
    ("62F",   "Parametric Inference",                   2, "62"),
    ("62M",   "Time Series Analysis",                   2, "62"),
    ("65F",   "Numerical Linear Algebra",               2, "65"),
    ("65N",   "Numerical Methods for PDEs",             2, "65"),
    ("68P",   "Theory of Data",                         2, "68"),
    ("68Q",   "Theory of Computing",                    2, "68"),
    ("90C",   "Mathematical Programming",               2, "90"),
    ("91A",   "Game Theory",                            2, "91"),
    ("91B",   "Mathematical Economics",                 2, "91"),
]


async def ingest_msc_2020(conn) -> int:
    """Ingest Mathematics Subject Classification 2020."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "MSC 2020",
        "Mathematics Subject Classification 2020",
        "Global",
        "2020",
        "American Mathematical Society (AMS) / zbMATH",
        "#7C3AED",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = code[:2]
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
    print(f"  Ingested {count} MSC 2020 codes")
    return count
