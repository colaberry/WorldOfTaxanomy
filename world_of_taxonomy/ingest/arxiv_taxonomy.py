"""arXiv subject taxonomy ingester.

arXiv is an open-access repository for scholarly articles in physics, math,
computer science, quantitative biology, quantitative finance, statistics,
electrical engineering and economics. The taxonomy covers ~150 leaf subject
categories under 20 top-level archives.
"""
from __future__ import annotations

SYSTEM_ID = "arxiv_taxonomy"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Archives (level 1)
    ("cs",      "Computer Science",                          1, None),
    ("math",    "Mathematics",                               1, None),
    ("physics", "Physics",                                   1, None),
    ("astro-ph","Astrophysics",                              1, None),
    ("cond-mat","Condensed Matter",                          1, None),
    ("q-bio",   "Quantitative Biology",                     1, None),
    ("q-fin",   "Quantitative Finance",                     1, None),
    ("stat",    "Statistics",                                1, None),
    ("eess",    "Electrical Engineering and Systems Science",1, None),
    ("econ",    "Economics",                                 1, None),
    ("hep-ph",  "High Energy Physics - Phenomenology",      1, None),
    ("hep-th",  "High Energy Physics - Theory",             1, None),
    ("hep-ex",  "High Energy Physics - Experiment",         1, None),
    ("hep-lat", "High Energy Physics - Lattice",            1, None),
    ("gr-qc",   "General Relativity and Quantum Cosmology", 1, None),
    ("quant-ph","Quantum Physics",                          1, None),
    ("nucl-th", "Nuclear Theory",                           1, None),
    ("nucl-ex", "Nuclear Experiment",                       1, None),
    ("nlin",    "Nonlinear Sciences",                       1, None),
    ("math-ph", "Mathematical Physics",                     1, None),
    # CS categories (level 2)
    ("cs.AI",  "Artificial Intelligence",                   2, "cs"),
    ("cs.CL",  "Computation and Language",                  2, "cs"),
    ("cs.CC",  "Computational Complexity",                  2, "cs"),
    ("cs.CE",  "Computational Engineering, Finance, Science",2,"cs"),
    ("cs.CG",  "Computational Geometry",                    2, "cs"),
    ("cs.GT",  "Computer Science and Game Theory",          2, "cs"),
    ("cs.CV",  "Computer Vision and Pattern Recognition",   2, "cs"),
    ("cs.CY",  "Computers and Society",                     2, "cs"),
    ("cs.CR",  "Cryptography and Security",                 2, "cs"),
    ("cs.DS",  "Data Structures and Algorithms",            2, "cs"),
    ("cs.DB",  "Databases",                                 2, "cs"),
    ("cs.DL",  "Digital Libraries",                         2, "cs"),
    ("cs.DM",  "Discrete Mathematics",                      2, "cs"),
    ("cs.DC",  "Distributed, Parallel, and Cluster Computing",2,"cs"),
    ("cs.ET",  "Emerging Technologies",                     2, "cs"),
    ("cs.FL",  "Formal Languages and Automata Theory",      2, "cs"),
    ("cs.GL",  "General Literature",                        2, "cs"),
    ("cs.GR",  "Graphics",                                  2, "cs"),
    ("cs.AR",  "Hardware Architecture",                     2, "cs"),
    ("cs.HC",  "Human-Computer Interaction",                2, "cs"),
    ("cs.IR",  "Information Retrieval",                     2, "cs"),
    ("cs.IT",  "Information Theory",                        2, "cs"),
    ("cs.LG",  "Machine Learning",                          2, "cs"),
    ("cs.LO",  "Logic in Computer Science",                 2, "cs"),
    ("cs.MS",  "Mathematical Software",                     2, "cs"),
    ("cs.MA",  "Multiagent Systems",                        2, "cs"),
    ("cs.MM",  "Multimedia",                                2, "cs"),
    ("cs.NI",  "Networking and Internet Architecture",      2, "cs"),
    ("cs.NE",  "Neural and Evolutionary Computing",         2, "cs"),
    ("cs.NA",  "Numerical Analysis",                        2, "cs"),
    ("cs.OS",  "Operating Systems",                         2, "cs"),
    ("cs.OH",  "Other Computer Science",                    2, "cs"),
    ("cs.PF",  "Performance",                               2, "cs"),
    ("cs.PL",  "Programming Languages",                     2, "cs"),
    ("cs.RO",  "Robotics",                                  2, "cs"),
    ("cs.SI",  "Social and Information Networks",           2, "cs"),
    ("cs.SE",  "Software Engineering",                      2, "cs"),
    ("cs.SD",  "Sound",                                     2, "cs"),
    ("cs.SC",  "Symbolic Computation",                      2, "cs"),
    ("cs.SY",  "Systems and Control",                       2, "cs"),
    # Math categories (level 2)
    ("math.AG","Algebraic Geometry",                        2, "math"),
    ("math.AT","Algebraic Topology",                        2, "math"),
    ("math.AP","Analysis of PDEs",                          2, "math"),
    ("math.CT","Category Theory",                           2, "math"),
    ("math.CA","Classical Analysis and ODEs",               2, "math"),
    ("math.CO","Combinatorics",                             2, "math"),
    ("math.AC","Commutative Algebra",                       2, "math"),
    ("math.CV","Complex Variables",                         2, "math"),
    ("math.DG","Differential Geometry",                     2, "math"),
    ("math.DS","Dynamical Systems",                         2, "math"),
    ("math.FA","Functional Analysis",                       2, "math"),
    ("math.GM","General Mathematics",                       2, "math"),
    ("math.GN","General Topology",                          2, "math"),
    ("math.GT","Geometric Topology",                        2, "math"),
    ("math.GR","Group Theory",                              2, "math"),
    ("math.HO","History and Overview",                      2, "math"),
    ("math.IT","Information Theory",                        2, "math"),
    ("math.KT","K-Theory and Homology",                     2, "math"),
    ("math.LO","Logic",                                     2, "math"),
    ("math.MP","Mathematical Physics",                      2, "math"),
    ("math.MG","Metric Geometry",                           2, "math"),
    ("math.NT","Number Theory",                             2, "math"),
    ("math.NA","Numerical Analysis",                        2, "math"),
    ("math.OA","Operator Algebras",                         2, "math"),
    ("math.OC","Optimization and Control",                  2, "math"),
    ("math.PR","Probability",                               2, "math"),
    ("math.QA","Quantum Algebra",                           2, "math"),
    ("math.RT","Representation Theory",                     2, "math"),
    ("math.RA","Rings and Algebras",                        2, "math"),
    ("math.SP","Spectral Theory",                           2, "math"),
    ("math.ST","Statistics Theory",                         2, "math"),
    ("math.SG","Symplectic Geometry",                       2, "math"),
    # q-fin categories (level 2)
    ("q-fin.CP","Computational Finance",                    2, "q-fin"),
    ("q-fin.EC","Economics",                                2, "q-fin"),
    ("q-fin.GN","General Finance",                          2, "q-fin"),
    ("q-fin.MF","Mathematical Finance",                     2, "q-fin"),
    ("q-fin.PM","Portfolio Management",                     2, "q-fin"),
    ("q-fin.PR","Pricing of Securities",                    2, "q-fin"),
    ("q-fin.RM","Risk Management",                          2, "q-fin"),
    ("q-fin.ST","Statistical Finance",                      2, "q-fin"),
    ("q-fin.TR","Trading and Market Microstructure",        2, "q-fin"),
    # stat categories (level 2)
    ("stat.AP","Applications",                              2, "stat"),
    ("stat.CO","Computation",                               2, "stat"),
    ("stat.ML","Machine Learning",                          2, "stat"),
    ("stat.ME","Methodology",                               2, "stat"),
    ("stat.OT","Other Statistics",                          2, "stat"),
    ("stat.TH","Statistics Theory",                         2, "stat"),
    # econ categories (level 2)
    ("econ.EM","Econometrics",                              2, "econ"),
    ("econ.GN","General Economics",                         2, "econ"),
    ("econ.TH","Theoretical Economics",                     2, "econ"),
    # Physics categories (level 2) - 21 entries
    ("physics.acc-ph",  "Accelerator Physics",                       2, "physics"),
    ("physics.ao-ph",   "Atmospheric and Oceanic Physics",           2, "physics"),
    ("physics.atom-ph", "Atomic Physics",                            2, "physics"),
    ("physics.bio-ph",  "Biological Physics",                        2, "physics"),
    ("physics.chem-ph", "Chemical Physics",                          2, "physics"),
    ("physics.class-ph","Classical Physics",                         2, "physics"),
    ("physics.comp-ph", "Computational Physics",                     2, "physics"),
    ("physics.data-an", "Data Analysis, Statistics and Probability", 2, "physics"),
    ("physics.ed-ph",   "Physics Education",                         2, "physics"),
    ("physics.flu-dyn", "Fluid Dynamics",                            2, "physics"),
    ("physics.gen-ph",  "General Physics",                           2, "physics"),
    ("physics.geo-ph",  "Geophysics",                                2, "physics"),
    ("physics.hist-ph", "History and Philosophy of Physics",         2, "physics"),
    ("physics.ins-det", "Instrumentation and Detectors",             2, "physics"),
    ("physics.med-ph",  "Medical Physics",                           2, "physics"),
    ("physics.optics",  "Optics",                                    2, "physics"),
    ("physics.plasm-ph","Plasma Physics",                            2, "physics"),
    ("physics.pop-ph",  "Popular Physics",                           2, "physics"),
    ("physics.soc-ph",  "Physics and Society",                       2, "physics"),
    ("physics.space-ph","Space Physics",                             2, "physics"),
    ("physics.atm-clus","Atomic and Molecular Clusters",             2, "physics"),
    # Astrophysics categories (level 2) - 6 entries
    ("astro-ph.GA","Astrophysics of Galaxies",                       2, "astro-ph"),
    ("astro-ph.CO","Cosmology and Nongalactic Astrophysics",         2, "astro-ph"),
    ("astro-ph.EP","Earth and Planetary Astrophysics",               2, "astro-ph"),
    ("astro-ph.HE","High Energy Astrophysical Phenomena",            2, "astro-ph"),
    ("astro-ph.IM","Instrumentation and Methods for Astrophysics",   2, "astro-ph"),
    ("astro-ph.SR","Solar and Stellar Astrophysics",                 2, "astro-ph"),
    # Condensed Matter categories (level 2) - 9 entries
    ("cond-mat.dis-nn",   "Disordered Systems and Neural Networks",  2, "cond-mat"),
    ("cond-mat.mes-hall", "Mesoscale and Nanoscale Physics",         2, "cond-mat"),
    ("cond-mat.mtrl-sci", "Materials Science",                       2, "cond-mat"),
    ("cond-mat.other",    "Other Condensed Matter",                  2, "cond-mat"),
    ("cond-mat.quant-gas","Quantum Gases",                           2, "cond-mat"),
    ("cond-mat.soft",     "Soft Condensed Matter",                   2, "cond-mat"),
    ("cond-mat.stat-mech","Statistical Mechanics",                   2, "cond-mat"),
    ("cond-mat.str-el",   "Strongly Correlated Electrons",           2, "cond-mat"),
    ("cond-mat.supr-con", "Superconductivity",                       2, "cond-mat"),
    # Quantitative Biology categories (level 2) - 10 entries
    ("q-bio.BM","Biomolecules",                                      2, "q-bio"),
    ("q-bio.CB","Cell Behavior",                                     2, "q-bio"),
    ("q-bio.GN","Genomics",                                          2, "q-bio"),
    ("q-bio.MN","Molecular Networks",                                2, "q-bio"),
    ("q-bio.NC","Neurons and Cognition",                             2, "q-bio"),
    ("q-bio.OT","Other Quantitative Biology",                        2, "q-bio"),
    ("q-bio.PE","Populations and Evolution",                         2, "q-bio"),
    ("q-bio.QM","Quantitative Methods",                              2, "q-bio"),
    ("q-bio.SC","Subcellular Processes",                             2, "q-bio"),
    ("q-bio.TO","Tissues and Organs",                                2, "q-bio"),
    # Electrical Engineering and Systems Science categories (level 2) - 4 entries
    ("eess.AS","Audio and Speech Processing",                        2, "eess"),
    ("eess.IV","Image and Video Processing",                         2, "eess"),
    ("eess.SP","Signal Processing",                                  2, "eess"),
    ("eess.SY","Systems and Control",                                2, "eess"),
    # Nonlinear Sciences categories (level 2) - 5 entries
    ("nlin.AO","Adaptation and Self-Organizing Systems",             2, "nlin"),
    ("nlin.CG","Cellular Automata and Lattice Gases",                2, "nlin"),
    ("nlin.CD","Chaotic Dynamics",                                   2, "nlin"),
    ("nlin.PS","Pattern Formation and Solitons",                     2, "nlin"),
    ("nlin.SI","Exactly Solvable and Integrable Systems",            2, "nlin"),
]


async def ingest_arxiv_taxonomy(conn) -> int:
    """Ingest arXiv subject taxonomy."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "arXiv Taxonomy",
        "arXiv Subject Classification Taxonomy",
        "Global",
        "2023",
        "Cornell University / arXiv.org",
        "#B45309",
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
    print(f"  Ingested {count} arXiv taxonomy codes")
    return count
