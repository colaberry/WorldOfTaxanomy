"""Physics and Astronomy Classification System (PACS) ingester.

PACS was developed by the American Institute of Physics (AIP) to classify
physics and astronomy literature. It was used by Physical Review, Applied
Physics Letters, and Journal of Applied Physics. The taxonomy has
been replaced by the PACS 2010 edition which remains the last official version.
"""
from __future__ import annotations

SYSTEM_ID = "pacs"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Sections (level 1)
    ("00",  "General",                                          1, None),
    ("02",  "Mathematical Methods in Physics",                  1, None),
    ("03",  "Quantum Mechanics and Quantum Information",        1, None),
    ("04",  "General Relativity and Gravitation",               1, None),
    ("05",  "Statistical Physics, Thermodynamics, Nonlinear Dynamics",1,None),
    ("06",  "Metrology; Measurement; and Laboratory Techniques",1, None),
    ("07",  "Instruments, Apparatus, Components and Techniques",1, None),
    ("11",  "General Theory of Fields and Particles",           1, None),
    ("12",  "Specific Theories and Interaction Models",         1, None),
    ("13",  "Specific Reactions and Phenomenology",             1, None),
    ("14",  "Properties of Specific Particles",                 1, None),
    ("21",  "Nuclear Structure",                                1, None),
    ("23",  "Radioactive Decay and In-beam Spectroscopy",       1, None),
    ("24",  "Nuclear Reactions",                                1, None),
    ("25",  "Nuclear Scattering",                               1, None),
    ("27",  "Properties of Specific Nuclei",                    1, None),
    ("28",  "Nuclear Engineering and Nuclear Power Studies",    1, None),
    ("29",  "Experimental Methods and Instrumentation for Nuclear Physics",1,None),
    ("31",  "Electronic Structure of Atoms and Molecules",      1, None),
    ("32",  "Atomic Properties and Interactions with Photons",  1, None),
    ("33",  "Molecular Properties and Interactions with Photons",1,None),
    ("34",  "Atomic and Molecular Collision Processes and Interactions",1,None),
    ("36",  "Nanoscale Physics",                                1, None),
    ("41",  "Electromagnetism and Optics",                      1, None),
    ("42",  "Optics",                                           1, None),
    ("43",  "Acoustics",                                        1, None),
    ("45",  "Classical and Quantum Statistical Mechanics",      1, None),
    ("47",  "Fluid Dynamics",                                   1, None),
    ("51",  "Electromagnetism, Charged Particle Beams",         1, None),
    ("52",  "Physics of Plasmas and Electric Discharges",       1, None),
    ("61",  "Structure of Solids and Liquids",                  1, None),
    ("62",  "Mechanical and Acoustical Properties of Condensed Matter",1,None),
    ("63",  "Lattice Dynamics and Crystal Statistics",          1, None),
    ("64",  "Equations of State, Phase Equilibria, Phase Transitions",1,None),
    ("65",  "Thermal Properties of Condensed Matter",           1, None),
    ("66",  "Transport Properties of Condensed Matter",         1, None),
    ("67",  "Quantum Fluids and Solids",                        1, None),
    ("68",  "Surfaces and Interfaces; Thin Films and Nanotechnology",1,None),
    ("71",  "Electronic Structure of Bulk Materials",           1, None),
    ("72",  "Electronic Transport in Condensed Matter",         1, None),
    ("73",  "Electronic Structure and Electrical Properties of Surfaces, Interfaces",1,None),
    ("74",  "Superconductivity",                                1, None),
    ("75",  "Magnetic Properties and Materials",                1, None),
    ("76",  "Magnetic Resonance and Relaxation",                1, None),
    ("77",  "Dielectrics, Piezoelectrics, and Ferroelectrics",  1, None),
    ("78",  "Optical Properties, Condensed-Matter Spectroscopy",1, None),
    ("79",  "Electron and Ion Emission by Liquids and Solids",  1, None),
    ("81",  "Materials Science",                                1, None),
    ("82",  "Physical Chemistry and Chemical Physics",          1, None),
    ("83",  "Rheology",                                         1, None),
    ("84",  "Electronics; Radiowave and Microwave Technology",  1, None),
    ("85",  "Quantum Electronics and Photonics",                1, None),
    ("87",  "Biological and Medical Physics",                   1, None),
    ("88",  "Instrumentation and Special Applications",         1, None),
    ("89",  "Other Areas of Applied and Interdisciplinary Physics",1,None),
    ("91",  "Atmospheric, Oceanic, and Planetary Physics",      1, None),
    ("92",  "Astrophysics and Cosmology",                       1, None),
    ("93",  "Stellar Physics",                                  1, None),
    ("94",  "Solar System",                                     1, None),
    ("95",  "Fundamental Astronomy and Astrophysics; Instrumentation",1,None),
    ("96",  "Solar Physics, Astrophysics and Astronomy",        1, None),
    ("97",  "Stars",                                            1, None),
    ("98",  "Stellar Systems; Galactic and Extragalactic Objects",1,None),
    # Key sub-sections (level 2)
    ("03.67","Quantum Information",                             2, "03"),
    ("05.45","Nonlinear Dynamics and Chaos",                    2, "05"),
    ("47.10","General Theory in Fluid Dynamics",                2, "47"),
    ("47.27","Turbulent Flows",                                 2, "47"),
    ("61.46","Nanoscale Materials",                             2, "61"),
    ("74.20","Theories and Models of Superconducting State",    2, "74"),
    ("89.20","Interdisciplinary Applications of Physics",       2, "89"),
]


async def ingest_pacs(conn) -> int:
    """Ingest Physics and Astronomy Classification System (PACS 2010)."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "PACS",
        "Physics and Astronomy Classification System (PACS 2010)",
        "Global",
        "2010",
        "American Institute of Physics (AIP)",
        "#0EA5E9",
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
    print(f"  Ingested {count} PACS codes")
    return count
