"""EdTech Platform domain taxonomy ingester.

EdTech platform taxonomy organizes educational technology segments and models:
  K-12 EdTech      (det_k12*)      - LMS, adaptive, assessment, classroom tools
  Higher Ed        (det_higher*)   - OPM, course marketplace, credentialing
  Corporate Learn  (det_corp*)     - LXP, simulation, compliance, upskilling
  Language Learn   (det_lang*)     - app-based, tutoring, immersion, AI-driven
  STEM Education   (det_stem*)     - coding bootcamp, lab simulation, robotics

Source: HolonIQ global EdTech taxonomy and NAICS 611x education subsectors.
Public domain. Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
EDTECH_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- K-12 EdTech category --
    ("det_k12",              "K-12 EdTech",                                            1, None),
    ("det_k12_lms",          "K-12 Learning Management System (Canvas, Schoology)",   2, "det_k12"),
    ("det_k12_adaptive",     "Adaptive Learning Platform (DreamBox, i-Ready)",        2, "det_k12"),
    ("det_k12_assess",       "Digital Assessment and Testing (MAP, SBAC, iStation)",  2, "det_k12"),
    ("det_k12_classroom",    "Classroom Management Tool (ClassDojo, Google Classroom)",2, "det_k12"),
    ("det_k12_curriculum",   "Digital Curriculum and Content (Khan Academy, IXL)",    2, "det_k12"),

    # -- Higher Ed Platforms category --
    ("det_higher",           "Higher Education Platforms",                              1, None),
    ("det_higher_opm",       "Online Program Management (OPM, 2U, Noodle)",           2, "det_higher"),
    ("det_higher_mooc",      "MOOC and Course Marketplace (Coursera, edX, Udemy)",    2, "det_higher"),
    ("det_higher_credential","Digital Credential and Micro-Credential (Credly, Parchment)", 2, "det_higher"),
    ("det_higher_sis",       "Student Information System (Ellucian, Workday Student)",2, "det_higher"),

    # -- Corporate Learning category --
    ("det_corp",             "Corporate Learning and Development",                     1, None),
    ("det_corp_lxp",         "Learning Experience Platform (Degreed, EdCast, Cornerstone)",2, "det_corp"),
    ("det_corp_sim",         "Simulation and Scenario Training (VR, role-play)",       2, "det_corp"),
    ("det_corp_compliance",  "Compliance and Mandatory Training (SOC, HIPAA, OSHA)",  2, "det_corp"),
    ("det_corp_upskill",     "Upskilling and Reskilling Platform (Pluralsight, Udacity)",2, "det_corp"),
    ("det_corp_coach",       "Coaching and Mentoring Platform (BetterUp, CoachHub)",   2, "det_corp"),

    # -- Language Learning category --
    ("det_lang",             "Language Learning",                                       1, None),
    ("det_lang_app",         "App-Based Language Learning (Duolingo, Babbel, Busuu)", 2, "det_lang"),
    ("det_lang_tutor",       "Online Language Tutoring (iTalki, Preply, Cambly)",     2, "det_lang"),
    ("det_lang_immersion",   "Immersive and VR Language Learning",                    2, "det_lang"),
    ("det_lang_ai",          "AI Conversation and Speech Practice Tool",               2, "det_lang"),

    # -- STEM Education category --
    ("det_stem",             "STEM Education Platforms",                                1, None),
    ("det_stem_coding",      "Coding Bootcamp and Platform (Codecademy, freeCodeCamp)",2, "det_stem"),
    ("det_stem_lab",         "Virtual Lab and Science Simulation (Labster, PhET)",    2, "det_stem"),
    ("det_stem_robotics",    "Robotics and Hardware Learning Kit (LEGO, Arduino)",    2, "det_stem"),
    ("det_stem_data",        "Data Science and AI Education (DataCamp, fast.ai)",     2, "det_stem"),
]

_DOMAIN_ROW = (
    "domain_edtech_platform",
    "EdTech Platform Types",
    "K-12, higher ed, corporate learning, language learning and STEM education taxonomy",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["6111", "6112", "6113"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific edtech types."""
    parts = code.split("_")
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_edtech_platform(conn) -> int:
    """Ingest EdTech Platform domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_edtech_platform'), and links NAICS 6111/6112/6113 nodes
    via node_taxonomy_link.

    Returns total edtech platform node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_edtech_platform",
        "EdTech Platform Types",
        "K-12, higher ed, corporate learning, language learning and STEM education taxonomy",
        "1.0",
        "Global",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in EDTECH_NODES if parent is not None}

    rows = [
        (
            "domain_edtech_platform",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in EDTECH_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(EDTECH_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_edtech_platform'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_edtech_platform'",
        count,
    )

    naics_codes = [
        row["code"]
        for prefix in _NAICS_PREFIXES
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE $1",
            prefix + "%",
        )
    ]

    if naics_codes:
        await conn.executemany(
            """INSERT INTO node_taxonomy_link
                   (system_id, node_code, taxonomy_id, relevance)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
            [("naics_2022", code, "domain_edtech_platform", "primary") for code in naics_codes],
        )

    return count
