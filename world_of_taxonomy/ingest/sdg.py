"""UN Sustainable Development Goals (SDG) ingester.

The 2030 Agenda for Sustainable Development contains 17 goals and 169 targets.
Used by governments, NGOs, investors (SDG bonds, impact finance), and
development banks to classify projects and track progress.
"""
from __future__ import annotations

SYSTEM_ID = "sdg"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Goals (level 1)
    ("SDG1",  "No Poverty",                               1, None),
    ("SDG2",  "Zero Hunger",                              1, None),
    ("SDG3",  "Good Health and Well-Being",               1, None),
    ("SDG4",  "Quality Education",                        1, None),
    ("SDG5",  "Gender Equality",                          1, None),
    ("SDG6",  "Clean Water and Sanitation",               1, None),
    ("SDG7",  "Affordable and Clean Energy",              1, None),
    ("SDG8",  "Decent Work and Economic Growth",          1, None),
    ("SDG9",  "Industry, Innovation and Infrastructure",  1, None),
    ("SDG10", "Reduced Inequalities",                     1, None),
    ("SDG11", "Sustainable Cities and Communities",       1, None),
    ("SDG12", "Responsible Consumption and Production",   1, None),
    ("SDG13", "Climate Action",                           1, None),
    ("SDG14", "Life Below Water",                         1, None),
    ("SDG15", "Life on Land",                             1, None),
    ("SDG16", "Peace, Justice and Strong Institutions",   1, None),
    ("SDG17", "Partnerships for the Goals",               1, None),
    # Targets (level 2) - selected key targets per goal
    ("SDG1.1", "Eradicate extreme poverty ($1.25/day)",  2, "SDG1"),
    ("SDG1.2", "Reduce poverty by at least half",        2, "SDG1"),
    ("SDG1.3", "Social protection systems and floors",   2, "SDG1"),
    ("SDG1.4", "Equal rights to resources and services", 2, "SDG1"),
    ("SDG1.5", "Resilience of poor and vulnerable",      2, "SDG1"),
    ("SDG2.1", "End hunger and ensure food access",      2, "SDG2"),
    ("SDG2.2", "End all forms of malnutrition",          2, "SDG2"),
    ("SDG2.3", "Double agricultural productivity",       2, "SDG2"),
    ("SDG2.4", "Sustainable food production systems",    2, "SDG2"),
    ("SDG2.5", "Genetic diversity of seeds and crops",   2, "SDG2"),
    ("SDG3.1", "Reduce maternal mortality",              2, "SDG3"),
    ("SDG3.2", "End preventable deaths of children",     2, "SDG3"),
    ("SDG3.3", "End AIDS, TB, malaria, NTDs",            2, "SDG3"),
    ("SDG3.4", "Reduce NCDs and mental health",          2, "SDG3"),
    ("SDG3.8", "Universal health coverage",              2, "SDG3"),
    ("SDG4.1", "Free and quality primary/secondary ed",  2, "SDG4"),
    ("SDG4.3", "Access to TVET and higher education",    2, "SDG4"),
    ("SDG4.4", "Relevant skills for employment",         2, "SDG4"),
    ("SDG4.7", "Education for sustainable development",  2, "SDG4"),
    ("SDG5.1", "End discrimination against women",       2, "SDG5"),
    ("SDG5.2", "Eliminate violence against women",       2, "SDG5"),
    ("SDG5.5", "Women in leadership",                    2, "SDG5"),
    ("SDG6.1", "Safe and affordable drinking water",     2, "SDG6"),
    ("SDG6.2", "Adequate sanitation and hygiene",        2, "SDG6"),
    ("SDG6.3", "Water quality and wastewater treatment", 2, "SDG6"),
    ("SDG6.4", "Water use efficiency",                   2, "SDG6"),
    ("SDG7.1", "Universal access to energy services",    2, "SDG7"),
    ("SDG7.2", "Increase renewable energy share",        2, "SDG7"),
    ("SDG7.3", "Double energy efficiency improvement",   2, "SDG7"),
    ("SDG8.1", "Sustain per capita economic growth",     2, "SDG8"),
    ("SDG8.2", "Diversification and technology upgrade", 2, "SDG8"),
    ("SDG8.5", "Full employment and decent work",        2, "SDG8"),
    ("SDG8.7", "End child labour, forced labour",        2, "SDG8"),
    ("SDG8.10","Financial services for all",             2, "SDG8"),
    ("SDG9.1", "Resilient and sustainable infrastructure",2,"SDG9"),
    ("SDG9.2", "Inclusive and sustainable industrialisation",2,"SDG9"),
    ("SDG9.4", "Upgrade industries for sustainability",  2, "SDG9"),
    ("SDG9.5", "Enhance research and technology",        2, "SDG9"),
    ("SDG10.1","Income growth of bottom 40 percent",     2, "SDG10"),
    ("SDG10.4","Fiscal, wage and social protection",     2, "SDG10"),
    ("SDG10.7","Safe migration and mobility policies",   2, "SDG10"),
    ("SDG11.1","Safe and affordable housing",            2, "SDG11"),
    ("SDG11.2","Sustainable transport systems",          2, "SDG11"),
    ("SDG11.3","Inclusive and sustainable urbanisation", 2, "SDG11"),
    ("SDG11.6","Reduce environmental impact of cities",  2, "SDG11"),
    ("SDG12.2","Sustainable management of resources",    2, "SDG12"),
    ("SDG12.4","Responsible management of chemicals",    2, "SDG12"),
    ("SDG12.5","Reduce waste generation",                2, "SDG12"),
    ("SDG13.1","Resilience to climate hazards",          2, "SDG13"),
    ("SDG13.2","Climate change into policy and planning",2, "SDG13"),
    ("SDG13.3","Education and capacity on climate change",2,"SDG13"),
    ("SDG14.1","Prevent marine pollution",               2, "SDG14"),
    ("SDG14.2","Protect marine ecosystems",              2, "SDG14"),
    ("SDG14.5","Conserve coastal and marine areas",      2, "SDG14"),
    ("SDG15.1","Conserve terrestrial ecosystems",        2, "SDG15"),
    ("SDG15.2","Halt deforestation",                     2, "SDG15"),
    ("SDG15.5","Reduce loss of biodiversity",            2, "SDG15"),
    ("SDG16.1","Reduce violence and death rates",        2, "SDG16"),
    ("SDG16.3","Access to justice for all",              2, "SDG16"),
    ("SDG16.6","Accountable and transparent institutions",2,"SDG16"),
    ("SDG16.9","Legal identity for all",                 2, "SDG16"),
    ("SDG17.1","Strengthen domestic resource mobilisation",2,"SDG17"),
    ("SDG17.3","Mobilise financial resources",           2, "SDG17"),
    ("SDG17.16","Enhance global partnerships",           2, "SDG17"),
    ("SDG17.17","Effective partnerships",                2, "SDG17"),
]


async def ingest_sdg(conn) -> int:
    """Ingest UN Sustainable Development Goals taxonomy."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "SDG 2030",
        "UN Sustainable Development Goals 2030 Agenda",
        "Global (UN)",
        "2015",
        "United Nations",
        "#4ADE80",
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
    print(f"  Ingested {count} SDG codes")
    return count
