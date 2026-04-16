"""Australian and New Zealand Standard Research Classification - Fields of Research 2020.

ANZSRC FOR 2020 is the classification used by the Australian Research Council (ARC)
and New Zealand research funding agencies to classify R&D activity. The 2020 revision
replaced the 2008 edition with 22 two-digit divisions and over 150 four-digit groups.
Used for ARC grant applications, ERA (Excellence in Research for Australia), and
university HERDC reporting.
"""
from __future__ import annotations

SYSTEM_ID = "anzsrc_for_2020"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Divisions (level 1)
    ("30", "Agricultural, Veterinary and Food Sciences",  1, None),
    ("31", "Biological Sciences",                         1, None),
    ("32", "Biomedical and Clinical Sciences",            1, None),
    ("33", "Built Environment and Design",                1, None),
    ("34", "Chemical Sciences",                          1, None),
    ("35", "Commerce, Management, Tourism and Services",  1, None),
    ("36", "Creative Arts and Writing",                   1, None),
    ("37", "Earth Sciences",                              1, None),
    ("38", "Economics",                                   1, None),
    ("39", "Education",                                   1, None),
    ("40", "Engineering",                                 1, None),
    ("41", "Environmental Sciences",                      1, None),
    ("42", "Health Sciences",                             1, None),
    ("43", "History, Heritage and Archaeology",           1, None),
    ("44", "Human Society",                               1, None),
    ("45", "Indigenous Studies",                          1, None),
    ("46", "Information and Computing Sciences",          1, None),
    ("47", "Language, Communication and Culture",         1, None),
    ("48", "Law and Legal Studies",                       1, None),
    ("49", "Mathematical Sciences",                       1, None),
    ("50", "Philosophy and Religious Studies",            1, None),
    ("52", "Psychology",                                  1, None),
    # Groups (level 2)
    ("3001", "Agricultural Biotechnology",                          2, "30"),
    ("3002", "Agriculture, Land and Farm Management",               2, "30"),
    ("3003", "Animal Production",                                   2, "30"),
    ("3004", "Crop and Pasture Production",                         2, "30"),
    ("3005", "Fisheries Sciences",                                  2, "30"),
    ("3006", "Food Sciences",                                       2, "30"),
    ("3007", "Forestry Sciences",                                   2, "30"),
    ("3008", "Horticultural Production",                            2, "30"),
    ("3009", "Veterinary Sciences",                                 2, "30"),
    ("3101", "Biochemistry and Cell Biology",                       2, "31"),
    ("3102", "Ecology",                                             2, "31"),
    ("3103", "Evolutionary Biology",                                2, "31"),
    ("3104", "Genetics",                                            2, "31"),
    ("3105", "Microbiology",                                        2, "31"),
    ("3106", "Plant Biology",                                       2, "31"),
    ("3107", "Zoology",                                             2, "31"),
    ("3201", "Cardiovascular Medicine and Haematology",             2, "32"),
    ("3202", "Clinical Sciences",                                   2, "32"),
    ("3203", "Dentistry",                                           2, "32"),
    ("3204", "Immunology",                                          2, "32"),
    ("3205", "Medical Biochemistry and Metabolomics",               2, "32"),
    ("3206", "Medical Physiology",                                  2, "32"),
    ("3207", "Neurosciences",                                       2, "32"),
    ("3208", "Oncology and Carcinogenesis",                         2, "32"),
    ("3301", "Architecture",                                        2, "33"),
    ("3302", "Building",                                            2, "33"),
    ("3303", "Urban and Regional Planning",                         2, "33"),
    ("3304", "Landscape Architecture",                              2, "33"),
    ("3401", "Analytical Chemistry",                                2, "34"),
    ("3402", "Inorganic Chemistry",                                 2, "34"),
    ("3403", "Macromolecular and Materials Chemistry",              2, "34"),
    ("3404", "Medicinal and Biomolecular Chemistry",                2, "34"),
    ("3405", "Organic Chemistry",                                   2, "34"),
    ("3406", "Physical Chemistry",                                  2, "34"),
    ("3501", "Accounting, Auditing and Accountability",             2, "35"),
    ("3502", "Banking, Finance and Investment",                     2, "35"),
    ("3503", "Business Systems in Context",                         2, "35"),
    ("3504", "Commercial Services",                                 2, "35"),
    ("3505", "Human Resources and Industrial Relations",            2, "35"),
    ("3506", "Marketing",                                           2, "35"),
    ("3507", "Strategy, Management and Organisational Behaviour",   2, "35"),
    ("3508", "Tourism",                                             2, "35"),
    ("3601", "Art History, Theory and Criticism",                   2, "36"),
    ("3602", "Creative and Professional Writing",                   2, "36"),
    ("3603", "Music",                                               2, "36"),
    ("3604", "Performing Arts",                                     2, "36"),
    ("3605", "Screen and Digital Media",                            2, "36"),
    ("3701", "Atmospheric Sciences",                                2, "37"),
    ("3702", "Climate Change Science",                              2, "37"),
    ("3703", "Geochemistry",                                        2, "37"),
    ("3704", "Geoinformatics",                                      2, "37"),
    ("3705", "Geology",                                             2, "37"),
    ("3706", "Geophysics",                                          2, "37"),
    ("3707", "Oceanography",                                        2, "37"),
    ("3801", "Applied Economics",                                   2, "38"),
    ("3802", "Econometrics",                                        2, "38"),
    ("3803", "Economic Theory",                                     2, "38"),
    ("3901", "Curriculum and Pedagogy",                             2, "39"),
    ("3902", "Education Policy, Sociology and Philosophy",          2, "39"),
    ("3903", "Education Systems",                                   2, "39"),
    ("3904", "Specialist Studies in Education",                     2, "39"),
    ("4001", "Aerospace Engineering",                               2, "40"),
    ("4002", "Automotive Engineering",                              2, "40"),
    ("4003", "Biomedical Engineering",                              2, "40"),
    ("4004", "Chemical Engineering",                                2, "40"),
    ("4005", "Civil Engineering",                                   2, "40"),
    ("4006", "Communications Engineering",                          2, "40"),
    ("4007", "Control Engineering, Mechatronics and Robotics",      2, "40"),
    ("4008", "Electrical Engineering",                              2, "40"),
    ("4009", "Electronics, Sensors and Digital Hardware",           2, "40"),
    ("4010", "Engineering Practice and Education",                  2, "40"),
    ("4011", "Environmental Engineering",                           2, "40"),
    ("4012", "Fluid Mechanics and Thermal Engineering",             2, "40"),
    ("4013", "Geomatic Engineering",                                2, "40"),
    ("4014", "Manufacturing Engineering",                           2, "40"),
    ("4015", "Maritime Engineering",                                2, "40"),
    ("4016", "Materials Engineering",                               2, "40"),
    ("4017", "Mechanical Engineering",                              2, "40"),
    ("4018", "Nanotechnology",                                      2, "40"),
    ("4019", "Resources Engineering and Extractive Metallurgy",     2, "40"),
    ("4101", "Climate Change Impacts and Adaptation",               2, "41"),
    ("4102", "Ecological Applications",                             2, "41"),
    ("4103", "Environmental Biotechnology",                         2, "41"),
    ("4104", "Environmental Management",                            2, "41"),
    ("4105", "Pollution and Contamination",                         2, "41"),
    ("4106", "Soil Sciences",                                       2, "41"),
    ("4201", "Allied Health and Rehabilitation Science",            2, "42"),
    ("4202", "Epidemiology",                                        2, "42"),
    ("4203", "Health Services and Systems",                         2, "42"),
    ("4204", "Midwifery",                                           2, "42"),
    ("4205", "Nursing",                                             2, "42"),
    ("4206", "Public Health",                                       2, "42"),
    ("4207", "Sports Science and Exercise",                         2, "42"),
    ("4301", "Archaeology",                                         2, "43"),
    ("4302", "Heritage, Museum and Conservation Studies",           2, "43"),
    ("4303", "Historical Studies",                                  2, "43"),
    ("4401", "Anthropology",                                        2, "44"),
    ("4402", "Criminology",                                         2, "44"),
    ("4403", "Demography",                                          2, "44"),
    ("4404", "Development Studies",                                 2, "44"),
    ("4405", "Gender Studies",                                      2, "44"),
    ("4406", "Human Geography",                                     2, "44"),
    ("4407", "Policy and Administration",                           2, "44"),
    ("4408", "Political Science",                                   2, "44"),
    ("4409", "Social Work",                                         2, "44"),
    ("4410", "Sociology",                                           2, "44"),
    ("4501", "Aboriginal and Torres Strait Islander Studies",       2, "45"),
    ("4502", "Maori Peoples Studies",                               2, "45"),
    ("4503", "Pacific Peoples Studies",                             2, "45"),
    ("4601", "Applied Computing",                                   2, "46"),
    ("4602", "Artificial Intelligence",                             2, "46"),
    ("4603", "Computer Vision and Multimedia Computation",          2, "46"),
    ("4604", "Cybersecurity and Privacy",                           2, "46"),
    ("4605", "Data Management and Data Science",                    2, "46"),
    ("4606", "Distributed Computing and Systems Software",          2, "46"),
    ("4607", "Graphics, Augmented Reality and Games",               2, "46"),
    ("4608", "Human-Centred Computing",                             2, "46"),
    ("4609", "Information Systems",                                 2, "46"),
    ("4610", "Library and Information Studies",                     2, "46"),
    ("4611", "Machine Learning",                                    2, "46"),
    ("4612", "Software Engineering",                                2, "46"),
    ("4613", "Theory of Computation",                               2, "46"),
    ("4701", "Applied Linguistics and Language Technology",         2, "47"),
    ("4702", "Cultural Studies",                                    2, "47"),
    ("4703", "Language Studies",                                    2, "47"),
    ("4704", "Linguistics",                                         2, "47"),
    ("4705", "Literary Studies",                                    2, "47"),
    ("4801", "Commercial and Contract Law",                         2, "48"),
    ("4802", "Environmental and Resources Law",                     2, "48"),
    ("4803", "International and Comparative Law",                   2, "48"),
    ("4804", "Law in Context",                                      2, "48"),
    ("4805", "Legal Theory, Jurisprudence and Legal Institutions",  2, "48"),
    ("4901", "Applied Mathematics",                                 2, "49"),
    ("4902", "Mathematical Physics",                                2, "49"),
    ("4903", "Numerical and Computational Mathematics",             2, "49"),
    ("4904", "Pure Mathematics",                                    2, "49"),
    ("4905", "Statistics",                                          2, "49"),
    ("5001", "Philosophy",                                          2, "50"),
    ("5002", "Religion and Religious Studies",                      2, "50"),
    ("5201", "Applied and Developmental Psychology",                2, "52"),
    ("5202", "Biological Psychology",                               2, "52"),
    ("5203", "Clinical and Health Psychology",                      2, "52"),
    ("5204", "Cognitive and Computational Psychology",              2, "52"),
    ("5205", "Social and Personality Psychology",                   2, "52"),
]


async def ingest_anzsrc_for_2020(conn) -> int:
    """Ingest ANZSRC Fields of Research 2020 (Australian/NZ research classification)."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "ANZSRC FOR 2020",
        "Australian and New Zealand Standard Research Classification - Fields of Research 2020",
        "Australia / New Zealand",
        "2020",
        "Australian Bureau of Statistics (ABS) / Stats NZ",
        "#059669",
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
    print(f"  Ingested {count} ANZSRC FOR 2020 codes")
    return count
