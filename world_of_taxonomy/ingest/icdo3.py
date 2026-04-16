"""ICD-O-3 International Classification of Diseases for Oncology skeleton ingester.

ICD-O-3 is a dual-axis classification system for oncology used by cancer
registries worldwide. It classifies neoplasms by topography (site) using
the C00-C80 ICD-10 codes, and by morphology (histology and behaviour) using
a 5-digit code. This ingester covers the 20 topography chapters and
40 morphology groups as a skeleton. Used by cancer registries, NCI SEER,
hospital tumour boards, and population cancer surveillance programmes.
"""
from __future__ import annotations

SYSTEM_ID = "icdo3"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Topography main groups (level 1)
    ("TOPO",   "Topography (Anatomic Site)",                      1, None),
    ("MORPH",  "Morphology (Histology)",                          1, None),
    # Topography chapters (level 2) - C00-C80
    ("T-C00",  "Lip",                                             2, "TOPO"),
    ("T-C01",  "Base of Tongue",                                  2, "TOPO"),
    ("T-C02",  "Other and Unspecified Parts of Tongue",           2, "TOPO"),
    ("T-C03",  "Gum",                                             2, "TOPO"),
    ("T-C04",  "Floor of Mouth",                                  2, "TOPO"),
    ("T-C05",  "Palate",                                          2, "TOPO"),
    ("T-C06",  "Other and Unspecified Parts of Mouth",            2, "TOPO"),
    ("T-C07",  "Parotid Gland",                                   2, "TOPO"),
    ("T-C08",  "Other and Unspecified Major Salivary Glands",     2, "TOPO"),
    ("T-C09",  "Tonsil",                                          2, "TOPO"),
    ("T-C10",  "Oropharynx",                                      2, "TOPO"),
    ("T-C11",  "Nasopharynx",                                     2, "TOPO"),
    ("T-C12",  "Pyriform Sinus",                                  2, "TOPO"),
    ("T-C13",  "Hypopharynx",                                     2, "TOPO"),
    ("T-C14",  "Other Ill-Defined Sites in Lip, Oral Cavity",     2, "TOPO"),
    ("T-C15",  "Esophagus",                                       2, "TOPO"),
    ("T-C16",  "Stomach",                                         2, "TOPO"),
    ("T-C17",  "Small Intestine",                                 2, "TOPO"),
    ("T-C18",  "Colon",                                           2, "TOPO"),
    ("T-C19",  "Rectosigmoid Junction",                           2, "TOPO"),
    ("T-C20",  "Rectum",                                          2, "TOPO"),
    ("T-C21",  "Anus and Anal Canal",                             2, "TOPO"),
    ("T-C22",  "Liver and Intrahepatic Bile Ducts",               2, "TOPO"),
    ("T-C23",  "Gallbladder",                                     2, "TOPO"),
    ("T-C24",  "Other Biliary Tract",                             2, "TOPO"),
    ("T-C25",  "Pancreas",                                        2, "TOPO"),
    ("T-C26",  "Other Digestive Organs",                          2, "TOPO"),
    ("T-C30",  "Nasal Cavity and Middle Ear",                     2, "TOPO"),
    ("T-C31",  "Accessory Sinuses",                               2, "TOPO"),
    ("T-C32",  "Larynx",                                          2, "TOPO"),
    ("T-C33",  "Trachea",                                         2, "TOPO"),
    ("T-C34",  "Bronchus and Lung",                               2, "TOPO"),
    ("T-C37",  "Thymus",                                          2, "TOPO"),
    ("T-C38",  "Heart, Mediastinum and Pleura",                   2, "TOPO"),
    ("T-C39",  "Other Respiratory and Intrathoracic Organs",      2, "TOPO"),
    ("T-C40",  "Bones and Joints",                                2, "TOPO"),
    ("T-C41",  "Bones and Articular Cartilage of Other Sites",    2, "TOPO"),
    ("T-C42",  "Hematopoietic and Reticuloendothelial Systems",   2, "TOPO"),
    ("T-C44",  "Skin",                                            2, "TOPO"),
    ("T-C47",  "Peripheral Nerves and Autonomic Nervous System",  2, "TOPO"),
    ("T-C48",  "Retroperitoneum and Peritoneum",                  2, "TOPO"),
    ("T-C49",  "Connective, Subcutaneous and Other Soft Tissues", 2, "TOPO"),
    ("T-C50",  "Breast",                                          2, "TOPO"),
    ("T-C51",  "Vulva",                                           2, "TOPO"),
    ("T-C52",  "Vagina",                                          2, "TOPO"),
    ("T-C53",  "Cervix Uteri",                                    2, "TOPO"),
    ("T-C54",  "Corpus Uteri",                                    2, "TOPO"),
    ("T-C55",  "Uterus",                                          2, "TOPO"),
    ("T-C56",  "Ovary",                                           2, "TOPO"),
    ("T-C57",  "Other Female Genital Organs",                     2, "TOPO"),
    ("T-C58",  "Placenta",                                        2, "TOPO"),
    ("T-C60",  "Penis",                                           2, "TOPO"),
    ("T-C61",  "Prostate Gland",                                  2, "TOPO"),
    ("T-C62",  "Testis",                                          2, "TOPO"),
    ("T-C63",  "Other Male Genital Organs",                       2, "TOPO"),
    ("T-C64",  "Kidney",                                          2, "TOPO"),
    ("T-C65",  "Renal Pelvis",                                    2, "TOPO"),
    ("T-C66",  "Ureter",                                          2, "TOPO"),
    ("T-C67",  "Bladder",                                         2, "TOPO"),
    ("T-C68",  "Other Urinary Organs",                            2, "TOPO"),
    ("T-C69",  "Eye and Adnexa",                                  2, "TOPO"),
    ("T-C70",  "Meninges",                                        2, "TOPO"),
    ("T-C71",  "Brain",                                           2, "TOPO"),
    ("T-C72",  "Spinal Cord and Cranial Nerves",                  2, "TOPO"),
    ("T-C73",  "Thyroid Gland",                                   2, "TOPO"),
    ("T-C74",  "Adrenal Gland",                                   2, "TOPO"),
    ("T-C75",  "Other Endocrine Glands and Related Structures",   2, "TOPO"),
    ("T-C76",  "Other and Ill-Defined Sites",                     2, "TOPO"),
    ("T-C77",  "Lymph Nodes",                                     2, "TOPO"),
    ("T-C80",  "Unknown Primary Site",                            2, "TOPO"),
    # Morphology groups (level 2)
    ("M-800",  "Neoplasms NOS",                                   2, "MORPH"),
    ("M-801",  "Epithelial Neoplasms NOS",                        2, "MORPH"),
    ("M-803",  "Squamous Cell Neoplasms",                         2, "MORPH"),
    ("M-805",  "Papillary and Squamous Cell Neoplasms",           2, "MORPH"),
    ("M-807",  "Basal Cell Neoplasms",                            2, "MORPH"),
    ("M-809",  "Adenomas and Adenocarcinomas",                    2, "MORPH"),
    ("M-814",  "Adenocarcinoma (specific types)",                 2, "MORPH"),
    ("M-820",  "Cystic, Mucinous and Serous Neoplasms",           2, "MORPH"),
    ("M-840",  "Ductal, Lobular and Medullary Neoplasms",         2, "MORPH"),
    ("M-850",  "Acinar Cell Neoplasms",                           2, "MORPH"),
    ("M-860",  "Complex Epithelial Neoplasms",                    2, "MORPH"),
    ("M-870",  "Thymic Epithelial Neoplasms",                     2, "MORPH"),
    ("M-880",  "Soft Tissue Tumors and Sarcomas NOS",             2, "MORPH"),
    ("M-881",  "Fibromatous Neoplasms",                           2, "MORPH"),
    ("M-888",  "Lipomatous Neoplasms",                            2, "MORPH"),
    ("M-889",  "Myomatous Neoplasms",                             2, "MORPH"),
    ("M-894",  "Complex Mixed and Stromal Neoplasms",             2, "MORPH"),
    ("M-896",  "Fibroepithelial Neoplasms",                       2, "MORPH"),
    ("M-900",  "Trophoblastic Neoplasms",                         2, "MORPH"),
    ("M-904",  "Mesonephromas",                                   2, "MORPH"),
    ("M-905",  "Mesothelial Neoplasms",                           2, "MORPH"),
    ("M-906",  "Germ Cell Neoplasms",                             2, "MORPH"),
    ("M-912",  "Blood Vessel Tumors",                             2, "MORPH"),
    ("M-915",  "Lymphatic Vessel Tumors",                         2, "MORPH"),
    ("M-920",  "Osseous and Chondromatous Neoplasms",             2, "MORPH"),
    ("M-924",  "Giant Cell Tumors",                               2, "MORPH"),
    ("M-926",  "Miscellaneous Bone Tumors",                       2, "MORPH"),
    ("M-930",  "Odontogenic Tumors",                              2, "MORPH"),
    ("M-935",  "Miscellaneous Tumors",                            2, "MORPH"),
    ("M-936",  "Gliomas",                                         2, "MORPH"),
    ("M-938",  "Neuroepitheliomatous Neoplasms",                  2, "MORPH"),
    ("M-940",  "Meningiomas",                                     2, "MORPH"),
    ("M-950",  "Lymphomas and Related or Unspecified Neoplasms",  2, "MORPH"),
    ("M-965",  "Hodgkin Lymphomas",                               2, "MORPH"),
    ("M-966",  "Non-Hodgkin Lymphomas",                           2, "MORPH"),
    ("M-970",  "Other Lymphoreticular Neoplasms",                 2, "MORPH"),
    ("M-975",  "Plasma Cell Tumors",                              2, "MORPH"),
    ("M-980",  "Mast Cell Tumors",                                2, "MORPH"),
    ("M-982",  "Immunoproliferative Diseases",                    2, "MORPH"),
    ("M-990",  "Leukemias NOS",                                   2, "MORPH"),
    ("M-994",  "Lymphoid Leukemias",                              2, "MORPH"),
    ("M-996",  "Myeloid Leukemias",                               2, "MORPH"),
    ("M-998",  "Other Leukemias",                                 2, "MORPH"),
]


async def ingest_icdo3(conn) -> int:
    """Ingest ICD-O-3 skeleton (topography sites and morphology groups)."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "ICD-O-3",
        "International Classification of Diseases for Oncology, Third Edition",
        "Global (WHO)",
        "3.2",
        "World Health Organization (WHO)",
        "#BE185D",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = parent_code if parent_code else code
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
    print(f"  Ingested {count} ICD-O-3 codes")
    return count
