"""HealthTech domain taxonomy ingester.

Organizes health technology sector types aligned with
NAICS 6211 (Offices of physicians),
NAICS 5112 (Software publishers).

Code prefix: dht_
Categories: Telemedicine, Remote Patient Monitoring,
Health Data Analytics, Digital Therapeutics, Clinical Trial Tech.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
HEALTHTECH_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Telemedicine --
    ("dht_telemed",            "Telemedicine",                                         1, None),
    ("dht_telemed_video",      "Video Consultation Platforms",                         2, "dht_telemed"),
    ("dht_telemed_async",      "Asynchronous (Store-and-Forward) Telemedicine",       2, "dht_telemed"),
    ("dht_telemed_triage",     "AI-Powered Symptom Triage and Routing",               2, "dht_telemed"),
    ("dht_telemed_mental",     "Telepsychiatry and Mental Health Platforms",           2, "dht_telemed"),

    # -- Remote Patient Monitoring --
    ("dht_rpm",                "Remote Patient Monitoring",                             1, None),
    ("dht_rpm_wearable",       "Wearable Health Devices and Biosensors",              2, "dht_rpm"),
    ("dht_rpm_chronic",        "Chronic Disease Monitoring Systems",                   2, "dht_rpm"),
    ("dht_rpm_cardiac",        "Remote Cardiac Monitoring and ECG Devices",            2, "dht_rpm"),
    ("dht_rpm_glucose",        "Continuous Glucose Monitoring (CGM) Platforms",        2, "dht_rpm"),

    # -- Health Data Analytics --
    ("dht_analytics",          "Health Data Analytics",                                 1, None),
    ("dht_analytics_pop",      "Population Health Analytics and Risk Stratification",  2, "dht_analytics"),
    ("dht_analytics_ehr",      "EHR Data Mining and Clinical Insights",                2, "dht_analytics"),
    ("dht_analytics_genomic",  "Genomic Data Analytics and Precision Medicine",        2, "dht_analytics"),
    ("dht_analytics_cost",     "Healthcare Cost and Utilization Analytics",             2, "dht_analytics"),
    ("dht_analytics_imaging",  "Medical Imaging AI and Diagnostics",                   2, "dht_analytics"),

    # -- Digital Therapeutics --
    ("dht_dtx",                "Digital Therapeutics",                                   1, None),
    ("dht_dtx_behavior",       "Behavioral Health Digital Interventions",              2, "dht_dtx"),
    ("dht_dtx_musculo",        "Musculoskeletal Digital Therapy Platforms",            2, "dht_dtx"),
    ("dht_dtx_substance",      "Substance Use Disorder Digital Programs",              2, "dht_dtx"),
    ("dht_dtx_cognitive",      "Cognitive Training and Neurotherapy Apps",             2, "dht_dtx"),

    # -- Clinical Trial Tech --
    ("dht_trial",              "Clinical Trial Tech",                                   1, None),
    ("dht_trial_recruit",      "Patient Recruitment and Matching Platforms",            2, "dht_trial"),
    ("dht_trial_edc",          "Electronic Data Capture (EDC) Systems",               2, "dht_trial"),
    ("dht_trial_decen",        "Decentralized Clinical Trial Platforms",               2, "dht_trial"),
    ("dht_trial_safety",       "Pharmacovigilance and Safety Reporting Tech",          2, "dht_trial"),
]

_DOMAIN_ROW = (
    "domain_healthtech",
    "HealthTech Types",
    "Health technology types covering telemedicine, remote patient monitoring, "
    "health data analytics, digital therapeutics, and clinical trial tech taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 6211 (Offices of physicians), 5112 (Software publishers)
_NAICS_PREFIXES = ["6211", "5112"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific HealthTech types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_healthtech(conn) -> int:
    """Ingest HealthTech domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_healthtech'), and links NAICS 6211/5112 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_healthtech",
        "HealthTech Types",
        "Health technology types covering telemedicine, remote patient monitoring, "
        "health data analytics, digital therapeutics, and clinical trial tech taxonomy",
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

    parent_codes = {parent for _, _, _, parent in HEALTHTECH_NODES if parent is not None}

    rows = [
        (
            "domain_healthtech",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in HEALTHTECH_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(HEALTHTECH_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_healthtech'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_healthtech'",
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
            [("naics_2022", code, "domain_healthtech", "primary") for code in naics_codes],
        )

    return count
