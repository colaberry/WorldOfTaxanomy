"""Ingest Mental Health Service and Treatment Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_mental_health",
    "Mental Health Service Types",
    "Mental Health Service and Treatment Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("mh_set", "Service Settings", 1, None),
    ("mh_ther", "Therapy Modalities", 1, None),
    ("mh_pop", "Population Focus", 1, None),
    ("mh_set_inpat", "Inpatient psychiatric facility", 2, "mh_set"),
    ("mh_set_outpat", "Outpatient mental health clinic", 2, "mh_set"),
    ("mh_set_crisis", "Crisis intervention and stabilization", 2, "mh_set"),
    ("mh_set_resid", "Residential treatment facility", 2, "mh_set"),
    ("mh_set_tele", "Telepsychiatry and digital mental health", 2, "mh_set"),
    ("mh_set_comm", "Community mental health center", 2, "mh_set"),
    ("mh_ther_cbt", "Cognitive behavioral therapy (CBT)", 2, "mh_ther"),
    ("mh_ther_dbt", "Dialectical behavior therapy (DBT)", 2, "mh_ther"),
    ("mh_ther_emdr", "EMDR (eye movement desensitization)", 2, "mh_ther"),
    ("mh_ther_group", "Group therapy", 2, "mh_ther"),
    ("mh_ther_fam", "Family and couples therapy", 2, "mh_ther"),
    ("mh_ther_art", "Art and music therapy", 2, "mh_ther"),
    ("mh_ther_med", "Psychopharmacology", 2, "mh_ther"),
    ("mh_pop_child", "Child and adolescent services", 2, "mh_pop"),
    ("mh_pop_adult", "Adult mental health services", 2, "mh_pop"),
    ("mh_pop_geri", "Geriatric psychiatry", 2, "mh_pop"),
    ("mh_pop_sud", "Substance use disorder treatment", 2, "mh_pop"),
    ("mh_pop_vet", "Veteran and military mental health", 2, "mh_pop"),
    ("mh_pop_forens", "Forensic mental health", 2, "mh_pop"),
]


async def ingest_domain_mental_health(conn) -> int:
    """Insert or update Mental Health Service Types system and its nodes. Returns node count."""
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count,
                source_url, data_provenance, license)
           VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)
           ON CONFLICT (id) DO UPDATE
               SET node_count = 0, source_url = EXCLUDED.source_url,
                   data_provenance = EXCLUDED.data_provenance, license = EXCLUDED.license""",
        *_SYSTEM_ROW, _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute(
        "DELETE FROM classification_node WHERE system_id = $1", "domain_mental_health"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_mental_health", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_mental_health",
    )
    return count
