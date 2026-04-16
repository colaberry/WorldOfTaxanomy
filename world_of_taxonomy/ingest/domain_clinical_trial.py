"""Ingest Clinical Trial Phase and Design Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_clinical_trial",
    "Clinical Trial Classification",
    "Clinical Trial Phase and Design Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ct_phase", "Trial Phases", 1, None),
    ("ct_design", "Study Designs", 1, None),
    ("ct_endpoint", "Endpoint Types", 1, None),
    ("ct_phase_0", "Phase 0 (exploratory IND)", 2, "ct_phase"),
    ("ct_phase_1", "Phase I (safety, dose-finding)", 2, "ct_phase"),
    ("ct_phase_1b", "Phase Ib (expansion cohorts)", 2, "ct_phase"),
    ("ct_phase_2", "Phase II (efficacy, dose-ranging)", 2, "ct_phase"),
    ("ct_phase_2b", "Phase IIb (dose-confirming)", 2, "ct_phase"),
    ("ct_phase_3", "Phase III (confirmatory, pivotal)", 2, "ct_phase"),
    ("ct_phase_3b", "Phase IIIb (supplemental studies)", 2, "ct_phase"),
    ("ct_phase_4", "Phase IV (post-marketing)", 2, "ct_phase"),
    ("ct_design_rct", "Randomized controlled trial (RCT)", 2, "ct_design"),
    ("ct_design_cross", "Crossover design", 2, "ct_design"),
    ("ct_design_adapt", "Adaptive design (platform, basket, umbrella)", 2, "ct_design"),
    ("ct_design_obs", "Observational study (cohort, case-control)", 2, "ct_design"),
    ("ct_design_single", "Single-arm study", 2, "ct_design"),
    ("ct_design_rwe", "Real-world evidence study", 2, "ct_design"),
    ("ct_ep_primary", "Primary efficacy endpoint", 2, "ct_endpoint"),
    ("ct_ep_secondary", "Secondary endpoints", 2, "ct_endpoint"),
    ("ct_ep_surrogate", "Surrogate endpoints (biomarker-based)", 2, "ct_endpoint"),
    ("ct_ep_composite", "Composite endpoints", 2, "ct_endpoint"),
    ("ct_ep_pro", "Patient-reported outcomes (PRO)", 2, "ct_endpoint"),
]


async def ingest_domain_clinical_trial(conn) -> int:
    """Insert or update Clinical Trial Classification system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_clinical_trial"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_clinical_trial", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_clinical_trial",
    )
    return count
