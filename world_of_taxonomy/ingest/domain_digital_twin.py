"""Ingest Digital Twin Application and Platform Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_digital_twin",
    "Digital Twin Application Types",
    "Digital Twin Application and Platform Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("dt_ind", "Industry Applications", 1, None),
    ("dt_comp", "Components", 1, None),
    ("dt_mat", "Maturity Levels", 1, None),
    ("dt_ind_mfg", "Manufacturing digital twin", 2, "dt_ind"),
    ("dt_ind_build", "Building and facility twin", 2, "dt_ind"),
    ("dt_ind_city", "City and infrastructure twin", 2, "dt_ind"),
    ("dt_ind_health", "Healthcare digital twin (patient, device)", 2, "dt_ind"),
    ("dt_ind_supply", "Supply chain digital twin", 2, "dt_ind"),
    ("dt_ind_energy", "Energy system digital twin", 2, "dt_ind"),
    ("dt_comp_model", "Physics-based models", 2, "dt_comp"),
    ("dt_comp_data", "Data integration and IoT feeds", 2, "dt_comp"),
    ("dt_comp_sim", "Simulation engine", 2, "dt_comp"),
    ("dt_comp_viz", "3D visualization", 2, "dt_comp"),
    ("dt_comp_ai", "AI and predictive analytics", 2, "dt_comp"),
    ("dt_mat_desc", "Descriptive twin (current state)", 2, "dt_mat"),
    ("dt_mat_info", "Informative twin (alerts and KPIs)", 2, "dt_mat"),
    ("dt_mat_pred", "Predictive twin (forecasting)", 2, "dt_mat"),
    ("dt_mat_presc", "Prescriptive twin (optimization)", 2, "dt_mat"),
    ("dt_mat_auto", "Autonomous twin (self-adjusting)", 2, "dt_mat"),
]


async def ingest_domain_digital_twin(conn) -> int:
    """Insert or update Digital Twin Application Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_digital_twin"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_digital_twin", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_digital_twin",
    )
    return count
