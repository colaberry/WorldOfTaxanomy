"""Ingest Private Equity and Venture Capital Stage Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_pe_stage",
    "Private Equity Stage Types",
    "Private Equity and Venture Capital Stage Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pe_vc", "Venture Capital Stages", 1, None),
    ("pe_growth", "Growth and Buyout", 1, None),
    ("pe_strat", "Strategy Types", 1, None),
    ("pe_vc_pre", "Pre-seed and angel", 2, "pe_vc"),
    ("pe_vc_seed", "Seed round", 2, "pe_vc"),
    ("pe_vc_a", "Series A", 2, "pe_vc"),
    ("pe_vc_b", "Series B", 2, "pe_vc"),
    ("pe_vc_c", "Series C and later", 2, "pe_vc"),
    ("pe_vc_bridge", "Bridge and mezzanine", 2, "pe_vc"),
    ("pe_growth_exp", "Growth equity", 2, "pe_growth"),
    ("pe_growth_lbo", "Leveraged buyout (LBO)", 2, "pe_growth"),
    ("pe_growth_mbo", "Management buyout (MBO)", 2, "pe_growth"),
    ("pe_growth_recap", "Recapitalization", 2, "pe_growth"),
    ("pe_growth_sec", "Secondary transactions", 2, "pe_growth"),
    ("pe_strat_dist", "Distressed and turnaround", 2, "pe_strat"),
    ("pe_strat_infra", "Infrastructure PE", 2, "pe_strat"),
    ("pe_strat_re", "Real estate PE", 2, "pe_strat"),
    ("pe_strat_nat", "Natural resources PE", 2, "pe_strat"),
    ("pe_strat_impact", "Impact investing PE", 2, "pe_strat"),
    ("pe_strat_fund", "Fund of funds", 2, "pe_strat"),
]


async def ingest_domain_pe_stage(conn) -> int:
    """Insert or update Private Equity Stage Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_pe_stage"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_pe_stage", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_pe_stage",
    )
    return count
