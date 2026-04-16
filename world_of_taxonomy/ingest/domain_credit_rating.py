"""Ingest Credit Rating Scale and Methodology Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_credit_rating",
    "Credit Rating Scale Types",
    "Credit Rating Scale and Methodology Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cr_lt", "Long-Term Ratings", 1, None),
    ("cr_st", "Short-Term Ratings", 1, None),
    ("cr_type", "Rating Categories", 1, None),
    ("cr_lt_aaa", "AAA/Aaa (highest quality)", 2, "cr_lt"),
    ("cr_lt_aa", "AA/Aa (high quality)", 2, "cr_lt"),
    ("cr_lt_a", "A (upper medium grade)", 2, "cr_lt"),
    ("cr_lt_bbb", "BBB/Baa (medium grade, investment)", 2, "cr_lt"),
    ("cr_lt_bb", "BB/Ba (speculative, non-investment)", 2, "cr_lt"),
    ("cr_lt_b", "B (highly speculative)", 2, "cr_lt"),
    ("cr_lt_ccc", "CCC/Caa (substantial risk)", 2, "cr_lt"),
    ("cr_lt_d", "D/C (default)", 2, "cr_lt"),
    ("cr_st_a1", "A-1/P-1 (superior short-term)", 2, "cr_st"),
    ("cr_st_a2", "A-2/P-2 (satisfactory short-term)", 2, "cr_st"),
    ("cr_st_a3", "A-3/P-3 (adequate short-term)", 2, "cr_st"),
    ("cr_st_np", "NP/B or below (non-prime short-term)", 2, "cr_st"),
    ("cr_type_sov", "Sovereign credit ratings", 2, "cr_type"),
    ("cr_type_corp", "Corporate credit ratings", 2, "cr_type"),
    ("cr_type_muni", "Municipal bond ratings", 2, "cr_type"),
    ("cr_type_struct", "Structured finance ratings", 2, "cr_type"),
    ("cr_type_bank", "Bank financial strength ratings", 2, "cr_type"),
    ("cr_type_insur", "Insurance financial strength ratings", 2, "cr_type"),
]


async def ingest_domain_credit_rating(conn) -> int:
    """Insert or update Credit Rating Scale Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_credit_rating"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_credit_rating", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_credit_rating",
    )
    return count
