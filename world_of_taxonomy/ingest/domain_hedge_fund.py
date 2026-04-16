"""Ingest Hedge Fund Investment Strategy Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_hedge_fund",
    "Hedge Fund Strategy Types",
    "Hedge Fund Investment Strategy Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("hf_eq", "Equity Strategies", 1, None),
    ("hf_macro", "Macro and Relative Value", 1, None),
    ("hf_event", "Event-Driven", 1, None),
    ("hf_quant", "Quantitative", 1, None),
    ("hf_eq_ls", "Long/short equity", 2, "hf_eq"),
    ("hf_eq_mn", "Market neutral equity", 2, "hf_eq"),
    ("hf_eq_sect", "Sector specialist", 2, "hf_eq"),
    ("hf_eq_activ", "Activist investing", 2, "hf_eq"),
    ("hf_macro_disc", "Discretionary macro", 2, "hf_macro"),
    ("hf_macro_sys", "Systematic macro / CTA", 2, "hf_macro"),
    ("hf_macro_rv", "Fixed income relative value", 2, "hf_macro"),
    ("hf_macro_fx", "FX and rates trading", 2, "hf_macro"),
    ("hf_event_merger", "Merger arbitrage", 2, "hf_event"),
    ("hf_event_dist", "Distressed debt", 2, "hf_event"),
    ("hf_event_spec", "Special situations", 2, "hf_event"),
    ("hf_quant_stat", "Statistical arbitrage", 2, "hf_quant"),
    ("hf_quant_hft", "High-frequency trading (HFT)", 2, "hf_quant"),
    ("hf_quant_ml", "Machine learning alpha", 2, "hf_quant"),
]


async def ingest_domain_hedge_fund(conn) -> int:
    """Insert or update Hedge Fund Strategy Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_hedge_fund"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_hedge_fund", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_hedge_fund",
    )
    return count
