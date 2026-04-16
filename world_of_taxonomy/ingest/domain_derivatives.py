"""Ingest Financial Derivatives Instrument Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_derivatives",
    "Derivatives Instrument Types",
    "Financial Derivatives Instrument Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("der_opt", "Options", 1, None),
    ("der_fut", "Futures and Forwards", 1, None),
    ("der_swap", "Swaps", 1, None),
    ("der_exot", "Exotic and Structured", 1, None),
    ("der_opt_call", "Call options", 2, "der_opt"),
    ("der_opt_put", "Put options", 2, "der_opt"),
    ("der_opt_amer", "American-style (early exercise)", 2, "der_opt"),
    ("der_opt_euro", "European-style (expiry only)", 2, "der_opt"),
    ("der_opt_fx", "FX options (vanilla, barrier)", 2, "der_opt"),
    ("der_fut_comm", "Commodity futures", 2, "der_fut"),
    ("der_fut_idx", "Equity index futures", 2, "der_fut"),
    ("der_fut_ir", "Interest rate futures", 2, "der_fut"),
    ("der_fut_fx", "Currency forwards and futures", 2, "der_fut"),
    ("der_swap_irs", "Interest rate swaps", 2, "der_swap"),
    ("der_swap_cds", "Credit default swaps", 2, "der_swap"),
    ("der_swap_trs", "Total return swaps", 2, "der_swap"),
    ("der_swap_cur", "Currency swaps", 2, "der_swap"),
    ("der_swap_eq", "Equity swaps", 2, "der_swap"),
    ("der_exot_bar", "Barrier options", 2, "der_exot"),
    ("der_exot_bin", "Binary/digital options", 2, "der_exot"),
    ("der_exot_cdo", "CDOs and CLOs", 2, "der_exot"),
    ("der_exot_var", "Variance and volatility swaps", 2, "der_exot"),
]


async def ingest_domain_derivatives(conn) -> int:
    """Insert or update Derivatives Instrument Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_derivatives"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_derivatives", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_derivatives",
    )
    return count
