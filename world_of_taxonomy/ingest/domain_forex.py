"""Ingest Foreign Exchange Instrument and Market Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_forex",
    "Forex Instrument Types",
    "Foreign Exchange Instrument and Market Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("fx_spot", "Spot Market", 1, None),
    ("fx_deriv", "FX Derivatives", 1, None),
    ("fx_pair", "Currency Pair Categories", 1, None),
    ("fx_spot_inter", "Interbank spot FX", 2, "fx_spot"),
    ("fx_spot_retail", "Retail FX (CFDs, spread betting)", 2, "fx_spot"),
    ("fx_spot_ecn", "ECN / electronic matching", 2, "fx_spot"),
    ("fx_deriv_fwd", "FX forwards and NDFs", 2, "fx_deriv"),
    ("fx_deriv_swap", "FX swaps (short-dated, long-dated)", 2, "fx_deriv"),
    ("fx_deriv_opt", "FX options (vanilla, barrier, digital)", 2, "fx_deriv"),
    ("fx_deriv_ccs", "Cross-currency swaps", 2, "fx_deriv"),
    ("fx_pair_major", "Major pairs (EUR/USD, GBP/USD, USD/JPY)", 2, "fx_pair"),
    ("fx_pair_minor", "Minor / cross pairs (EUR/GBP, AUD/NZD)", 2, "fx_pair"),
    ("fx_pair_exot", "Exotic pairs (USD/TRY, USD/ZAR)", 2, "fx_pair"),
    ("fx_pair_em", "Emerging market pairs", 2, "fx_pair"),
    ("fx_pair_crypto", "Crypto-fiat pairs", 2, "fx_pair"),
]


async def ingest_domain_forex(conn) -> int:
    """Insert or update Forex Instrument Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_forex"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_forex", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_forex",
    )
    return count
