"""Finance Instrument Type domain taxonomy ingester.

Financial instrument taxonomy organizes securities and financial products:
  Equity        (dfi_equity*) - common stock, preferred, ETF, ADR
  Fixed Income  (dfi_fixed*)  - government bonds, corporate bonds, MBS, municipal
  Derivatives   (dfi_deriv*)  - options, futures, swaps, forwards
  Alternative   (dfi_alt*)    - real estate, commodities, private equity, hedge funds
  Cash/Money    (dfi_cash*)   - deposits, money market, CDs, commercial paper

Source: FIGI (Financial Instrument Global Identifier) asset class framework. Open standard.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
FINANCE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Equity category --
    ("dfi_equity",          "Equity Securities",                              1, None),
    ("dfi_equity_common",   "Common Stock (ordinary shares)",                2, "dfi_equity"),
    ("dfi_equity_pref",     "Preferred Stock (preference shares)",           2, "dfi_equity"),
    ("dfi_equity_etf",      "Exchange-Traded Funds (ETF, ETP)",              2, "dfi_equity"),
    ("dfi_equity_adr",      "ADRs and GDRs (depositary receipts)",           2, "dfi_equity"),

    # -- Fixed Income category --
    ("dfi_fixed",         "Fixed Income Securities",                          1, None),
    ("dfi_fixed_govt",    "Government Bonds (Treasury, sovereign)",          2, "dfi_fixed"),
    ("dfi_fixed_corp",    "Corporate Bonds (investment grade, high yield)",  2, "dfi_fixed"),
    ("dfi_fixed_mbs",     "Mortgage-Backed Securities (MBS, ABS, CMBS)",    2, "dfi_fixed"),
    ("dfi_fixed_muni",    "Municipal Bonds (tax-exempt, revenue, GO)",       2, "dfi_fixed"),
    ("dfi_fixed_mm",      "Money Market Securities (T-bills, CP, repos)",    2, "dfi_fixed"),

    # -- Derivatives category --
    ("dfi_deriv",         "Derivatives",                                      1, None),
    ("dfi_deriv_option",  "Options (equity, index, currency, commodity)",    2, "dfi_deriv"),
    ("dfi_deriv_future",  "Futures (commodity, financial, index)",           2, "dfi_deriv"),
    ("dfi_deriv_swap",    "Swaps (interest rate, currency, credit default)", 2, "dfi_deriv"),
    ("dfi_deriv_fwd",     "Forwards (FX forwards, commodity forwards)",      2, "dfi_deriv"),

    # -- Alternative Investments category --
    ("dfi_alt",          "Alternative Investments",                           1, None),
    ("dfi_alt_re",       "Real Estate (REIT, direct, private)",              2, "dfi_alt"),
    ("dfi_alt_comm",     "Commodities (physical, futures-based)",            2, "dfi_alt"),
    ("dfi_alt_pe",       "Private Equity (buyout, venture, growth)",         2, "dfi_alt"),
    ("dfi_alt_hf",       "Hedge Funds (multi-strategy, macro, quant)",       2, "dfi_alt"),

    # -- Cash and Cash Equivalents category --
    ("dfi_cash",         "Cash and Cash Equivalents",                         1, None),
    ("dfi_cash_deposit", "Bank Deposits (savings, checking, sweep)",         2, "dfi_cash"),
    ("dfi_cash_mmf",     "Money Market Funds",                                2, "dfi_cash"),
    ("dfi_cash_cd",      "Certificates of Deposit (CDs)",                   2, "dfi_cash"),
]

_DOMAIN_ROW = (
    "domain_finance_instrument",
    "Finance Instrument Types",
    "Equity, fixed income, derivatives, alternatives and cash instrument taxonomy for financial services",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["52"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific instrument types."""
    parts = code.split("_")
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_finance_instrument(conn) -> int:
    """Ingest Finance Instrument Type domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_finance_instrument'), and links NAICS 52xxx nodes
    via node_taxonomy_link.

    Returns total instrument node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_finance_instrument",
        "Finance Instrument Types",
        "Equity, fixed income, derivatives, alternatives and cash instrument taxonomy",
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

    parent_codes = {parent for _, _, _, parent in FINANCE_NODES if parent is not None}

    rows = [
        (
            "domain_finance_instrument",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in FINANCE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(FINANCE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_finance_instrument'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_finance_instrument'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '52%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_finance_instrument", "primary") for code in naics_codes],
    )

    return count
