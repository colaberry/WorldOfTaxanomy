"""Tests for Finance Instrument Type domain taxonomy ingester.

RED tests - written before any implementation exists.

Financial instrument taxonomy organizes securities and financial products:
  Equity        (dfi_equity*)  - common stock, preferred, ETF, ADR
  Fixed Income  (dfi_fixed*)   - government bonds, corporate bonds, MBS, muni
  Derivatives   (dfi_deriv*)   - options, futures, swaps, forwards
  Alternative   (dfi_alt*)     - real estate, commodities, private equity, hedge funds
  Cash/Money    (dfi_cash*)    - deposits, money market, CDs, commercial paper

Source: FIGI (Financial Instrument Global Identifier) asset class framework. Open standard.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.domain_finance_instrument import (
    FINANCE_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_finance_instrument,
)


class TestDetermineLevel:
    def test_equity_category_is_level_1(self):
        assert _determine_level("dfi_equity") == 1

    def test_common_stock_is_level_2(self):
        assert _determine_level("dfi_equity_common") == 2

    def test_fixed_category_is_level_1(self):
        assert _determine_level("dfi_fixed") == 1

    def test_govt_bond_is_level_2(self):
        assert _determine_level("dfi_fixed_govt") == 2


class TestDetermineParent:
    def test_equity_category_has_no_parent(self):
        assert _determine_parent("dfi_equity") is None

    def test_common_stock_parent_is_equity(self):
        assert _determine_parent("dfi_equity_common") == "dfi_equity"

    def test_govt_bond_parent_is_fixed(self):
        assert _determine_parent("dfi_fixed_govt") == "dfi_fixed"


class TestFinanceNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(FINANCE_NODES) > 0

    def test_has_equity_category(self):
        codes = [n[0] for n in FINANCE_NODES]
        assert "dfi_equity" in codes

    def test_has_fixed_income_category(self):
        codes = [n[0] for n in FINANCE_NODES]
        assert "dfi_fixed" in codes

    def test_has_derivatives_category(self):
        codes = [n[0] for n in FINANCE_NODES]
        assert "dfi_deriv" in codes

    def test_has_common_stock(self):
        codes = [n[0] for n in FINANCE_NODES]
        assert "dfi_equity_common" in codes

    def test_has_govt_bond(self):
        codes = [n[0] for n in FINANCE_NODES]
        assert "dfi_fixed_govt" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in FINANCE_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in FINANCE_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in FINANCE_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in FINANCE_NODES:
            if level == 2:
                assert parent is not None


def test_domain_finance_instrument_module_importable():
    assert callable(ingest_domain_finance_instrument)
    assert isinstance(FINANCE_NODES, list)


def test_ingest_domain_finance_instrument(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_finance_instrument(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_finance_instrument'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_finance_instrument_idempotent(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_finance_instrument(conn)
            count2 = await ingest_domain_finance_instrument(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
