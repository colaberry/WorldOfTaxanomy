"""Tests for FinTech Services domain taxonomy ingester.

RED tests - written before any implementation exists.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_fintech_service import (
    FINTECH_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_fintech_service,
)


class TestDetermineLevel:
    def test_payments_category_is_level_1(self):
        assert _determine_level("dft_payments") == 1

    def test_wallet_is_level_2(self):
        assert _determine_level("dft_payments_wallet") == 2

    def test_neobank_category_is_level_1(self):
        assert _determine_level("dft_neobank") == 1


class TestDetermineParent:
    def test_payments_has_no_parent(self):
        assert _determine_parent("dft_payments") is None

    def test_wallet_parent_is_payments(self):
        assert _determine_parent("dft_payments_wallet") == "dft_payments"

    def test_consumer_parent_is_neobank(self):
        assert _determine_parent("dft_neobank_consumer") == "dft_neobank"


class TestNodes:
    def test_nodes_non_empty(self):
        assert len(FINTECH_NODES) > 0

    def test_has_payments_category(self):
        codes = [n[0] for n in FINTECH_NODES]
        assert "dft_payments" in codes

    def test_has_neobank_category(self):
        codes = [n[0] for n in FINTECH_NODES]
        assert "dft_neobank" in codes

    def test_has_lending_category(self):
        codes = [n[0] for n in FINTECH_NODES]
        assert "dft_lending" in codes

    def test_has_wallet_node(self):
        codes = [n[0] for n in FINTECH_NODES]
        assert "dft_payments_wallet" in codes

    def test_has_consumer_node(self):
        codes = [n[0] for n in FINTECH_NODES]
        assert "dft_neobank_consumer" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in FINTECH_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in FINTECH_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in FINTECH_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in FINTECH_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(FINTECH_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in FINTECH_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_module_importable():
    assert callable(ingest_domain_fintech_service)
    assert isinstance(FINTECH_NODES, list)


def test_ingest(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_fintech_service(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_fintech_service'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_fintech_service(conn)
            count2 = await ingest_domain_fintech_service(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
