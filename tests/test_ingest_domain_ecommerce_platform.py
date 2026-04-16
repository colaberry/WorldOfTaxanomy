"""Tests for E-Commerce Platform domain taxonomy ingester.

RED tests - written before any implementation exists.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_ecommerce_platform import (
    ECOMMERCE_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_ecommerce_platform,
)


class TestDetermineLevel:
    def test_b2c_category_is_level_1(self):
        assert _determine_level("dep_b2c") == 1

    def test_general_is_level_2(self):
        assert _determine_level("dep_b2c_general") == 2

    def test_b2b_category_is_level_1(self):
        assert _determine_level("dep_b2b") == 1


class TestDetermineParent:
    def test_b2c_has_no_parent(self):
        assert _determine_parent("dep_b2c") is None

    def test_general_parent_is_b2c(self):
        assert _determine_parent("dep_b2c_general") == "dep_b2c"

    def test_wholesale_parent_is_b2b(self):
        assert _determine_parent("dep_b2b_wholesale") == "dep_b2b"


class TestNodes:
    def test_nodes_non_empty(self):
        assert len(ECOMMERCE_NODES) > 0

    def test_has_b2c_category(self):
        codes = [n[0] for n in ECOMMERCE_NODES]
        assert "dep_b2c" in codes

    def test_has_b2b_category(self):
        codes = [n[0] for n in ECOMMERCE_NODES]
        assert "dep_b2b" in codes

    def test_has_d2c_category(self):
        codes = [n[0] for n in ECOMMERCE_NODES]
        assert "dep_d2c" in codes

    def test_has_general_node(self):
        codes = [n[0] for n in ECOMMERCE_NODES]
        assert "dep_b2c_general" in codes

    def test_has_wholesale_node(self):
        codes = [n[0] for n in ECOMMERCE_NODES]
        assert "dep_b2b_wholesale" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in ECOMMERCE_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in ECOMMERCE_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in ECOMMERCE_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in ECOMMERCE_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(ECOMMERCE_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in ECOMMERCE_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_module_importable():
    assert callable(ingest_domain_ecommerce_platform)
    assert isinstance(ECOMMERCE_NODES, list)


def test_ingest(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_ecommerce_platform(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_ecommerce_platform'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_ecommerce_platform(conn)
            count2 = await ingest_domain_ecommerce_platform(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
