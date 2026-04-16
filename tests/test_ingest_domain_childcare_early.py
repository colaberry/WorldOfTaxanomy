"""Tests for Childcare and Early Education domain taxonomy ingester.

RED tests - written before any implementation exists.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_childcare_early import (
    CHILDCARE_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_childcare_early,
)


class TestDetermineLevel:
    def test_center_category_is_level_1(self):
        assert _determine_level("dce_center") == 1

    def test_daycare_is_level_2(self):
        assert _determine_level("dce_center_daycare") == 2

    def test_family_category_is_level_1(self):
        assert _determine_level("dce_family") == 1


class TestDetermineParent:
    def test_center_has_no_parent(self):
        assert _determine_parent("dce_center") is None

    def test_daycare_parent_is_center(self):
        assert _determine_parent("dce_center_daycare") == "dce_center"

    def test_home_parent_is_family(self):
        assert _determine_parent("dce_family_home") == "dce_family"


class TestNodes:
    def test_nodes_non_empty(self):
        assert len(CHILDCARE_NODES) > 0

    def test_has_center_category(self):
        codes = [n[0] for n in CHILDCARE_NODES]
        assert "dce_center" in codes

    def test_has_family_category(self):
        codes = [n[0] for n in CHILDCARE_NODES]
        assert "dce_family" in codes

    def test_has_headstart_category(self):
        codes = [n[0] for n in CHILDCARE_NODES]
        assert "dce_headstart" in codes

    def test_has_daycare_node(self):
        codes = [n[0] for n in CHILDCARE_NODES]
        assert "dce_center_daycare" in codes

    def test_has_home_node(self):
        codes = [n[0] for n in CHILDCARE_NODES]
        assert "dce_family_home" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in CHILDCARE_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in CHILDCARE_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in CHILDCARE_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in CHILDCARE_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(CHILDCARE_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in CHILDCARE_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_module_importable():
    assert callable(ingest_domain_childcare_early)
    assert isinstance(CHILDCARE_NODES, list)


def test_ingest(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_childcare_early(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_childcare_early'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_childcare_early(conn)
            count2 = await ingest_domain_childcare_early(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
