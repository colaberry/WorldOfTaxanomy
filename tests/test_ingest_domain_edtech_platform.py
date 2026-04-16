"""Tests for EdTech Platform domain taxonomy ingester.

RED tests - written before any implementation exists.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_edtech_platform import (
    EDTECH_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_edtech_platform,
)


class TestDetermineLevel:
    def test_k12_category_is_level_1(self):
        assert _determine_level("det_k12") == 1

    def test_lms_is_level_2(self):
        assert _determine_level("det_k12_lms") == 2

    def test_higher_category_is_level_1(self):
        assert _determine_level("det_higher") == 1


class TestDetermineParent:
    def test_k12_has_no_parent(self):
        assert _determine_parent("det_k12") is None

    def test_lms_parent_is_k12(self):
        assert _determine_parent("det_k12_lms") == "det_k12"

    def test_opm_parent_is_higher(self):
        assert _determine_parent("det_higher_opm") == "det_higher"


class TestNodes:
    def test_nodes_non_empty(self):
        assert len(EDTECH_NODES) > 0

    def test_has_k12_category(self):
        codes = [n[0] for n in EDTECH_NODES]
        assert "det_k12" in codes

    def test_has_higher_category(self):
        codes = [n[0] for n in EDTECH_NODES]
        assert "det_higher" in codes

    def test_has_corp_category(self):
        codes = [n[0] for n in EDTECH_NODES]
        assert "det_corp" in codes

    def test_has_lms_node(self):
        codes = [n[0] for n in EDTECH_NODES]
        assert "det_k12_lms" in codes

    def test_has_opm_node(self):
        codes = [n[0] for n in EDTECH_NODES]
        assert "det_higher_opm" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in EDTECH_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in EDTECH_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in EDTECH_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in EDTECH_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(EDTECH_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in EDTECH_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_module_importable():
    assert callable(ingest_domain_edtech_platform)
    assert isinstance(EDTECH_NODES, list)


def test_ingest(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_edtech_platform(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_edtech_platform'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_edtech_platform(conn)
            count2 = await ingest_domain_edtech_platform(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
