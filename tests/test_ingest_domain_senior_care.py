"""Tests for Senior Care and Elder Services domain taxonomy ingester.

RED tests - written before any implementation exists.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_senior_care import (
    SENIOR_CARE_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_senior_care,
)


class TestDetermineLevel:
    def test_assisted_category_is_level_1(self):
        assert _determine_level("dsc_assisted") == 1

    def test_memory_is_level_2(self):
        assert _determine_level("dsc_assisted_memory") == 2

    def test_nursing_category_is_level_1(self):
        assert _determine_level("dsc_nursing") == 1


class TestDetermineParent:
    def test_assisted_has_no_parent(self):
        assert _determine_parent("dsc_assisted") is None

    def test_memory_parent_is_assisted(self):
        assert _determine_parent("dsc_assisted_memory") == "dsc_assisted"

    def test_snf_parent_is_nursing(self):
        assert _determine_parent("dsc_nursing_snf") == "dsc_nursing"


class TestNodes:
    def test_nodes_non_empty(self):
        assert len(SENIOR_CARE_NODES) > 0

    def test_has_assisted_category(self):
        codes = [n[0] for n in SENIOR_CARE_NODES]
        assert "dsc_assisted" in codes

    def test_has_nursing_category(self):
        codes = [n[0] for n in SENIOR_CARE_NODES]
        assert "dsc_nursing" in codes

    def test_has_home_category(self):
        codes = [n[0] for n in SENIOR_CARE_NODES]
        assert "dsc_home" in codes

    def test_has_memory_node(self):
        codes = [n[0] for n in SENIOR_CARE_NODES]
        assert "dsc_assisted_memory" in codes

    def test_has_snf_node(self):
        codes = [n[0] for n in SENIOR_CARE_NODES]
        assert "dsc_nursing_snf" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in SENIOR_CARE_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in SENIOR_CARE_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in SENIOR_CARE_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in SENIOR_CARE_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(SENIOR_CARE_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in SENIOR_CARE_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_module_importable():
    assert callable(ingest_domain_senior_care)
    assert isinstance(SENIOR_CARE_NODES, list)


def test_ingest(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_senior_care(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_senior_care'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_senior_care(conn)
            count2 = await ingest_domain_senior_care(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
