"""Tests for Advertising and Marketing Services domain taxonomy ingester.

RED tests - written before any implementation exists.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_advertising_mktg import (
    ADVERTISING_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_advertising_mktg,
)


class TestDetermineLevel:
    def test_digital_category_is_level_1(self):
        assert _determine_level("dam_digital") == 1

    def test_search_is_level_2(self):
        assert _determine_level("dam_digital_search") == 2

    def test_trad_category_is_level_1(self):
        assert _determine_level("dam_trad") == 1


class TestDetermineParent:
    def test_digital_has_no_parent(self):
        assert _determine_parent("dam_digital") is None

    def test_search_parent_is_digital(self):
        assert _determine_parent("dam_digital_search") == "dam_digital"

    def test_tv_parent_is_trad(self):
        assert _determine_parent("dam_trad_tv") == "dam_trad"


class TestNodes:
    def test_nodes_non_empty(self):
        assert len(ADVERTISING_NODES) > 0

    def test_has_digital_category(self):
        codes = [n[0] for n in ADVERTISING_NODES]
        assert "dam_digital" in codes

    def test_has_trad_category(self):
        codes = [n[0] for n in ADVERTISING_NODES]
        assert "dam_trad" in codes

    def test_has_content_category(self):
        codes = [n[0] for n in ADVERTISING_NODES]
        assert "dam_content" in codes

    def test_has_search_node(self):
        codes = [n[0] for n in ADVERTISING_NODES]
        assert "dam_digital_search" in codes

    def test_has_tv_node(self):
        codes = [n[0] for n in ADVERTISING_NODES]
        assert "dam_trad_tv" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in ADVERTISING_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in ADVERTISING_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in ADVERTISING_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in ADVERTISING_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(ADVERTISING_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in ADVERTISING_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_module_importable():
    assert callable(ingest_domain_advertising_mktg)
    assert isinstance(ADVERTISING_NODES, list)


def test_ingest(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_advertising_mktg(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_advertising_mktg'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_advertising_mktg(conn)
            count2 = await ingest_domain_advertising_mktg(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
