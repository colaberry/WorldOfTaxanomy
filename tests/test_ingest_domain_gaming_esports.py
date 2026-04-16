"""Tests for Gaming and Esports domain taxonomy ingester.

Gaming and Esports taxonomy organizes gaming and esports sector types:
  PC Gaming          (dge_pc*)       - MMO, FPS, strategy, simulation
  Console Gaming     (dge_console*)  - action, sports, RPG, exclusives
  Mobile Gaming      (dge_mobile*)   - casual, mid-core, social
  Esports Competitive (dge_esport*) - leagues, teams, betting
  Game Development   (dge_dev*)      - engines, studios, indie
  Streaming/Content  (dge_stream*)   - live streaming, video, merch

Source: NAICS 7112 gaming/esports industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_gaming_esports import (
    NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_gaming_esports,
)


class TestDetermineLevel:
    def test_pc_category_is_level_1(self):
        assert _determine_level("dge_pc") == 1

    def test_pc_type_is_level_2(self):
        assert _determine_level("dge_pc_mmo") == 2

    def test_console_category_is_level_1(self):
        assert _determine_level("dge_console") == 1

    def test_console_type_is_level_2(self):
        assert _determine_level("dge_console_action") == 2

    def test_esport_category_is_level_1(self):
        assert _determine_level("dge_esport") == 1

    def test_esport_type_is_level_2(self):
        assert _determine_level("dge_esport_league") == 2


class TestDetermineParent:
    def test_pc_category_has_no_parent(self):
        assert _determine_parent("dge_pc") is None

    def test_pc_mmo_parent_is_pc(self):
        assert _determine_parent("dge_pc_mmo") == "dge_pc"

    def test_console_action_parent_is_console(self):
        assert _determine_parent("dge_console_action") == "dge_console"

    def test_esport_league_parent_is_esport(self):
        assert _determine_parent("dge_esport_league") == "dge_esport"


class TestNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(NODES) > 0

    def test_minimum_node_count(self):
        assert len(NODES) >= 20

    def test_has_pc_category(self):
        codes = [n[0] for n in NODES]
        assert "dge_pc" in codes

    def test_has_console_category(self):
        codes = [n[0] for n in NODES]
        assert "dge_console" in codes

    def test_has_esport_category(self):
        codes = [n[0] for n in NODES]
        assert "dge_esport" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in NODES:
            if level == 2:
                assert parent is not None

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_gaming_esports_module_importable():
    assert callable(ingest_domain_gaming_esports)
    assert isinstance(NODES, list)


def test_ingest_domain_gaming_esports(db_pool):
    """Integration test: gaming/esports taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_gaming_esports(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_gaming_esports'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_gaming_esports_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_gaming_esports(conn)
            count2 = await ingest_domain_gaming_esports(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
