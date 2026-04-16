"""Tests for Fishing and Aquaculture domain taxonomy ingester.

RED tests - written before any implementation exists.

Fishing and aquaculture taxonomy organizes types into categories:
  Marine Fishing     (dfa_marine*)   - pelagic, demersal, shellfish, artisanal
  Freshwater Fishing (dfa_fresh*)    - river, lake, recreational
  Aquaculture Farm   (dfa_aqua*)     - finfish, shellfish, seaweed, offshore, RAS
  Seafood Processing (dfa_proc*)     - fillet, frozen, canned, value-added
  Fish Feed/Hatchery (dfa_feed*)     - pellets, hatchery, biotech

Source: NAICS 1141 + 1142 fishing industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_fishing_aqua import (
    FISHING_AQUA_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_fishing_aqua,
)


class TestDetermineLevel:
    def test_marine_category_is_level_1(self):
        assert _determine_level("dfa_marine") == 1

    def test_marine_type_is_level_2(self):
        assert _determine_level("dfa_marine_pelagic") == 2

    def test_aqua_category_is_level_1(self):
        assert _determine_level("dfa_aqua") == 1

    def test_aqua_type_is_level_2(self):
        assert _determine_level("dfa_aqua_finfish") == 2

    def test_feed_category_is_level_1(self):
        assert _determine_level("dfa_feed") == 1

    def test_feed_type_is_level_2(self):
        assert _determine_level("dfa_feed_pellet") == 2


class TestDetermineParent:
    def test_marine_category_has_no_parent(self):
        assert _determine_parent("dfa_marine") is None

    def test_marine_pelagic_parent_is_marine(self):
        assert _determine_parent("dfa_marine_pelagic") == "dfa_marine"

    def test_aqua_finfish_parent_is_aqua(self):
        assert _determine_parent("dfa_aqua_finfish") == "dfa_aqua"

    def test_proc_frozen_parent_is_proc(self):
        assert _determine_parent("dfa_proc_frozen") == "dfa_proc"


class TestFishingAquaNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(FISHING_AQUA_NODES) > 0

    def test_has_marine_fishing_category(self):
        codes = [n[0] for n in FISHING_AQUA_NODES]
        assert "dfa_marine" in codes

    def test_has_aquaculture_category(self):
        codes = [n[0] for n in FISHING_AQUA_NODES]
        assert "dfa_aqua" in codes

    def test_has_seafood_processing_category(self):
        codes = [n[0] for n in FISHING_AQUA_NODES]
        assert "dfa_proc" in codes

    def test_has_freshwater_category(self):
        codes = [n[0] for n in FISHING_AQUA_NODES]
        assert "dfa_fresh" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in FISHING_AQUA_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in FISHING_AQUA_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in FISHING_AQUA_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in FISHING_AQUA_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(FISHING_AQUA_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in FISHING_AQUA_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_fishing_aqua_module_importable():
    assert callable(ingest_domain_fishing_aqua)
    assert isinstance(FISHING_AQUA_NODES, list)


def test_ingest_domain_fishing_aqua(db_pool):
    """Integration test: fishing/aquaculture taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_fishing_aqua(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_fishing_aqua'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_fishing_aqua'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_fishing_aqua_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_fishing_aqua(conn)
            count2 = await ingest_domain_fishing_aqua(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
