"""Tests for Wine and Spirits domain taxonomy ingester.

RED tests - written before any implementation exists.

Wine and spirits taxonomy organizes types into categories:
  Viticulture        (dws_viti*)     - vineyard, nursery, organic, harvest
  Wine Production    (dws_wine*)     - still, sparkling, fortified, natural
  Distilled Spirits  (dws_spirit*)   - whiskey, vodka, rum, gin, tequila, brandy
  Craft Brewing      (dws_brew*)     - ale, cider, seltzer
  Beverage Distrib.  (dws_dist*)     - wholesale, import, DTC, e-commerce

Source: NAICS 3121 beverage manufacturing. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_wine_spirits import (
    WINE_SPIRITS_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_wine_spirits,
)


class TestDetermineLevel:
    def test_viti_category_is_level_1(self):
        assert _determine_level("dws_viti") == 1

    def test_viti_type_is_level_2(self):
        assert _determine_level("dws_viti_vineyard") == 2

    def test_spirit_category_is_level_1(self):
        assert _determine_level("dws_spirit") == 1

    def test_spirit_type_is_level_2(self):
        assert _determine_level("dws_spirit_whiskey") == 2

    def test_brew_category_is_level_1(self):
        assert _determine_level("dws_brew") == 1

    def test_brew_type_is_level_2(self):
        assert _determine_level("dws_brew_ale") == 2


class TestDetermineParent:
    def test_viti_category_has_no_parent(self):
        assert _determine_parent("dws_viti") is None

    def test_viti_vineyard_parent_is_viti(self):
        assert _determine_parent("dws_viti_vineyard") == "dws_viti"

    def test_spirit_whiskey_parent_is_spirit(self):
        assert _determine_parent("dws_spirit_whiskey") == "dws_spirit"

    def test_dist_wholesale_parent_is_dist(self):
        assert _determine_parent("dws_dist_wholesale") == "dws_dist"


class TestWineSpiritsNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(WINE_SPIRITS_NODES) > 0

    def test_has_viticulture_category(self):
        codes = [n[0] for n in WINE_SPIRITS_NODES]
        assert "dws_viti" in codes

    def test_has_wine_production_category(self):
        codes = [n[0] for n in WINE_SPIRITS_NODES]
        assert "dws_wine" in codes

    def test_has_distilled_spirits_category(self):
        codes = [n[0] for n in WINE_SPIRITS_NODES]
        assert "dws_spirit" in codes

    def test_has_craft_brewing_category(self):
        codes = [n[0] for n in WINE_SPIRITS_NODES]
        assert "dws_brew" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in WINE_SPIRITS_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in WINE_SPIRITS_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in WINE_SPIRITS_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in WINE_SPIRITS_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(WINE_SPIRITS_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in WINE_SPIRITS_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_wine_spirits_module_importable():
    assert callable(ingest_domain_wine_spirits)
    assert isinstance(WINE_SPIRITS_NODES, list)


def test_ingest_domain_wine_spirits(db_pool):
    """Integration test: wine/spirits taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_wine_spirits(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_wine_spirits'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_wine_spirits'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_wine_spirits_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_wine_spirits(conn)
            count2 = await ingest_domain_wine_spirits(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
