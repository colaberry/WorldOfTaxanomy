"""Tests for Hydrogen Economy domain taxonomy ingester.

RED tests - written before any implementation exists.

Hydrogen economy taxonomy organizes types into categories:
  Green Hydrogen     (dhe_green*)    - electrolysis, solar, wind, biomass
  Blue Hydrogen      (dhe_blue*)     - SMR, ATR, carbon capture, grey
  Fuel Cells         (dhe_cell*)     - PEM, SOFC, MCFC, stack mfg
  Hydrogen Storage   (dhe_store*)    - compressed, liquid, metal hydride, underground
  Hydrogen Transport (dhe_trans*)    - pipeline, blending, truck, ammonia, station

Source: NAICS 2211 + 3259 hydrogen/energy industry. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_hydrogen_economy import (
    HYDROGEN_ECONOMY_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_hydrogen_economy,
)


class TestDetermineLevel:
    def test_green_category_is_level_1(self):
        assert _determine_level("dhe_green") == 1

    def test_green_type_is_level_2(self):
        assert _determine_level("dhe_green_electro") == 2

    def test_cell_category_is_level_1(self):
        assert _determine_level("dhe_cell") == 1

    def test_cell_type_is_level_2(self):
        assert _determine_level("dhe_cell_pem") == 2

    def test_store_category_is_level_1(self):
        assert _determine_level("dhe_store") == 1

    def test_store_type_is_level_2(self):
        assert _determine_level("dhe_store_compress") == 2


class TestDetermineParent:
    def test_green_category_has_no_parent(self):
        assert _determine_parent("dhe_green") is None

    def test_green_electro_parent_is_green(self):
        assert _determine_parent("dhe_green_electro") == "dhe_green"

    def test_cell_pem_parent_is_cell(self):
        assert _determine_parent("dhe_cell_pem") == "dhe_cell"

    def test_trans_pipeline_parent_is_trans(self):
        assert _determine_parent("dhe_trans_pipeline") == "dhe_trans"


class TestHydrogenEconomyNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(HYDROGEN_ECONOMY_NODES) > 0

    def test_has_green_hydrogen_category(self):
        codes = [n[0] for n in HYDROGEN_ECONOMY_NODES]
        assert "dhe_green" in codes

    def test_has_blue_hydrogen_category(self):
        codes = [n[0] for n in HYDROGEN_ECONOMY_NODES]
        assert "dhe_blue" in codes

    def test_has_fuel_cells_category(self):
        codes = [n[0] for n in HYDROGEN_ECONOMY_NODES]
        assert "dhe_cell" in codes

    def test_has_storage_category(self):
        codes = [n[0] for n in HYDROGEN_ECONOMY_NODES]
        assert "dhe_store" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in HYDROGEN_ECONOMY_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in HYDROGEN_ECONOMY_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in HYDROGEN_ECONOMY_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in HYDROGEN_ECONOMY_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(HYDROGEN_ECONOMY_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in HYDROGEN_ECONOMY_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_hydrogen_economy_module_importable():
    assert callable(ingest_domain_hydrogen_economy)
    assert isinstance(HYDROGEN_ECONOMY_NODES, list)


def test_ingest_domain_hydrogen_economy(db_pool):
    """Integration test: hydrogen economy taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_hydrogen_economy(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_hydrogen_economy'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_hydrogen_economy'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_hydrogen_economy_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_hydrogen_economy(conn)
            count2 = await ingest_domain_hydrogen_economy(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
