"""Tests for Climate Technology domain taxonomy ingester.

RED tests - written before any implementation exists.

Climate technology taxonomy organizes climate tech sector types:
  Solar Energy      (dct_solar*)   - PV, CSP, BIPV, floating
  Wind Energy       (dct_wind*)    - onshore, offshore, small wind
  Green Hydrogen    (dct_h2*)      - electrolysis, storage, distribution
  Carbon Capture    (dct_ccs*)     - CCUS, DAC, ocean capture
  Carbon Markets    (dct_carbon*)  - offsets, compliance markets, MRV
  Electric Vehicles (dct_ev*)      - passenger, commercial, charging
  Grid Modernization (dct_grid*)   - smart grid, BESS, demand response
  Building Efficiency (dct_bldg*)  - insulation, HVAC, smart buildings

Source: NAICS 2211 + 3353 climate tech industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.domain_climate_tech import (
    CLIMATE_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_climate_tech,
)


class TestDetermineLevel:
    def test_solar_category_is_level_1(self):
        assert _determine_level("dct_solar") == 1

    def test_solar_type_is_level_2(self):
        assert _determine_level("dct_solar_pv") == 2

    def test_wind_category_is_level_1(self):
        assert _determine_level("dct_wind") == 1

    def test_wind_type_is_level_2(self):
        assert _determine_level("dct_wind_onshore") == 2

    def test_h2_category_is_level_1(self):
        assert _determine_level("dct_h2") == 1

    def test_h2_type_is_level_2(self):
        assert _determine_level("dct_h2_electro") == 2


class TestDetermineParent:
    def test_solar_category_has_no_parent(self):
        assert _determine_parent("dct_solar") is None

    def test_solar_pv_parent_is_solar(self):
        assert _determine_parent("dct_solar_pv") == "dct_solar"

    def test_wind_onshore_parent_is_wind(self):
        assert _determine_parent("dct_wind_onshore") == "dct_wind"

    def test_h2_electro_parent_is_h2(self):
        assert _determine_parent("dct_h2_electro") == "dct_h2"


class TestClimateNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(CLIMATE_NODES) > 0

    def test_has_solar_category(self):
        codes = [n[0] for n in CLIMATE_NODES]
        assert "dct_solar" in codes

    def test_has_wind_category(self):
        codes = [n[0] for n in CLIMATE_NODES]
        assert "dct_wind" in codes

    def test_has_carbon_capture_category(self):
        codes = [n[0] for n in CLIMATE_NODES]
        assert "dct_ccs" in codes

    def test_has_electric_vehicles_category(self):
        codes = [n[0] for n in CLIMATE_NODES]
        assert "dct_ev" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in CLIMATE_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in CLIMATE_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in CLIMATE_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in CLIMATE_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(CLIMATE_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in CLIMATE_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_climate_tech_module_importable():
    assert callable(ingest_domain_climate_tech)
    assert isinstance(CLIMATE_NODES, list)


def test_ingest_domain_climate_tech(db_pool):
    """Integration test: climate tech taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_climate_tech(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_climate_tech'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_climate_tech'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_climate_tech_idempotent(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_climate_tech(conn)
            count2 = await ingest_domain_climate_tech(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
