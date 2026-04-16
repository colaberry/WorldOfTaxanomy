"""Tests for CleanTech domain taxonomy ingester.

RED tests - written before any implementation exists.

CleanTech taxonomy organizes clean technology sector types:
  Carbon Capture           (dcl_carbon*)    - DAC, point-source, storage, CCU
  Water Purification Tech  (dcl_water*)     - membrane, desalination, UV, reuse
  Circular Economy         (dcl_circular*)  - recycling, remanufacturing, sharing
  Green Chemistry          (dcl_greenchem*) - bio-based, catalysis, biodegradable
  Environmental Monitoring (dcl_envmon*)    - air, water, satellite, biodiversity

Source: NAICS 5629 + 3339 waste/machinery industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_cleantech import (
    CLEANTECH_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_cleantech,
)


class TestDetermineLevel:
    def test_carbon_category_is_level_1(self):
        assert _determine_level("dcl_carbon") == 1

    def test_carbon_type_is_level_2(self):
        assert _determine_level("dcl_carbon_dac") == 2

    def test_water_category_is_level_1(self):
        assert _determine_level("dcl_water") == 1

    def test_water_type_is_level_2(self):
        assert _determine_level("dcl_water_membrane") == 2

    def test_circular_category_is_level_1(self):
        assert _determine_level("dcl_circular") == 1

    def test_circular_type_is_level_2(self):
        assert _determine_level("dcl_circular_recycle") == 2


class TestDetermineParent:
    def test_carbon_category_has_no_parent(self):
        assert _determine_parent("dcl_carbon") is None

    def test_carbon_dac_parent_is_carbon(self):
        assert _determine_parent("dcl_carbon_dac") == "dcl_carbon"

    def test_water_membrane_parent_is_water(self):
        assert _determine_parent("dcl_water_membrane") == "dcl_water"

    def test_envmon_air_parent_is_envmon(self):
        assert _determine_parent("dcl_envmon_air") == "dcl_envmon"


class TestCleanTechNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(CLEANTECH_NODES) > 0

    def test_has_carbon_category(self):
        codes = [n[0] for n in CLEANTECH_NODES]
        assert "dcl_carbon" in codes

    def test_has_water_category(self):
        codes = [n[0] for n in CLEANTECH_NODES]
        assert "dcl_water" in codes

    def test_has_circular_category(self):
        codes = [n[0] for n in CLEANTECH_NODES]
        assert "dcl_circular" in codes

    def test_has_greenchem_category(self):
        codes = [n[0] for n in CLEANTECH_NODES]
        assert "dcl_greenchem" in codes

    def test_has_envmon_category(self):
        codes = [n[0] for n in CLEANTECH_NODES]
        assert "dcl_envmon" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in CLEANTECH_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in CLEANTECH_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in CLEANTECH_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in CLEANTECH_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(CLEANTECH_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in CLEANTECH_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_cleantech_module_importable():
    assert callable(ingest_domain_cleantech)
    assert isinstance(CLEANTECH_NODES, list)


def test_ingest_domain_cleantech(db_pool):
    """Integration test: CleanTech taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_cleantech(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_cleantech'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_cleantech'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_cleantech_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_cleantech(conn)
            count2 = await ingest_domain_cleantech(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
