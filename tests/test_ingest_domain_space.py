"""Tests for Space and Satellite Economy domain taxonomy ingester.

RED tests - written before any implementation exists.

Space taxonomy organizes space and satellite economy sector types:
  Launch Vehicles   (dsp_launch*)  - small, medium, heavy, super-heavy lift
  Satellite Types   (dsp_sat*)     - communications, Earth observation, navigation, science
  In-Orbit Services (dsp_orbit*)   - servicing, refueling, debris removal
  Ground Segment    (dsp_ground*)  - control, tracking, TT&C
  Downstream Apps   (dsp_down*)    - imagery analytics, connectivity, position services
  Space Tourism     (dsp_tour*)    - suborbital, orbital, habitat stays

Source: NAICS 336414 + 517 space industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_space import (
    SPACE_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_space,
)


class TestDetermineLevel:
    def test_launch_category_is_level_1(self):
        assert _determine_level("dsp_launch") == 1

    def test_launch_type_is_level_2(self):
        assert _determine_level("dsp_launch_small") == 2

    def test_sat_category_is_level_1(self):
        assert _determine_level("dsp_sat") == 1

    def test_sat_type_is_level_2(self):
        assert _determine_level("dsp_sat_comms") == 2

    def test_down_category_is_level_1(self):
        assert _determine_level("dsp_down") == 1

    def test_down_type_is_level_2(self):
        assert _determine_level("dsp_down_imagery") == 2


class TestDetermineParent:
    def test_launch_category_has_no_parent(self):
        assert _determine_parent("dsp_launch") is None

    def test_launch_small_parent_is_launch(self):
        assert _determine_parent("dsp_launch_small") == "dsp_launch"

    def test_sat_comms_parent_is_sat(self):
        assert _determine_parent("dsp_sat_comms") == "dsp_sat"

    def test_down_imagery_parent_is_down(self):
        assert _determine_parent("dsp_down_imagery") == "dsp_down"


class TestSpaceNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(SPACE_NODES) > 0

    def test_has_launch_vehicles_category(self):
        codes = [n[0] for n in SPACE_NODES]
        assert "dsp_launch" in codes

    def test_has_satellite_types_category(self):
        codes = [n[0] for n in SPACE_NODES]
        assert "dsp_sat" in codes

    def test_has_ground_segment_category(self):
        codes = [n[0] for n in SPACE_NODES]
        assert "dsp_ground" in codes

    def test_has_downstream_apps_category(self):
        codes = [n[0] for n in SPACE_NODES]
        assert "dsp_down" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in SPACE_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in SPACE_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in SPACE_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in SPACE_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(SPACE_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in SPACE_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_space_module_importable():
    assert callable(ingest_domain_space)
    assert isinstance(SPACE_NODES, list)


def test_ingest_domain_space(db_pool):
    """Integration test: space taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_space(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_space'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_space'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_space_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_space(conn)
            count2 = await ingest_domain_space(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
