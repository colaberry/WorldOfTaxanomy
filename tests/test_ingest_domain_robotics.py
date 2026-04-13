"""Tests for Autonomous Systems and Robotics domain taxonomy ingester.

RED tests - written before any implementation exists.

Robotics taxonomy organizes autonomous systems and robotics sector types:
  Industrial Robots (drb_indust*)  - welding, assembly, painting, pick-and-place
  Collaborative     (drb_cobot*)   - human-robot collaboration, safety-rated
  Mobile Robots     (drb_amr*)     - AMR, AGV, indoor navigation
  Aerial Robots     (drb_drone*)   - commercial UAV, delivery, inspection
  Humanoid Robots   (drb_human*)   - bipedal, service, social robots
  Surgical Robots   (drb_surg*)    - laparoscopic, orthopedic, neuro
  Autonomous Vehicles (drb_av*)    - passenger, freight, off-highway
  Robot Software    (drb_soft*)    - ROS, perception, motion planning

Source: NAICS 333 + 336 + 5415 robotics industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.domain_robotics import (
    ROBOTICS_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_robotics,
)


class TestDetermineLevel:
    def test_indust_category_is_level_1(self):
        assert _determine_level("drb_indust") == 1

    def test_indust_type_is_level_2(self):
        assert _determine_level("drb_indust_weld") == 2

    def test_amr_category_is_level_1(self):
        assert _determine_level("drb_amr") == 1

    def test_amr_type_is_level_2(self):
        assert _determine_level("drb_amr_indoor") == 2

    def test_av_category_is_level_1(self):
        assert _determine_level("drb_av") == 1

    def test_av_type_is_level_2(self):
        assert _determine_level("drb_av_passenger") == 2


class TestDetermineParent:
    def test_indust_category_has_no_parent(self):
        assert _determine_parent("drb_indust") is None

    def test_indust_weld_parent_is_indust(self):
        assert _determine_parent("drb_indust_weld") == "drb_indust"

    def test_amr_indoor_parent_is_amr(self):
        assert _determine_parent("drb_amr_indoor") == "drb_amr"

    def test_av_passenger_parent_is_av(self):
        assert _determine_parent("drb_av_passenger") == "drb_av"


class TestRoboticsNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(ROBOTICS_NODES) > 0

    def test_has_industrial_robots_category(self):
        codes = [n[0] for n in ROBOTICS_NODES]
        assert "drb_indust" in codes

    def test_has_mobile_robots_category(self):
        codes = [n[0] for n in ROBOTICS_NODES]
        assert "drb_amr" in codes

    def test_has_autonomous_vehicles_category(self):
        codes = [n[0] for n in ROBOTICS_NODES]
        assert "drb_av" in codes

    def test_has_drones_category(self):
        codes = [n[0] for n in ROBOTICS_NODES]
        assert "drb_drone" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in ROBOTICS_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in ROBOTICS_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in ROBOTICS_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in ROBOTICS_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(ROBOTICS_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in ROBOTICS_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_robotics_module_importable():
    assert callable(ingest_domain_robotics)
    assert isinstance(ROBOTICS_NODES, list)


def test_ingest_domain_robotics(db_pool):
    """Integration test: robotics taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_robotics(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_robotics'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_robotics'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_robotics_idempotent(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_robotics(conn)
            count2 = await ingest_domain_robotics(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
