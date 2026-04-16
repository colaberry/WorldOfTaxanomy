"""Tests for AgriTech domain taxonomy ingester.

RED tests - written before any implementation exists.

AgriTech taxonomy organizes agricultural technology sector types:
  Precision Agriculture     (dat_precision*) - GPS, VRA, remote sensing, yield
  Farm Management Software  (dat_fms*)       - ERP, crop planning, livestock
  Agricultural Drones       (dat_drone*)     - spraying, scouting, mapping
  Soil Analytics            (dat_soil*)      - sensors, testing, carbon, moisture
  Vertical Farming          (dat_vertical*)  - hydroponic, aeroponic, LED, climate

Source: NAICS 1111 + 1112 agriculture industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_agritech import (
    AGRITECH_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_agritech,
)


class TestDetermineLevel:
    def test_precision_category_is_level_1(self):
        assert _determine_level("dat_precision") == 1

    def test_precision_type_is_level_2(self):
        assert _determine_level("dat_precision_gps") == 2

    def test_fms_category_is_level_1(self):
        assert _determine_level("dat_fms") == 1

    def test_fms_type_is_level_2(self):
        assert _determine_level("dat_fms_erp") == 2

    def test_drone_category_is_level_1(self):
        assert _determine_level("dat_drone") == 1

    def test_drone_type_is_level_2(self):
        assert _determine_level("dat_drone_spray") == 2


class TestDetermineParent:
    def test_precision_category_has_no_parent(self):
        assert _determine_parent("dat_precision") is None

    def test_precision_gps_parent_is_precision(self):
        assert _determine_parent("dat_precision_gps") == "dat_precision"

    def test_fms_erp_parent_is_fms(self):
        assert _determine_parent("dat_fms_erp") == "dat_fms"

    def test_drone_spray_parent_is_drone(self):
        assert _determine_parent("dat_drone_spray") == "dat_drone"


class TestAgriTechNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(AGRITECH_NODES) > 0

    def test_has_precision_category(self):
        codes = [n[0] for n in AGRITECH_NODES]
        assert "dat_precision" in codes

    def test_has_fms_category(self):
        codes = [n[0] for n in AGRITECH_NODES]
        assert "dat_fms" in codes

    def test_has_drone_category(self):
        codes = [n[0] for n in AGRITECH_NODES]
        assert "dat_drone" in codes

    def test_has_soil_category(self):
        codes = [n[0] for n in AGRITECH_NODES]
        assert "dat_soil" in codes

    def test_has_vertical_category(self):
        codes = [n[0] for n in AGRITECH_NODES]
        assert "dat_vertical" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in AGRITECH_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in AGRITECH_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in AGRITECH_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in AGRITECH_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(AGRITECH_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in AGRITECH_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_agritech_module_importable():
    assert callable(ingest_domain_agritech)
    assert isinstance(AGRITECH_NODES, list)


def test_ingest_domain_agritech(db_pool):
    """Integration test: AgriTech taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_agritech(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_agritech'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_agritech'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_agritech_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_agritech(conn)
            count2 = await ingest_domain_agritech(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
