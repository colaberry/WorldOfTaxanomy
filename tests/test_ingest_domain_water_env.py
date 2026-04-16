"""Tests for Water and Environment domain taxonomy ingester.

RED tests - written before any implementation exists.

Water and environment taxonomy organizes water/env sector types:
  Water Treatment   (dwe_treat*)   - drinking water, purification, disinfection
  Water Distribution (dwe_dist*)   - infrastructure, pipes, pumping
  Wastewater        (dwe_ww*)      - collection, treatment, reuse
  Stormwater        (dwe_storm*)   - management, flood control, retention
  Groundwater       (dwe_gw*)      - aquifer, well, monitoring
  Desalination      (dwe_desal*)   - RO, thermal, brackish water
  Water Quality     (dwe_qual*)    - monitoring, testing, compliance
  Environmental     (dwe_env*)     - remediation, assessment, protection

Source: NAICS 2213 + 5622 + 5416 water/environment industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_water_env import (
    WATER_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_water_env,
)


class TestDetermineLevel:
    def test_treat_category_is_level_1(self):
        assert _determine_level("dwe_treat") == 1

    def test_treat_type_is_level_2(self):
        assert _determine_level("dwe_treat_drink") == 2

    def test_ww_category_is_level_1(self):
        assert _determine_level("dwe_ww") == 1

    def test_ww_type_is_level_2(self):
        assert _determine_level("dwe_ww_collect") == 2

    def test_env_category_is_level_1(self):
        assert _determine_level("dwe_env") == 1

    def test_env_type_is_level_2(self):
        assert _determine_level("dwe_env_remed") == 2


class TestDetermineParent:
    def test_treat_category_has_no_parent(self):
        assert _determine_parent("dwe_treat") is None

    def test_treat_drink_parent_is_treat(self):
        assert _determine_parent("dwe_treat_drink") == "dwe_treat"

    def test_ww_collect_parent_is_ww(self):
        assert _determine_parent("dwe_ww_collect") == "dwe_ww"

    def test_env_remed_parent_is_env(self):
        assert _determine_parent("dwe_env_remed") == "dwe_env"


class TestWaterNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(WATER_NODES) > 0

    def test_has_water_treatment_category(self):
        codes = [n[0] for n in WATER_NODES]
        assert "dwe_treat" in codes

    def test_has_wastewater_category(self):
        codes = [n[0] for n in WATER_NODES]
        assert "dwe_ww" in codes

    def test_has_groundwater_category(self):
        codes = [n[0] for n in WATER_NODES]
        assert "dwe_gw" in codes

    def test_has_environmental_category(self):
        codes = [n[0] for n in WATER_NODES]
        assert "dwe_env" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in WATER_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in WATER_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in WATER_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in WATER_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(WATER_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in WATER_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_water_env_module_importable():
    assert callable(ingest_domain_water_env)
    assert isinstance(WATER_NODES, list)


def test_ingest_domain_water_env(db_pool):
    """Integration test: water/env taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_water_env(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_water_env'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_water_env'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_water_env_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_water_env(conn)
            count2 = await ingest_domain_water_env(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
