"""Tests for New Energy Storage domain taxonomy ingester.

RED tests - written before any implementation exists.

Energy storage taxonomy organizes new energy storage sector types:
  Battery Chemistries (des_batt*)  - Li-ion, solid-state, sodium-ion, flow
  Grid-Scale Storage  (des_grid*)  - utility BESS, pumped hydro, CAES
  Thermal Storage     (des_therm*) - molten salt, ice storage, phase-change
  Hydrogen Storage    (des_h2*)    - compressed gas, liquid H2, metal hydride
  Vehicle Batteries   (des_veh*)   - passenger EV, commercial EV, e-bike
  Storage Software    (des_soft*)  - BMS, EMS, grid dispatch, VPP

Source: NAICS 335 + 3691 + 2211 energy storage industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.domain_energy_storage import (
    STORAGE_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_energy_storage,
)


class TestDetermineLevel:
    def test_batt_category_is_level_1(self):
        assert _determine_level("des_batt") == 1

    def test_batt_type_is_level_2(self):
        assert _determine_level("des_batt_liion") == 2

    def test_grid_category_is_level_1(self):
        assert _determine_level("des_grid") == 1

    def test_grid_type_is_level_2(self):
        assert _determine_level("des_grid_bess") == 2

    def test_h2_category_is_level_1(self):
        assert _determine_level("des_h2") == 1

    def test_h2_type_is_level_2(self):
        assert _determine_level("des_h2_compressed") == 2


class TestDetermineParent:
    def test_batt_category_has_no_parent(self):
        assert _determine_parent("des_batt") is None

    def test_batt_liion_parent_is_batt(self):
        assert _determine_parent("des_batt_liion") == "des_batt"

    def test_grid_bess_parent_is_grid(self):
        assert _determine_parent("des_grid_bess") == "des_grid"

    def test_h2_compressed_parent_is_h2(self):
        assert _determine_parent("des_h2_compressed") == "des_h2"


class TestStorageNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(STORAGE_NODES) > 0

    def test_has_battery_chemistries_category(self):
        codes = [n[0] for n in STORAGE_NODES]
        assert "des_batt" in codes

    def test_has_grid_scale_storage_category(self):
        codes = [n[0] for n in STORAGE_NODES]
        assert "des_grid" in codes

    def test_has_hydrogen_storage_category(self):
        codes = [n[0] for n in STORAGE_NODES]
        assert "des_h2" in codes

    def test_has_thermal_storage_category(self):
        codes = [n[0] for n in STORAGE_NODES]
        assert "des_therm" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in STORAGE_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in STORAGE_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in STORAGE_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in STORAGE_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(STORAGE_NODES) >= 18

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in STORAGE_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_energy_storage_module_importable():
    assert callable(ingest_domain_energy_storage)
    assert isinstance(STORAGE_NODES, list)


def test_ingest_domain_energy_storage(db_pool):
    """Integration test: energy storage taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_energy_storage(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_energy_storage'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_energy_storage'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_energy_storage_idempotent(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_energy_storage(conn)
            count2 = await ingest_domain_energy_storage(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
