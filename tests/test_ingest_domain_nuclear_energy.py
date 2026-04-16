"""Tests for Nuclear Energy domain taxonomy ingester.

RED tests - written before any implementation exists.

Nuclear energy taxonomy organizes types into categories:
  Nuclear Power Gen  (dne_power*)    - LWR, heavy water, SMR, grid integration
  Fuel Cycle         (dne_fuel*)     - mining, enrichment, fabrication, reprocessing
  Reactor Technology (dne_reactor*)  - Gen IV, fusion, control, safety
  Decommissioning    (dne_decom*)    - dismantling, waste, monitoring
  Nuclear Medicine   (dne_med*)      - isotopes, therapy, imaging, sterilization

Source: NAICS 2211 electric power generation. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_nuclear_energy import (
    NUCLEAR_ENERGY_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_nuclear_energy,
)


class TestDetermineLevel:
    def test_power_category_is_level_1(self):
        assert _determine_level("dne_power") == 1

    def test_power_type_is_level_2(self):
        assert _determine_level("dne_power_lwr") == 2

    def test_fuel_category_is_level_1(self):
        assert _determine_level("dne_fuel") == 1

    def test_fuel_type_is_level_2(self):
        assert _determine_level("dne_fuel_mining") == 2

    def test_reactor_category_is_level_1(self):
        assert _determine_level("dne_reactor") == 1

    def test_reactor_type_is_level_2(self):
        assert _determine_level("dne_reactor_gen4") == 2


class TestDetermineParent:
    def test_power_category_has_no_parent(self):
        assert _determine_parent("dne_power") is None

    def test_power_lwr_parent_is_power(self):
        assert _determine_parent("dne_power_lwr") == "dne_power"

    def test_fuel_mining_parent_is_fuel(self):
        assert _determine_parent("dne_fuel_mining") == "dne_fuel"

    def test_med_isotope_parent_is_med(self):
        assert _determine_parent("dne_med_isotope") == "dne_med"


class TestNuclearEnergyNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(NUCLEAR_ENERGY_NODES) > 0

    def test_has_power_generation_category(self):
        codes = [n[0] for n in NUCLEAR_ENERGY_NODES]
        assert "dne_power" in codes

    def test_has_fuel_cycle_category(self):
        codes = [n[0] for n in NUCLEAR_ENERGY_NODES]
        assert "dne_fuel" in codes

    def test_has_reactor_technology_category(self):
        codes = [n[0] for n in NUCLEAR_ENERGY_NODES]
        assert "dne_reactor" in codes

    def test_has_decommissioning_category(self):
        codes = [n[0] for n in NUCLEAR_ENERGY_NODES]
        assert "dne_decom" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in NUCLEAR_ENERGY_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in NUCLEAR_ENERGY_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in NUCLEAR_ENERGY_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in NUCLEAR_ENERGY_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(NUCLEAR_ENERGY_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in NUCLEAR_ENERGY_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_nuclear_energy_module_importable():
    assert callable(ingest_domain_nuclear_energy)
    assert isinstance(NUCLEAR_ENERGY_NODES, list)


def test_ingest_domain_nuclear_energy(db_pool):
    """Integration test: nuclear energy taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_nuclear_energy(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_nuclear_energy'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_nuclear_energy'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_nuclear_energy_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_nuclear_energy(conn)
            count2 = await ingest_domain_nuclear_energy(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
