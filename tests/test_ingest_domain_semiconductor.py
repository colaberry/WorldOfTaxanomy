"""Tests for Next-Generation Semiconductor domain taxonomy ingester.

RED tests - written before any implementation exists.

Semiconductor taxonomy organizes next-gen semiconductor sector types:
  Logic Chips       (dsc_logic*)   - CPU, GPU, NPU, FPGA, ASIC
  Memory            (dsc_mem*)     - DRAM, NAND, MRAM, 3D XPoint
  Analog/Mixed      (dsc_analog*)  - data converters, RF, power management
  Power Semis       (dsc_power*)   - SiC, GaN, IGBT, MOSFETs
  Photonics ICs     (dsc_phot*)    - silicon photonics, III-V lasers, detectors
  MEMS              (dsc_mems*)    - sensors, actuators, RF MEMS
  Advanced Packaging (dsc_pkg*)    - 2.5D, 3D IC, chiplets, advanced PCB
  Foundry/Process   (dsc_fab*)     - leading-edge nodes, mature nodes, specialty

Source: NAICS 3344 + 5415 semiconductor industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_semiconductor import (
    SEMICONDUCTOR_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_semiconductor,
)


class TestDetermineLevel:
    def test_logic_category_is_level_1(self):
        assert _determine_level("dsc_logic") == 1

    def test_logic_type_is_level_2(self):
        assert _determine_level("dsc_logic_gpu") == 2

    def test_mem_category_is_level_1(self):
        assert _determine_level("dsc_mem") == 1

    def test_mem_type_is_level_2(self):
        assert _determine_level("dsc_mem_dram") == 2

    def test_power_category_is_level_1(self):
        assert _determine_level("dsc_power") == 1

    def test_power_type_is_level_2(self):
        assert _determine_level("dsc_power_sic") == 2


class TestDetermineParent:
    def test_logic_category_has_no_parent(self):
        assert _determine_parent("dsc_logic") is None

    def test_logic_gpu_parent_is_logic(self):
        assert _determine_parent("dsc_logic_gpu") == "dsc_logic"

    def test_mem_dram_parent_is_mem(self):
        assert _determine_parent("dsc_mem_dram") == "dsc_mem"

    def test_power_sic_parent_is_power(self):
        assert _determine_parent("dsc_power_sic") == "dsc_power"


class TestSemiconductorNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(SEMICONDUCTOR_NODES) > 0

    def test_has_logic_chips_category(self):
        codes = [n[0] for n in SEMICONDUCTOR_NODES]
        assert "dsc_logic" in codes

    def test_has_memory_category(self):
        codes = [n[0] for n in SEMICONDUCTOR_NODES]
        assert "dsc_mem" in codes

    def test_has_power_semis_category(self):
        codes = [n[0] for n in SEMICONDUCTOR_NODES]
        assert "dsc_power" in codes

    def test_has_advanced_packaging_category(self):
        codes = [n[0] for n in SEMICONDUCTOR_NODES]
        assert "dsc_pkg" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in SEMICONDUCTOR_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in SEMICONDUCTOR_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in SEMICONDUCTOR_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in SEMICONDUCTOR_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(SEMICONDUCTOR_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in SEMICONDUCTOR_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_semiconductor_module_importable():
    assert callable(ingest_domain_semiconductor)
    assert isinstance(SEMICONDUCTOR_NODES, list)


def test_ingest_domain_semiconductor(db_pool):
    """Integration test: semiconductor taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_semiconductor(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_semiconductor'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_semiconductor'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_semiconductor_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_semiconductor(conn)
            count2 = await ingest_domain_semiconductor(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
