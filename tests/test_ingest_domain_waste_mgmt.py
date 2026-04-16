"""Tests for Waste Management domain taxonomy ingester.

Waste Management taxonomy organizes waste management sector types:
  Solid Waste Collection (dwm_solid*)    - residential, commercial, transfer
  Recycling and Recovery (dwm_recycle*)  - single-stream, metal, e-waste
  Hazardous Waste        (dwm_hazard*)   - chemical, medical, nuclear
  Wastewater Treatment   (dwm_water*)    - municipal, industrial, reuse
  Waste-to-Energy        (dwm_energy*)   - incineration, biogas, plasma

Source: NAICS 5621/5622 waste management structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_waste_mgmt import (
    NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_waste_mgmt,
)


class TestDetermineLevel:
    def test_solid_category_is_level_1(self):
        assert _determine_level("dwm_solid") == 1

    def test_solid_type_is_level_2(self):
        assert _determine_level("dwm_solid_resi") == 2

    def test_recycle_category_is_level_1(self):
        assert _determine_level("dwm_recycle") == 1

    def test_recycle_type_is_level_2(self):
        assert _determine_level("dwm_recycle_metal") == 2

    def test_hazard_category_is_level_1(self):
        assert _determine_level("dwm_hazard") == 1

    def test_hazard_type_is_level_2(self):
        assert _determine_level("dwm_hazard_chem") == 2


class TestDetermineParent:
    def test_solid_category_has_no_parent(self):
        assert _determine_parent("dwm_solid") is None

    def test_solid_resi_parent_is_solid(self):
        assert _determine_parent("dwm_solid_resi") == "dwm_solid"

    def test_recycle_metal_parent_is_recycle(self):
        assert _determine_parent("dwm_recycle_metal") == "dwm_recycle"

    def test_energy_biogas_parent_is_energy(self):
        assert _determine_parent("dwm_energy_biogas") == "dwm_energy"


class TestNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(NODES) > 0

    def test_minimum_node_count(self):
        assert len(NODES) >= 20

    def test_has_solid_category(self):
        codes = [n[0] for n in NODES]
        assert "dwm_solid" in codes

    def test_has_recycle_category(self):
        codes = [n[0] for n in NODES]
        assert "dwm_recycle" in codes

    def test_has_hazard_category(self):
        codes = [n[0] for n in NODES]
        assert "dwm_hazard" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in NODES:
            if level == 2:
                assert parent is not None

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_waste_mgmt_module_importable():
    assert callable(ingest_domain_waste_mgmt)
    assert isinstance(NODES, list)


def test_ingest_domain_waste_mgmt(db_pool):
    """Integration test: waste management taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_waste_mgmt(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_waste_mgmt'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_waste_mgmt_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_waste_mgmt(conn)
            count2 = await ingest_domain_waste_mgmt(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
