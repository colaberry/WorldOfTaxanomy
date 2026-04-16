"""Tests for FMCSA -> Truck Domain crosswalk ingester.

RED tests - written before any implementation exists.

Links FMCSA regulation nodes to domain truck taxonomy nodes:
  fmcsa_regs -> domain_truck_freight  (regulation -> freight type)
  fmcsa_regs -> domain_truck_cargo    (regulation -> cargo type)
  fmcsa_regs -> domain_truck_vehicle  (regulation -> vehicle class)

Source: Derived from FMCSA regulatory scope. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.crosswalk_fmcsa_truck import (
    FMCSA_TRUCK_MAPPINGS,
    ingest_crosswalk_fmcsa_truck,
)


class TestFmcsaTruckMappings:
    def test_mappings_is_non_empty(self):
        assert len(FMCSA_TRUCK_MAPPINGS) > 0

    def test_mappings_are_tuples_of_four(self):
        for m in FMCSA_TRUCK_MAPPINGS:
            assert len(m) == 4, f"Expected 4-tuple, got {len(m)}: {m}"

    def test_has_hazmat_to_cargo_link(self):
        """FMCSA hazmat reg should link to dtc_haz (hazmat cargo category)."""
        targets = [(src, tgt) for src, src_sys, tgt, tgt_sys in FMCSA_TRUCK_MAPPINGS]
        # some fmcsa hazmat code should map to a dtc_haz* code
        haz_links = [(src, tgt) for src, tgt in targets if tgt.startswith("dtc_haz")]
        assert len(haz_links) > 0

    def test_has_hos_to_freight_link(self):
        """FMCSA HOS reg should link to freight service codes."""
        targets = [(src, tgt) for src, src_sys, tgt, tgt_sys in FMCSA_TRUCK_MAPPINGS]
        hos_links = [(src, tgt) for src, tgt in targets if src.startswith("fmcsa_hos")]
        assert len(hos_links) > 0

    def test_no_duplicate_pairs(self):
        pairs = [(src, tgt) for src, src_sys, tgt, tgt_sys in FMCSA_TRUCK_MAPPINGS]
        assert len(pairs) == len(set(pairs))

    def test_all_source_systems_are_fmcsa(self):
        for src, src_sys, tgt, tgt_sys in FMCSA_TRUCK_MAPPINGS:
            assert src_sys == "fmcsa_regs", f"Unexpected source system: {src_sys}"

    def test_all_target_systems_are_domain(self):
        valid_targets = {"domain_truck_freight", "domain_truck_cargo", "domain_truck_vehicle"}
        for src, src_sys, tgt, tgt_sys in FMCSA_TRUCK_MAPPINGS:
            assert tgt_sys in valid_targets, f"Unexpected target system: {tgt_sys}"


def test_crosswalk_fmcsa_truck_module_importable():
    assert callable(ingest_crosswalk_fmcsa_truck)
    assert isinstance(FMCSA_TRUCK_MAPPINGS, list)


def test_ingest_crosswalk_fmcsa_truck(db_pool):
    """Integration test: edges linking FMCSA regs to truck domain taxonomies."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        from world_of_taxonomy.ingest.fmcsa_regs import ingest_fmcsa_regs
        from world_of_taxonomy.ingest.domain_truck_freight import ingest_domain_truck_freight
        from world_of_taxonomy.ingest.domain_truck_cargo import ingest_domain_truck_cargo
        from world_of_taxonomy.ingest.domain_truck_vehicle import ingest_domain_truck_vehicle
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            await ingest_fmcsa_regs(conn)
            await ingest_domain_truck_freight(conn)
            await ingest_domain_truck_cargo(conn)
            await ingest_domain_truck_vehicle(conn)
            count = await ingest_crosswalk_fmcsa_truck(conn)
            assert count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_crosswalk_fmcsa_truck_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        from world_of_taxonomy.ingest.fmcsa_regs import ingest_fmcsa_regs
        from world_of_taxonomy.ingest.domain_truck_freight import ingest_domain_truck_freight
        from world_of_taxonomy.ingest.domain_truck_cargo import ingest_domain_truck_cargo
        from world_of_taxonomy.ingest.domain_truck_vehicle import ingest_domain_truck_vehicle
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            await ingest_fmcsa_regs(conn)
            await ingest_domain_truck_freight(conn)
            await ingest_domain_truck_cargo(conn)
            await ingest_domain_truck_vehicle(conn)
            count1 = await ingest_crosswalk_fmcsa_truck(conn)
            count2 = await ingest_crosswalk_fmcsa_truck(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
