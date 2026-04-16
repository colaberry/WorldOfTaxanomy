"""Tests for Telecom Service domain taxonomy ingester.

Telecom Service taxonomy organizes telecommunications service types:
  Mobile          (dts_mobile*)     - voice, data, MVNO, IoT/M2M
  Fixed-Line      (dts_fixed*)      - PSTN, VoIP, SIP trunking, FWA
  Internet        (dts_internet*)   - fiber, cable, DSL, ISP, CDN
  Enterprise      (dts_enterprise*) - MPLS/SD-WAN, UCaaS, DIA, colocation
  Satellite       (dts_satellite*)  - GEO, LEO, MEO

Source: NAICS 5171/5172/5174 telecom industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_telecom_service import (
    TELECOM_SERVICE_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_telecom_service,
)


class TestDetermineLevel:
    def test_mobile_category_is_level_1(self):
        assert _determine_level("dts_mobile") == 1

    def test_mobile_type_is_level_2(self):
        assert _determine_level("dts_mobile_voice") == 2

    def test_fixed_category_is_level_1(self):
        assert _determine_level("dts_fixed") == 1

    def test_internet_type_is_level_2(self):
        assert _determine_level("dts_internet_fiber") == 2

    def test_enterprise_category_is_level_1(self):
        assert _determine_level("dts_enterprise") == 1

    def test_satellite_type_is_level_2(self):
        assert _determine_level("dts_satellite_leo") == 2


class TestDetermineParent:
    def test_mobile_category_has_no_parent(self):
        assert _determine_parent("dts_mobile") is None

    def test_mobile_voice_parent_is_mobile(self):
        assert _determine_parent("dts_mobile_voice") == "dts_mobile"

    def test_internet_fiber_parent_is_internet(self):
        assert _determine_parent("dts_internet_fiber") == "dts_internet"

    def test_satellite_leo_parent_is_satellite(self):
        assert _determine_parent("dts_satellite_leo") == "dts_satellite"


class TestTelecomServiceNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(TELECOM_SERVICE_NODES) > 0

    def test_has_mobile_category(self):
        codes = [n[0] for n in TELECOM_SERVICE_NODES]
        assert "dts_mobile" in codes

    def test_has_fixed_category(self):
        codes = [n[0] for n in TELECOM_SERVICE_NODES]
        assert "dts_fixed" in codes

    def test_has_internet_category(self):
        codes = [n[0] for n in TELECOM_SERVICE_NODES]
        assert "dts_internet" in codes

    def test_has_enterprise_category(self):
        codes = [n[0] for n in TELECOM_SERVICE_NODES]
        assert "dts_enterprise" in codes

    def test_has_satellite_category(self):
        codes = [n[0] for n in TELECOM_SERVICE_NODES]
        assert "dts_satellite" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in TELECOM_SERVICE_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in TELECOM_SERVICE_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in TELECOM_SERVICE_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in TELECOM_SERVICE_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(TELECOM_SERVICE_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in TELECOM_SERVICE_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_telecom_service_module_importable():
    assert callable(ingest_domain_telecom_service)
    assert isinstance(TELECOM_SERVICE_NODES, list)


def test_ingest_domain_telecom_service(db_pool):
    """Integration test: telecom service taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_telecom_service(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_telecom_service'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_telecom_service_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_telecom_service(conn)
            count2 = await ingest_domain_telecom_service(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
