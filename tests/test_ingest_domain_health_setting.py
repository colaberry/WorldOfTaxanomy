"""Tests for Health Care Setting domain taxonomy ingester.

RED tests - written before any implementation exists.

Healthcare setting taxonomy organizes care delivery environments:
  Inpatient     (dhs_inpt*)   - acute hospital, ICU, surgical, rehab
  Outpatient    (dhs_outpt*)  - clinic, physician office, urgent care, ASC
  Post-Acute    (dhs_post*)   - SNF, home health, hospice, LTACH
  Behavioral    (dhs_beh*)    - inpatient psych, outpatient mental health, SUD
  Telehealth    (dhs_tele*)   - synchronous, asynchronous, remote monitoring

Source: CMS (Centers for Medicare and Medicaid Services) facility types. Public domain.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_health_setting import (
    HEALTH_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_health_setting,
)


class TestDetermineLevel:
    def test_inpatient_category_is_level_1(self):
        assert _determine_level("dhs_inpt") == 1

    def test_acute_hospital_is_level_2(self):
        assert _determine_level("dhs_inpt_acute") == 2

    def test_outpatient_category_is_level_1(self):
        assert _determine_level("dhs_outpt") == 1

    def test_clinic_is_level_2(self):
        assert _determine_level("dhs_outpt_clinic") == 2


class TestDetermineParent:
    def test_inpatient_category_has_no_parent(self):
        assert _determine_parent("dhs_inpt") is None

    def test_acute_parent_is_inpatient(self):
        assert _determine_parent("dhs_inpt_acute") == "dhs_inpt"

    def test_clinic_parent_is_outpatient(self):
        assert _determine_parent("dhs_outpt_clinic") == "dhs_outpt"


class TestHealthNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(HEALTH_NODES) > 0

    def test_has_inpatient_category(self):
        codes = [n[0] for n in HEALTH_NODES]
        assert "dhs_inpt" in codes

    def test_has_outpatient_category(self):
        codes = [n[0] for n in HEALTH_NODES]
        assert "dhs_outpt" in codes

    def test_has_post_acute_category(self):
        codes = [n[0] for n in HEALTH_NODES]
        assert "dhs_post" in codes

    def test_has_acute_hospital(self):
        codes = [n[0] for n in HEALTH_NODES]
        assert "dhs_inpt_acute" in codes

    def test_has_urgent_care(self):
        codes = [n[0] for n in HEALTH_NODES]
        assert "dhs_outpt_urgent" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in HEALTH_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in HEALTH_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in HEALTH_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in HEALTH_NODES:
            if level == 2:
                assert parent is not None


def test_domain_health_setting_module_importable():
    assert callable(ingest_domain_health_setting)
    assert isinstance(HEALTH_NODES, list)


def test_ingest_domain_health_setting(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_health_setting(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_health_setting'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_health_setting_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_health_setting(conn)
            count2 = await ingest_domain_health_setting(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
