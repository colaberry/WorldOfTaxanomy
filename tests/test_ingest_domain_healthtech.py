"""Tests for HealthTech domain taxonomy ingester.

RED tests - written before any implementation exists.

HealthTech taxonomy organizes health technology sector types:
  Telemedicine              (dht_telemed*)   - video, async, triage, mental
  Remote Patient Monitoring (dht_rpm*)       - wearable, chronic, cardiac, glucose
  Health Data Analytics     (dht_analytics*) - population, EHR, genomic, imaging
  Digital Therapeutics      (dht_dtx*)       - behavioral, musculoskeletal, cognitive
  Clinical Trial Tech       (dht_trial*)     - recruitment, EDC, decentralized

Source: NAICS 6211 + 5112 health/software industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_healthtech import (
    HEALTHTECH_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_healthtech,
)


class TestDetermineLevel:
    def test_telemed_category_is_level_1(self):
        assert _determine_level("dht_telemed") == 1

    def test_telemed_type_is_level_2(self):
        assert _determine_level("dht_telemed_video") == 2

    def test_rpm_category_is_level_1(self):
        assert _determine_level("dht_rpm") == 1

    def test_rpm_type_is_level_2(self):
        assert _determine_level("dht_rpm_wearable") == 2

    def test_analytics_category_is_level_1(self):
        assert _determine_level("dht_analytics") == 1

    def test_analytics_type_is_level_2(self):
        assert _determine_level("dht_analytics_pop") == 2


class TestDetermineParent:
    def test_telemed_category_has_no_parent(self):
        assert _determine_parent("dht_telemed") is None

    def test_telemed_video_parent_is_telemed(self):
        assert _determine_parent("dht_telemed_video") == "dht_telemed"

    def test_rpm_wearable_parent_is_rpm(self):
        assert _determine_parent("dht_rpm_wearable") == "dht_rpm"

    def test_analytics_pop_parent_is_analytics(self):
        assert _determine_parent("dht_analytics_pop") == "dht_analytics"


class TestHealthTechNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(HEALTHTECH_NODES) > 0

    def test_has_telemed_category(self):
        codes = [n[0] for n in HEALTHTECH_NODES]
        assert "dht_telemed" in codes

    def test_has_rpm_category(self):
        codes = [n[0] for n in HEALTHTECH_NODES]
        assert "dht_rpm" in codes

    def test_has_analytics_category(self):
        codes = [n[0] for n in HEALTHTECH_NODES]
        assert "dht_analytics" in codes

    def test_has_dtx_category(self):
        codes = [n[0] for n in HEALTHTECH_NODES]
        assert "dht_dtx" in codes

    def test_has_trial_category(self):
        codes = [n[0] for n in HEALTHTECH_NODES]
        assert "dht_trial" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in HEALTHTECH_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in HEALTHTECH_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in HEALTHTECH_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in HEALTHTECH_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(HEALTHTECH_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in HEALTHTECH_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_healthtech_module_importable():
    assert callable(ingest_domain_healthtech)
    assert isinstance(HEALTHTECH_NODES, list)


def test_ingest_domain_healthtech(db_pool):
    """Integration test: HealthTech taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_healthtech(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_healthtech'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_healthtech'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_healthtech_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_healthtech(conn)
            count2 = await ingest_domain_healthtech(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
