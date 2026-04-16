"""Tests for InsurTech domain taxonomy ingester.

RED tests - written before any implementation exists.

InsurTech taxonomy organizes insurance technology sector types:
  Digital Underwriting   (dit_underwrite*)  - auto risk, telematics, IoT, instant
  Claims Automation      (dit_claims*)      - FNOL, AI adjudication, fraud
  Parametric Insurance   (dit_parametric*)  - weather, catastrophe, crop, cyber
  Peer-to-Peer Insurance (dit_p2p*)         - mutual aid, micro, community, DAO
  Insurance Analytics    (dit_analytics*)   - actuarial, portfolio, customer, pricing

Source: NAICS 5241 + 5242 insurance industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_insurtech import (
    INSURTECH_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_insurtech,
)


class TestDetermineLevel:
    def test_underwrite_category_is_level_1(self):
        assert _determine_level("dit_underwrite") == 1

    def test_underwrite_type_is_level_2(self):
        assert _determine_level("dit_underwrite_auto") == 2

    def test_claims_category_is_level_1(self):
        assert _determine_level("dit_claims") == 1

    def test_claims_type_is_level_2(self):
        assert _determine_level("dit_claims_fnol") == 2

    def test_parametric_category_is_level_1(self):
        assert _determine_level("dit_parametric") == 1

    def test_parametric_type_is_level_2(self):
        assert _determine_level("dit_parametric_weather") == 2


class TestDetermineParent:
    def test_underwrite_category_has_no_parent(self):
        assert _determine_parent("dit_underwrite") is None

    def test_underwrite_auto_parent_is_underwrite(self):
        assert _determine_parent("dit_underwrite_auto") == "dit_underwrite"

    def test_claims_fnol_parent_is_claims(self):
        assert _determine_parent("dit_claims_fnol") == "dit_claims"

    def test_analytics_actuarial_parent_is_analytics(self):
        assert _determine_parent("dit_analytics_actuarial") == "dit_analytics"


class TestInsurTechNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(INSURTECH_NODES) > 0

    def test_has_underwrite_category(self):
        codes = [n[0] for n in INSURTECH_NODES]
        assert "dit_underwrite" in codes

    def test_has_claims_category(self):
        codes = [n[0] for n in INSURTECH_NODES]
        assert "dit_claims" in codes

    def test_has_parametric_category(self):
        codes = [n[0] for n in INSURTECH_NODES]
        assert "dit_parametric" in codes

    def test_has_p2p_category(self):
        codes = [n[0] for n in INSURTECH_NODES]
        assert "dit_p2p" in codes

    def test_has_analytics_category(self):
        codes = [n[0] for n in INSURTECH_NODES]
        assert "dit_analytics" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in INSURTECH_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in INSURTECH_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in INSURTECH_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in INSURTECH_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(INSURTECH_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in INSURTECH_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_insurtech_module_importable():
    assert callable(ingest_domain_insurtech)
    assert isinstance(INSURTECH_NODES, list)


def test_ingest_domain_insurtech(db_pool):
    """Integration test: InsurTech taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_insurtech(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_insurtech'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_insurtech'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_insurtech_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_insurtech(conn)
            count2 = await ingest_domain_insurtech(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
