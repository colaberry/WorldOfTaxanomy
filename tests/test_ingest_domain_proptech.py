"""Tests for PropTech domain taxonomy ingester.

RED tests - written before any implementation exists.

PropTech taxonomy organizes property technology sector types:
  Property Management Tech (dpt_mgmt*)   - tenant, lease, maintenance, accounting
  Real Estate Marketplaces (dpt_market*)  - residential, commercial, auction, data
  Construction Tech        (dpt_const*)   - BIM, project mgmt, drones, modular
  Smart Building           (dpt_smart*)   - IoT, energy, access, digital twin
  Mortgage Tech            (dpt_mortgage*) - origination, underwriting, closing

Source: NAICS 5312 + 5311 real estate industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_proptech import (
    PROPTECH_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_proptech,
)


class TestDetermineLevel:
    def test_mgmt_category_is_level_1(self):
        assert _determine_level("dpt_mgmt") == 1

    def test_mgmt_type_is_level_2(self):
        assert _determine_level("dpt_mgmt_tenant") == 2

    def test_market_category_is_level_1(self):
        assert _determine_level("dpt_market") == 1

    def test_market_type_is_level_2(self):
        assert _determine_level("dpt_market_resid") == 2

    def test_smart_category_is_level_1(self):
        assert _determine_level("dpt_smart") == 1

    def test_smart_type_is_level_2(self):
        assert _determine_level("dpt_smart_iot") == 2


class TestDetermineParent:
    def test_mgmt_category_has_no_parent(self):
        assert _determine_parent("dpt_mgmt") is None

    def test_mgmt_tenant_parent_is_mgmt(self):
        assert _determine_parent("dpt_mgmt_tenant") == "dpt_mgmt"

    def test_market_resid_parent_is_market(self):
        assert _determine_parent("dpt_market_resid") == "dpt_market"

    def test_smart_iot_parent_is_smart(self):
        assert _determine_parent("dpt_smart_iot") == "dpt_smart"


class TestPropTechNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(PROPTECH_NODES) > 0

    def test_has_mgmt_category(self):
        codes = [n[0] for n in PROPTECH_NODES]
        assert "dpt_mgmt" in codes

    def test_has_market_category(self):
        codes = [n[0] for n in PROPTECH_NODES]
        assert "dpt_market" in codes

    def test_has_const_category(self):
        codes = [n[0] for n in PROPTECH_NODES]
        assert "dpt_const" in codes

    def test_has_smart_category(self):
        codes = [n[0] for n in PROPTECH_NODES]
        assert "dpt_smart" in codes

    def test_has_mortgage_category(self):
        codes = [n[0] for n in PROPTECH_NODES]
        assert "dpt_mortgage" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in PROPTECH_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in PROPTECH_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in PROPTECH_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in PROPTECH_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(PROPTECH_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in PROPTECH_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_proptech_module_importable():
    assert callable(ingest_domain_proptech)
    assert isinstance(PROPTECH_NODES, list)


def test_ingest_domain_proptech(db_pool):
    """Integration test: PropTech taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_proptech(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_proptech'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_proptech'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_proptech_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_proptech(conn)
            count2 = await ingest_domain_proptech(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
