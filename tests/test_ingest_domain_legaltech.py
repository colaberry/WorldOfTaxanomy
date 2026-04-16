"""Tests for LegalTech domain taxonomy ingester.

RED tests - written before any implementation exists.

LegalTech taxonomy organizes legal technology sector types:
  Contract Automation    (dlt_contract*)    - CLM, drafting, review, e-sign
  Legal Research AI      (dlt_research*)    - case law, statute, prediction, citation
  E-Discovery            (dlt_ediscovery*)  - collection, processing, TAR, production
  Compliance Automation  (dlt_compliance*)  - monitoring, policy, audit, privacy
  Court Technology       (dlt_court*)       - e-filing, virtual hearing, docket

Source: NAICS 5411 + 5112 legal/software industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_legaltech import (
    LEGALTECH_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_legaltech,
)


class TestDetermineLevel:
    def test_contract_category_is_level_1(self):
        assert _determine_level("dlt_contract") == 1

    def test_contract_type_is_level_2(self):
        assert _determine_level("dlt_contract_lifecycle") == 2

    def test_research_category_is_level_1(self):
        assert _determine_level("dlt_research") == 1

    def test_research_type_is_level_2(self):
        assert _determine_level("dlt_research_case") == 2

    def test_ediscovery_category_is_level_1(self):
        assert _determine_level("dlt_ediscovery") == 1

    def test_ediscovery_type_is_level_2(self):
        assert _determine_level("dlt_ediscovery_collect") == 2


class TestDetermineParent:
    def test_contract_category_has_no_parent(self):
        assert _determine_parent("dlt_contract") is None

    def test_contract_lifecycle_parent_is_contract(self):
        assert _determine_parent("dlt_contract_lifecycle") == "dlt_contract"

    def test_research_case_parent_is_research(self):
        assert _determine_parent("dlt_research_case") == "dlt_research"

    def test_court_filing_parent_is_court(self):
        assert _determine_parent("dlt_court_filing") == "dlt_court"


class TestLegalTechNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(LEGALTECH_NODES) > 0

    def test_has_contract_category(self):
        codes = [n[0] for n in LEGALTECH_NODES]
        assert "dlt_contract" in codes

    def test_has_research_category(self):
        codes = [n[0] for n in LEGALTECH_NODES]
        assert "dlt_research" in codes

    def test_has_ediscovery_category(self):
        codes = [n[0] for n in LEGALTECH_NODES]
        assert "dlt_ediscovery" in codes

    def test_has_compliance_category(self):
        codes = [n[0] for n in LEGALTECH_NODES]
        assert "dlt_compliance" in codes

    def test_has_court_category(self):
        codes = [n[0] for n in LEGALTECH_NODES]
        assert "dlt_court" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in LEGALTECH_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in LEGALTECH_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in LEGALTECH_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in LEGALTECH_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(LEGALTECH_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in LEGALTECH_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_legaltech_module_importable():
    assert callable(ingest_domain_legaltech)
    assert isinstance(LEGALTECH_NODES, list)


def test_ingest_domain_legaltech(db_pool):
    """Integration test: LegalTech taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_legaltech(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_legaltech'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_legaltech'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_legaltech_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_legaltech(conn)
            count2 = await ingest_domain_legaltech(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
