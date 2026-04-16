"""Tests for Legal Practice domain taxonomy ingester.

Legal Practice taxonomy organizes legal practice area types:
  Corporate Law    (dlp_corp*)   - M&A, governance, securities, venture, banking
  Litigation       (dlp_lit*)    - civil, class action, arbitration, appellate
  IP Law           (dlp_ip*)     - patent, trademark, copyright, trade secret
  Regulatory       (dlp_reg*)    - antitrust, environmental, healthcare, privacy, tax
  Family Law       (dlp_family*) - divorce, custody, estate planning, adoption

Source: NAICS 5411 legal services industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_legal_practice import (
    LEGAL_PRACTICE_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_legal_practice,
)


class TestDetermineLevel:
    def test_corp_category_is_level_1(self):
        assert _determine_level("dlp_corp") == 1

    def test_corp_type_is_level_2(self):
        assert _determine_level("dlp_corp_manda") == 2

    def test_lit_category_is_level_1(self):
        assert _determine_level("dlp_lit") == 1

    def test_ip_type_is_level_2(self):
        assert _determine_level("dlp_ip_patent") == 2

    def test_reg_category_is_level_1(self):
        assert _determine_level("dlp_reg") == 1

    def test_family_type_is_level_2(self):
        assert _determine_level("dlp_family_divorce") == 2


class TestDetermineParent:
    def test_corp_category_has_no_parent(self):
        assert _determine_parent("dlp_corp") is None

    def test_corp_manda_parent_is_corp(self):
        assert _determine_parent("dlp_corp_manda") == "dlp_corp"

    def test_ip_patent_parent_is_ip(self):
        assert _determine_parent("dlp_ip_patent") == "dlp_ip"

    def test_family_divorce_parent_is_family(self):
        assert _determine_parent("dlp_family_divorce") == "dlp_family"


class TestLegalPracticeNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(LEGAL_PRACTICE_NODES) > 0

    def test_has_corporate_category(self):
        codes = [n[0] for n in LEGAL_PRACTICE_NODES]
        assert "dlp_corp" in codes

    def test_has_litigation_category(self):
        codes = [n[0] for n in LEGAL_PRACTICE_NODES]
        assert "dlp_lit" in codes

    def test_has_ip_category(self):
        codes = [n[0] for n in LEGAL_PRACTICE_NODES]
        assert "dlp_ip" in codes

    def test_has_regulatory_category(self):
        codes = [n[0] for n in LEGAL_PRACTICE_NODES]
        assert "dlp_reg" in codes

    def test_has_family_category(self):
        codes = [n[0] for n in LEGAL_PRACTICE_NODES]
        assert "dlp_family" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in LEGAL_PRACTICE_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in LEGAL_PRACTICE_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in LEGAL_PRACTICE_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in LEGAL_PRACTICE_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(LEGAL_PRACTICE_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in LEGAL_PRACTICE_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_legal_practice_module_importable():
    assert callable(ingest_domain_legal_practice)
    assert isinstance(LEGAL_PRACTICE_NODES, list)


def test_ingest_domain_legal_practice(db_pool):
    """Integration test: legal practice taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_legal_practice(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_legal_practice'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_legal_practice_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_legal_practice(conn)
            count2 = await ingest_domain_legal_practice(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
