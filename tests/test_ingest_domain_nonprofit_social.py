"""Tests for Nonprofit and Social Enterprise domain taxonomy ingester.

RED tests - written before any implementation exists.

Nonprofit and social enterprise taxonomy organizes types into categories:
  Charitable Orgs    (dns_charity*)   - human services, health, education, religious
  Foundations        (dns_found*)     - private, community, operating, donor-advised
  Social Enterprises (dns_social*)    - B Corp, cooperative, microfinance, impact
  Advocacy Groups    (dns_advocacy*)  - environmental, rights, policy, labor
  Intl Development   (dns_intl*)      - NGO, bilateral, multilateral, volunteer

Source: NAICS 8131 + 8132 + 8133 + 8134 nonprofit sector. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_nonprofit_social import (
    NONPROFIT_SOCIAL_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_nonprofit_social,
)


class TestDetermineLevel:
    def test_charity_category_is_level_1(self):
        assert _determine_level("dns_charity") == 1

    def test_charity_type_is_level_2(self):
        assert _determine_level("dns_charity_human") == 2

    def test_found_category_is_level_1(self):
        assert _determine_level("dns_found") == 1

    def test_found_type_is_level_2(self):
        assert _determine_level("dns_found_private") == 2

    def test_intl_category_is_level_1(self):
        assert _determine_level("dns_intl") == 1

    def test_intl_type_is_level_2(self):
        assert _determine_level("dns_intl_ngo") == 2


class TestDetermineParent:
    def test_charity_category_has_no_parent(self):
        assert _determine_parent("dns_charity") is None

    def test_charity_human_parent_is_charity(self):
        assert _determine_parent("dns_charity_human") == "dns_charity"

    def test_social_bcorp_parent_is_social(self):
        assert _determine_parent("dns_social_bcorp") == "dns_social"

    def test_advocacy_environ_parent_is_advocacy(self):
        assert _determine_parent("dns_advocacy_environ") == "dns_advocacy"


class TestNonprofitSocialNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(NONPROFIT_SOCIAL_NODES) > 0

    def test_has_charitable_category(self):
        codes = [n[0] for n in NONPROFIT_SOCIAL_NODES]
        assert "dns_charity" in codes

    def test_has_foundations_category(self):
        codes = [n[0] for n in NONPROFIT_SOCIAL_NODES]
        assert "dns_found" in codes

    def test_has_social_enterprises_category(self):
        codes = [n[0] for n in NONPROFIT_SOCIAL_NODES]
        assert "dns_social" in codes

    def test_has_advocacy_category(self):
        codes = [n[0] for n in NONPROFIT_SOCIAL_NODES]
        assert "dns_advocacy" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in NONPROFIT_SOCIAL_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in NONPROFIT_SOCIAL_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in NONPROFIT_SOCIAL_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in NONPROFIT_SOCIAL_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(NONPROFIT_SOCIAL_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in NONPROFIT_SOCIAL_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_nonprofit_social_module_importable():
    assert callable(ingest_domain_nonprofit_social)
    assert isinstance(NONPROFIT_SOCIAL_NODES, list)


def test_ingest_domain_nonprofit_social(db_pool):
    """Integration test: nonprofit/social taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_nonprofit_social(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_nonprofit_social'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_nonprofit_social'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_nonprofit_social_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_nonprofit_social(conn)
            count2 = await ingest_domain_nonprofit_social(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
