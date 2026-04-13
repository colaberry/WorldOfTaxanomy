"""Tests for NAICS 11 -> Agriculture Domain Taxonomies crosswalk ingester.

RED tests - written before any implementation exists.

Links NAICS 11xxx sector codes to four ag domain taxonomies:
  - domain_ag_crop (111xxx Crop Production)
  - domain_ag_livestock (112xxx Animal Production)
  - domain_ag_method (11xxx broadly - farming methods)
  - domain_ag_grade (11xxx broadly - quality grades)

Source: Derived. Open.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.crosswalk_naics11_domains import (
    NAICS_DOMAIN_LINKS,
    ingest_crosswalk_naics11_domains,
)


class TestNaicsDomainLinks:
    def test_links_list_is_non_empty(self):
        assert len(NAICS_DOMAIN_LINKS) > 0

    def test_links_to_crop_domain(self):
        domain_systems = [t[3] for t in NAICS_DOMAIN_LINKS]
        assert "domain_ag_crop" in domain_systems

    def test_links_to_livestock_domain(self):
        domain_systems = [t[3] for t in NAICS_DOMAIN_LINKS]
        assert "domain_ag_livestock" in domain_systems

    def test_links_to_method_domain(self):
        domain_systems = [t[3] for t in NAICS_DOMAIN_LINKS]
        assert "domain_ag_method" in domain_systems

    def test_links_to_grade_domain(self):
        domain_systems = [t[3] for t in NAICS_DOMAIN_LINKS]
        assert "domain_ag_grade" in domain_systems

    def test_all_naics_codes_start_with_11(self):
        for naics_code, _, _, _ in NAICS_DOMAIN_LINKS:
            assert naics_code.startswith("11"), f"Expected 11xxx, got '{naics_code}'"

    def test_naics_system_is_naics_2022(self):
        for _, naics_sys, _, _ in NAICS_DOMAIN_LINKS:
            assert naics_sys == "naics_2022"

    def test_has_crop_production_link(self):
        naics_codes = [t[0] for t in NAICS_DOMAIN_LINKS]
        assert "111" in naics_codes

    def test_has_animal_production_link(self):
        naics_codes = [t[0] for t in NAICS_DOMAIN_LINKS]
        assert "112" in naics_codes

    def test_tuples_have_four_elements(self):
        for t in NAICS_DOMAIN_LINKS:
            assert len(t) == 4, f"Expected 4-tuple, got {len(t)}-tuple: {t}"


def test_crosswalk_naics11_domains_module_importable():
    assert callable(ingest_crosswalk_naics11_domains)
    assert isinstance(NAICS_DOMAIN_LINKS, list)


def test_ingest_crosswalk_naics11_domains(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        from world_of_taxanomy.ingest.domain_ag_crop import ingest_domain_ag_crop
        from world_of_taxanomy.ingest.domain_ag_livestock import ingest_domain_ag_livestock
        from world_of_taxanomy.ingest.domain_ag_method import ingest_domain_ag_method
        from world_of_taxanomy.ingest.domain_ag_grade import ingest_domain_ag_grade
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            await ingest_domain_ag_crop(conn)
            await ingest_domain_ag_livestock(conn)
            await ingest_domain_ag_method(conn)
            await ingest_domain_ag_grade(conn)
            count = await ingest_crosswalk_naics11_domains(conn)
            assert count > 0
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_crosswalk_naics11_domains_idempotent(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        from world_of_taxanomy.ingest.domain_ag_crop import ingest_domain_ag_crop
        from world_of_taxanomy.ingest.domain_ag_livestock import ingest_domain_ag_livestock
        from world_of_taxanomy.ingest.domain_ag_method import ingest_domain_ag_method
        from world_of_taxanomy.ingest.domain_ag_grade import ingest_domain_ag_grade
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            await ingest_domain_ag_crop(conn)
            await ingest_domain_ag_livestock(conn)
            await ingest_domain_ag_method(conn)
            await ingest_domain_ag_grade(conn)
            count1 = await ingest_crosswalk_naics11_domains(conn)
            count2 = await ingest_crosswalk_naics11_domains(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
