"""Tests for SIC-SA (South Africa) ingester.

Covers unit tests for section data and integration test with the database.
SIC-SA - Standard Industrial Classification for South Africa, version 5 (2012).
Published by Statistics South Africa (Stats SA).
Aligned to ISIC Rev 4.
"""

import asyncio
import pytest

from world_of_taxonomy.ingest.sic_sa import (
    SIC_SA_SECTIONS,
    SIC_SA_TO_ISIC_MAPPING,
    ingest_sic_sa,
)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# -- Unit tests: SIC_SA_SECTIONS ----------------------------------------------


class TestSicSaSections:
    def test_at_least_19_sections(self):
        """SIC-SA has at least 19 sections aligned to ISIC Rev 4."""
        assert len(SIC_SA_SECTIONS) >= 19

    def test_all_codes_non_empty(self):
        for code in SIC_SA_SECTIONS:
            assert len(code) > 0

    def test_all_titles_non_empty(self):
        for code, title in SIC_SA_SECTIONS.items():
            assert len(title) > 0, f"Section {code} has empty title"

    def test_agriculture_section_exists(self):
        titles = list(SIC_SA_SECTIONS.values())
        assert any("Agriculture" in t or "Farming" in t or "Forestry" in t for t in titles)

    def test_manufacturing_section_exists(self):
        titles = list(SIC_SA_SECTIONS.values())
        assert any("Manufacturing" in t for t in titles)

    def test_mining_section_exists(self):
        titles = list(SIC_SA_SECTIONS.values())
        assert any("Mining" in t for t in titles)

    def test_no_duplicate_titles(self):
        titles = list(SIC_SA_SECTIONS.values())
        assert len(titles) == len(set(titles))


# -- Unit tests: SIC_SA_TO_ISIC_MAPPING --------------------------------------


class TestSicSaToIsicMapping:
    def test_mapping_keys_are_valid_sections(self):
        for code in SIC_SA_TO_ISIC_MAPPING:
            assert code in SIC_SA_SECTIONS, f"Mapping key '{code}' not a valid SIC-SA section"

    def test_at_least_15_sections_mapped(self):
        assert len(SIC_SA_TO_ISIC_MAPPING) >= 15

    def test_match_types_are_valid(self):
        valid = {"exact", "broad", "narrow", "related"}
        for sa_code, targets in SIC_SA_TO_ISIC_MAPPING.items():
            for isic_code, match_type in targets:
                assert match_type in valid

    def test_isic_codes_are_single_uppercase_letters(self):
        for sa_code, targets in SIC_SA_TO_ISIC_MAPPING.items():
            for isic_code, _ in targets:
                assert isic_code.isupper() and len(isic_code) == 1, (
                    f"ISIC code '{isic_code}' should be a single uppercase letter"
                )


# -- Integration test ---------------------------------------------------------


def test_ingest_sic_sa(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM equivalence WHERE source_system = 'sic_sa' OR target_system = 'sic_sa'"
            )
            await conn.execute(
                "DELETE FROM classification_node WHERE system_id = 'sic_sa'"
            )
            await conn.execute(
                "DELETE FROM classification_system WHERE id = 'sic_sa'"
            )

            count = await ingest_sic_sa(conn)

            assert count >= 19

            row = await conn.fetchrow(
                "SELECT * FROM classification_system WHERE id = 'sic_sa'"
            )
            assert row is not None
            assert row["name"] == "SIC-SA"
            assert row["node_count"] == count
            assert row["region"] == "South Africa"

            nodes = await conn.fetch(
                "SELECT code FROM classification_node WHERE system_id = 'sic_sa' AND level = 0"
            )
            assert len(nodes) >= 19

            forward_edges = await conn.fetch(
                "SELECT * FROM equivalence WHERE source_system = 'sic_sa' AND target_system = 'isic_rev4'"
            )
            assert len(forward_edges) > 0

            reverse_edges = await conn.fetch(
                "SELECT * FROM equivalence WHERE source_system = 'isic_rev4' AND target_system = 'sic_sa'"
            )
            assert len(reverse_edges) > 0

            assert len(forward_edges) == len(reverse_edges)

    _run(_test())


def test_ingest_sic_sa_idempotent(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM equivalence WHERE source_system = 'sic_sa' OR target_system = 'sic_sa'"
            )
            await conn.execute(
                "DELETE FROM classification_node WHERE system_id = 'sic_sa'"
            )
            await conn.execute(
                "DELETE FROM classification_system WHERE id = 'sic_sa'"
            )

            count1 = await ingest_sic_sa(conn)
            count2 = await ingest_sic_sa(conn)

            assert count1 == count2

            node_count = await conn.fetchval(
                "SELECT COUNT(*) FROM classification_node WHERE system_id = 'sic_sa'"
            )
            assert node_count == count1

    _run(_test())
