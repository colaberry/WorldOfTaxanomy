"""Tests for CSIC 2017 (China) ingester.

Covers unit tests for section data and integration test with the database.
CSIC - Chinese Standard Industrial Classification (GB/T 4754-2017).
"""

import asyncio
import pytest

from world_of_taxonomy.ingest.csic_2017 import (
    CSIC_SECTIONS,
    CSIC_TO_ISIC_MAPPING,
    ingest_csic_2017,
)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# -- Unit tests: CSIC_SECTIONS ------------------------------------------------


class TestCsicSections:
    def test_at_least_19_sections(self):
        """CSIC has at least 19 top-level sections."""
        assert len(CSIC_SECTIONS) >= 19

    def test_all_codes_non_empty(self):
        for code in CSIC_SECTIONS:
            assert len(code) > 0

    def test_all_titles_non_empty(self):
        for code, title in CSIC_SECTIONS.items():
            assert len(title) > 0, f"Section {code} has empty title"

    def test_agriculture_section_exists(self):
        titles = list(CSIC_SECTIONS.values())
        assert any("Agriculture" in t or "Farming" in t or "Forestry" in t for t in titles)

    def test_manufacturing_section_exists(self):
        titles = list(CSIC_SECTIONS.values())
        assert any("Manufacturing" in t or "Industry" in t or "Manufactur" in t for t in titles)

    def test_no_duplicate_titles(self):
        titles = list(CSIC_SECTIONS.values())
        assert len(titles) == len(set(titles))


# -- Unit tests: CSIC_TO_ISIC_MAPPING ----------------------------------------


class TestCsicToIsicMapping:
    def test_mapping_keys_are_valid_sections(self):
        for code in CSIC_TO_ISIC_MAPPING:
            assert code in CSIC_SECTIONS, f"Mapping key '{code}' is not a valid CSIC section"

    def test_at_least_15_mapped_sections(self):
        assert len(CSIC_TO_ISIC_MAPPING) >= 15

    def test_match_types_are_valid(self):
        valid = {"exact", "broad", "narrow", "related"}
        for csic_code, targets in CSIC_TO_ISIC_MAPPING.items():
            for isic_code, match_type in targets:
                assert match_type in valid, (
                    f"CSIC {csic_code} -> ISIC {isic_code}: invalid match_type '{match_type}'"
                )

    def test_isic_codes_are_single_letters(self):
        for csic_code, targets in CSIC_TO_ISIC_MAPPING.items():
            for isic_code, _ in targets:
                assert isic_code.isupper() and len(isic_code) == 1, (
                    f"ISIC code '{isic_code}' should be a single uppercase letter"
                )


# -- Integration test ---------------------------------------------------------


def test_ingest_csic_2017(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM equivalence WHERE source_system = 'csic_2017' OR target_system = 'csic_2017'"
            )
            await conn.execute(
                "DELETE FROM classification_node WHERE system_id = 'csic_2017'"
            )
            await conn.execute(
                "DELETE FROM classification_system WHERE id = 'csic_2017'"
            )

            count = await ingest_csic_2017(conn)

            assert count >= 19

            row = await conn.fetchrow(
                "SELECT * FROM classification_system WHERE id = 'csic_2017'"
            )
            assert row is not None
            assert row["name"] == "CSIC 2017"
            assert row["node_count"] == count
            assert row["region"] == "China"

            nodes = await conn.fetch(
                "SELECT code FROM classification_node WHERE system_id = 'csic_2017' AND level = 0"
            )
            assert len(nodes) >= 19

            forward_edges = await conn.fetch(
                "SELECT * FROM equivalence WHERE source_system = 'csic_2017' AND target_system = 'isic_rev4'"
            )
            assert len(forward_edges) > 0

            reverse_edges = await conn.fetch(
                "SELECT * FROM equivalence WHERE source_system = 'isic_rev4' AND target_system = 'csic_2017'"
            )
            assert len(reverse_edges) > 0

            assert len(forward_edges) == len(reverse_edges)

    _run(_test())


def test_ingest_csic_2017_idempotent(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM equivalence WHERE source_system = 'csic_2017' OR target_system = 'csic_2017'"
            )
            await conn.execute(
                "DELETE FROM classification_node WHERE system_id = 'csic_2017'"
            )
            await conn.execute(
                "DELETE FROM classification_system WHERE id = 'csic_2017'"
            )

            count1 = await ingest_csic_2017(conn)
            count2 = await ingest_csic_2017(conn)

            assert count1 == count2

            node_count = await conn.fetchval(
                "SELECT COUNT(*) FROM classification_node WHERE system_id = 'csic_2017'"
            )
            assert node_count == count1

    _run(_test())
