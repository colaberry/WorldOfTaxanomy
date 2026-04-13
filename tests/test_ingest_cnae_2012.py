"""Tests for CNAE 2.0 (Brazil) ingester.

Covers unit tests for section data and integration test with the database.
CNAE - Classificacao Nacional de Atividades Economicas, version 2.0 (2007).
"""

import asyncio
import pytest

from world_of_taxanomy.ingest.cnae_2012 import (
    CNAE_SECTIONS,
    CNAE_TO_ISIC_MAPPING,
    ingest_cnae_2012,
)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# -- Unit tests: CNAE_SECTIONS ------------------------------------------------


class TestCnaeSections:
    def test_at_least_21_sections(self):
        """CNAE 2.0 has 21 sections (A-U)."""
        assert len(CNAE_SECTIONS) >= 21

    def test_sections_include_a_through_u(self):
        expected = set("ABCDEFGHIJKLMNOPQRSTU")
        assert expected.issubset(set(CNAE_SECTIONS.keys()))

    def test_all_titles_non_empty(self):
        for code, title in CNAE_SECTIONS.items():
            assert len(title) > 0, f"Section {code} has empty title"

    def test_specific_section_titles(self):
        assert "Agriculture" in CNAE_SECTIONS["A"] or "Agricultura" in CNAE_SECTIONS["A"]
        assert "Manufacturing" in CNAE_SECTIONS["C"] or "Industrias" in CNAE_SECTIONS["C"] or "Manufactur" in CNAE_SECTIONS["C"]

    def test_no_duplicate_titles(self):
        titles = list(CNAE_SECTIONS.values())
        assert len(titles) == len(set(titles))


# -- Unit tests: CNAE_TO_ISIC_MAPPING ----------------------------------------


class TestCnaeToIsicMapping:
    def test_mapping_keys_are_valid_sections(self):
        for code in CNAE_TO_ISIC_MAPPING:
            assert code in CNAE_SECTIONS, f"{code} not a valid CNAE section"

    def test_at_least_20_mapped_sections(self):
        assert len(CNAE_TO_ISIC_MAPPING) >= 20

    def test_match_types_are_valid(self):
        valid = {"exact", "broad", "narrow", "related"}
        for cnae_code, targets in CNAE_TO_ISIC_MAPPING.items():
            for isic_code, match_type in targets:
                assert match_type in valid, (
                    f"CNAE {cnae_code} -> ISIC {isic_code}: invalid match_type '{match_type}'"
                )

    def test_isic_codes_are_uppercase_letters(self):
        for cnae_code, targets in CNAE_TO_ISIC_MAPPING.items():
            for isic_code, _ in targets:
                assert isic_code.isupper() and len(isic_code) == 1, (
                    f"ISIC code '{isic_code}' should be a single uppercase letter"
                )


# -- Integration test ---------------------------------------------------------


def test_ingest_cnae_2012(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM equivalence WHERE source_system = 'cnae_2012' OR target_system = 'cnae_2012'"
            )
            await conn.execute(
                "DELETE FROM classification_node WHERE system_id = 'cnae_2012'"
            )
            await conn.execute(
                "DELETE FROM classification_system WHERE id = 'cnae_2012'"
            )

            count = await ingest_cnae_2012(conn)

            assert count >= 21

            row = await conn.fetchrow(
                "SELECT * FROM classification_system WHERE id = 'cnae_2012'"
            )
            assert row is not None
            assert row["name"] == "CNAE 2.0"
            assert row["node_count"] == count
            assert row["region"] == "Brazil"

            nodes = await conn.fetch(
                "SELECT code FROM classification_node WHERE system_id = 'cnae_2012' AND level = 0"
            )
            assert len(nodes) >= 21

            node_a = await conn.fetchrow(
                "SELECT * FROM classification_node WHERE system_id = 'cnae_2012' AND code = 'A'"
            )
            assert node_a is not None
            assert node_a["level"] == 0
            assert node_a["parent_code"] is None
            assert node_a["is_leaf"] is True

            forward_edges = await conn.fetch(
                "SELECT * FROM equivalence WHERE source_system = 'cnae_2012' AND target_system = 'isic_rev4'"
            )
            assert len(forward_edges) > 0

            reverse_edges = await conn.fetch(
                "SELECT * FROM equivalence WHERE source_system = 'isic_rev4' AND target_system = 'cnae_2012'"
            )
            assert len(reverse_edges) > 0

            assert len(forward_edges) == len(reverse_edges)

    _run(_test())


def test_ingest_cnae_2012_idempotent(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM equivalence WHERE source_system = 'cnae_2012' OR target_system = 'cnae_2012'"
            )
            await conn.execute(
                "DELETE FROM classification_node WHERE system_id = 'cnae_2012'"
            )
            await conn.execute(
                "DELETE FROM classification_system WHERE id = 'cnae_2012'"
            )

            count1 = await ingest_cnae_2012(conn)
            count2 = await ingest_cnae_2012(conn)

            assert count1 == count2

            node_count = await conn.fetchval(
                "SELECT COUNT(*) FROM classification_node WHERE system_id = 'cnae_2012'"
            )
            assert node_count == count1

    _run(_test())
