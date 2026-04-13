"""Tests for KBLI 2020 (Indonesia) ingester.

Covers unit tests for section data and integration test with the database.
KBLI - Klasifikasi Baku Lapangan Usaha Indonesia, edition 2020.
Published by BPS (Badan Pusat Statistik - Statistics Indonesia).
Aligned to ISIC Rev 4.
"""

import asyncio
import pytest

from world_of_taxanomy.ingest.kbli_2020 import (
    KBLI_SECTIONS,
    KBLI_TO_ISIC_MAPPING,
    ingest_kbli_2020,
)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# -- Unit tests: KBLI_SECTIONS ------------------------------------------------


class TestKbliSections:
    def test_at_least_21_sections(self):
        """KBLI 2020 follows ISIC Rev 4 with 21 sections (A-U)."""
        assert len(KBLI_SECTIONS) >= 21

    def test_sections_include_a_through_u(self):
        expected = set("ABCDEFGHIJKLMNOPQRSTU")
        assert expected.issubset(set(KBLI_SECTIONS.keys()))

    def test_all_titles_non_empty(self):
        for code, title in KBLI_SECTIONS.items():
            assert len(title) > 0, f"Section {code} has empty title"

    def test_agriculture_section_exists(self):
        assert "A" in KBLI_SECTIONS
        assert len(KBLI_SECTIONS["A"]) > 0

    def test_manufacturing_section_exists(self):
        assert "C" in KBLI_SECTIONS
        assert len(KBLI_SECTIONS["C"]) > 0

    def test_no_duplicate_titles(self):
        titles = list(KBLI_SECTIONS.values())
        assert len(titles) == len(set(titles))


# -- Unit tests: KBLI_TO_ISIC_MAPPING ----------------------------------------


class TestKbliToIsicMapping:
    def test_mapping_keys_are_valid_sections(self):
        for code in KBLI_TO_ISIC_MAPPING:
            assert code in KBLI_SECTIONS, f"Mapping key '{code}' not a valid KBLI section"

    def test_at_least_21_sections_mapped(self):
        """KBLI is ISIC Rev 4 aligned so all sections should map."""
        assert len(KBLI_TO_ISIC_MAPPING) >= 21

    def test_match_types_are_valid(self):
        valid = {"exact", "broad", "narrow", "related"}
        for kbli_code, targets in KBLI_TO_ISIC_MAPPING.items():
            for isic_code, match_type in targets:
                assert match_type in valid

    def test_isic_codes_are_single_letters(self):
        for kbli_code, targets in KBLI_TO_ISIC_MAPPING.items():
            for isic_code, _ in targets:
                assert isic_code.isupper() and len(isic_code) == 1

    def test_section_a_maps_to_isic_a(self):
        targets = KBLI_TO_ISIC_MAPPING["A"]
        isic_codes = [t[0] for t in targets]
        assert "A" in isic_codes


# -- Integration test ---------------------------------------------------------


def test_ingest_kbli_2020(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM equivalence WHERE source_system = 'kbli_2020' OR target_system = 'kbli_2020'"
            )
            await conn.execute(
                "DELETE FROM classification_node WHERE system_id = 'kbli_2020'"
            )
            await conn.execute(
                "DELETE FROM classification_system WHERE id = 'kbli_2020'"
            )

            count = await ingest_kbli_2020(conn)

            assert count >= 21

            row = await conn.fetchrow(
                "SELECT * FROM classification_system WHERE id = 'kbli_2020'"
            )
            assert row is not None
            assert row["name"] == "KBLI 2020"
            assert row["node_count"] == count
            assert row["region"] == "Indonesia"

            nodes = await conn.fetch(
                "SELECT code FROM classification_node WHERE system_id = 'kbli_2020' AND level = 0"
            )
            assert len(nodes) >= 21

            node_a = await conn.fetchrow(
                "SELECT * FROM classification_node WHERE system_id = 'kbli_2020' AND code = 'A'"
            )
            assert node_a is not None
            assert node_a["level"] == 0
            assert node_a["parent_code"] is None
            assert node_a["is_leaf"] is True

            forward_edges = await conn.fetch(
                "SELECT * FROM equivalence WHERE source_system = 'kbli_2020' AND target_system = 'isic_rev4'"
            )
            assert len(forward_edges) > 0

            reverse_edges = await conn.fetch(
                "SELECT * FROM equivalence WHERE source_system = 'isic_rev4' AND target_system = 'kbli_2020'"
            )
            assert len(reverse_edges) > 0

            assert len(forward_edges) == len(reverse_edges)

    _run(_test())


def test_ingest_kbli_2020_idempotent(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM equivalence WHERE source_system = 'kbli_2020' OR target_system = 'kbli_2020'"
            )
            await conn.execute(
                "DELETE FROM classification_node WHERE system_id = 'kbli_2020'"
            )
            await conn.execute(
                "DELETE FROM classification_system WHERE id = 'kbli_2020'"
            )

            count1 = await ingest_kbli_2020(conn)
            count2 = await ingest_kbli_2020(conn)

            assert count1 == count2

            node_count = await conn.fetchval(
                "SELECT COUNT(*) FROM classification_node WHERE system_id = 'kbli_2020'"
            )
            assert node_count == count1

    _run(_test())
