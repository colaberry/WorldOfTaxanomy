"""Tests for OKVED-2 (Russia) ingester.

Covers unit tests for section data and integration test with the database.
OKVED-2 - Russian All-Union Classifier of Economic Activities, edition 2 (2014).
Based on NACE Rev 2 / ISIC Rev 4 structure.
"""

import asyncio
import pytest

from world_of_taxonomy.ingest.okved_2 import (
    OKVED2_SECTIONS,
    OKVED2_TO_ISIC_MAPPING,
    ingest_okved_2,
)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# -- Unit tests: OKVED2_SECTIONS ----------------------------------------------


class TestOkved2Sections:
    def test_exactly_21_sections(self):
        """OKVED-2 mirrors NACE/ISIC with 21 sections (A-U)."""
        assert len(OKVED2_SECTIONS) == 21

    def test_sections_are_a_through_u(self):
        expected = set("ABCDEFGHIJKLMNOPQRSTU")
        assert set(OKVED2_SECTIONS.keys()) == expected

    def test_all_titles_non_empty(self):
        for code, title in OKVED2_SECTIONS.items():
            assert len(title) > 0, f"Section {code} has empty title"

    def test_specific_section_titles(self):
        assert "Agriculture" in OKVED2_SECTIONS["A"]
        assert "Mining" in OKVED2_SECTIONS["B"]
        assert "Manufacturing" in OKVED2_SECTIONS["C"]

    def test_no_duplicate_titles(self):
        titles = list(OKVED2_SECTIONS.values())
        assert len(titles) == len(set(titles))


# -- Unit tests: OKVED2_TO_ISIC_MAPPING --------------------------------------


class TestOkved2ToIsicMapping:
    def test_mapping_keys_are_valid_sections(self):
        for code in OKVED2_TO_ISIC_MAPPING:
            assert code in OKVED2_SECTIONS, f"Mapping key '{code}' is not a valid section"

    def test_at_least_21_sections_mapped(self):
        """OKVED-2 is based on NACE Rev 2 so all sections should map to ISIC."""
        assert len(OKVED2_TO_ISIC_MAPPING) == 21

    def test_match_types_are_valid(self):
        valid = {"exact", "broad", "narrow", "related"}
        for okved_code, targets in OKVED2_TO_ISIC_MAPPING.items():
            for isic_code, match_type in targets:
                assert match_type in valid

    def test_section_a_maps_to_isic_a(self):
        targets = OKVED2_TO_ISIC_MAPPING["A"]
        isic_codes = [t[0] for t in targets]
        assert "A" in isic_codes

    def test_section_c_maps_to_isic_c(self):
        targets = OKVED2_TO_ISIC_MAPPING["C"]
        isic_codes = [t[0] for t in targets]
        assert "C" in isic_codes


# -- Integration test ---------------------------------------------------------


def test_ingest_okved_2(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM equivalence WHERE source_system = 'okved_2' OR target_system = 'okved_2'"
            )
            await conn.execute(
                "DELETE FROM classification_node WHERE system_id = 'okved_2'"
            )
            await conn.execute(
                "DELETE FROM classification_system WHERE id = 'okved_2'"
            )

            count = await ingest_okved_2(conn)

            assert count == 21

            row = await conn.fetchrow(
                "SELECT * FROM classification_system WHERE id = 'okved_2'"
            )
            assert row is not None
            assert row["name"] == "OKVED-2"
            assert row["node_count"] == 21
            assert row["region"] == "Russia"

            nodes = await conn.fetch(
                "SELECT code FROM classification_node WHERE system_id = 'okved_2' AND level = 0"
            )
            assert len(nodes) == 21
            codes = {r["code"] for r in nodes}
            assert codes == set("ABCDEFGHIJKLMNOPQRSTU")

            node_a = await conn.fetchrow(
                "SELECT * FROM classification_node WHERE system_id = 'okved_2' AND code = 'A'"
            )
            assert node_a is not None
            assert node_a["level"] == 0
            assert node_a["parent_code"] is None
            assert node_a["is_leaf"] is True

            forward_edges = await conn.fetch(
                "SELECT * FROM equivalence WHERE source_system = 'okved_2' AND target_system = 'isic_rev4'"
            )
            assert len(forward_edges) == 21

            reverse_edges = await conn.fetch(
                "SELECT * FROM equivalence WHERE source_system = 'isic_rev4' AND target_system = 'okved_2'"
            )
            assert len(reverse_edges) == 21

    _run(_test())


def test_ingest_okved_2_idempotent(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM equivalence WHERE source_system = 'okved_2' OR target_system = 'okved_2'"
            )
            await conn.execute(
                "DELETE FROM classification_node WHERE system_id = 'okved_2'"
            )
            await conn.execute(
                "DELETE FROM classification_system WHERE id = 'okved_2'"
            )

            count1 = await ingest_okved_2(conn)
            count2 = await ingest_okved_2(conn)

            assert count1 == count2 == 21

            node_count = await conn.fetchval(
                "SELECT COUNT(*) FROM classification_node WHERE system_id = 'okved_2'"
            )
            assert node_count == 21

    _run(_test())
