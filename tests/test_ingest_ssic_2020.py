"""Tests for Singapore Standard Industrial Classification 2020."""
import asyncio
import pytest
from world_of_taxonomy.ingest.ssic_2020 import (
    SECTIONS,
    SECTION_TO_ISIC,
    ingest_ssic_2020,
)

def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)

class TestSections:
    def test_has_21_sections(self):
        assert len(SECTIONS) == 21

    def test_sections_run_a_through_u(self):
        assert set(SECTIONS.keys()) == set("ABCDEFGHIJKLMNOPQRSTU")

    def test_all_titles_non_empty(self):
        for code, title in SECTIONS.items():
            assert len(title) > 0

    def test_section_a_title(self):
        assert SECTIONS["A"].startswith("Agriculture, Forestr")

class TestSectionToIsic:
    def test_all_sections_mapped(self):
        assert set(SECTION_TO_ISIC.keys()) == set(SECTIONS.keys())

    def test_isic_targets_are_valid_sections(self):
        valid = set("ABCDEFGHIJKLMNOPQRSTU")
        for src, tgt in SECTION_TO_ISIC.items():
            assert tgt in valid, f"{tgt} is not a valid ISIC section"

def test_ssic_2020_importable():
    assert callable(ingest_ssic_2020)

def test_ingest_ssic_2020(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM equivalence WHERE source_system=$1 OR target_system=$1", "ssic_2020")
            await conn.execute("DELETE FROM classification_node WHERE system_id=$1", "ssic_2020")
            await conn.execute("DELETE FROM classification_system WHERE id=$1", "ssic_2020")
            count = await ingest_ssic_2020(conn)
            assert count == 21
            row = await conn.fetchrow("SELECT node_count, region FROM classification_system WHERE id=$1", "ssic_2020")
            assert row is not None
            assert row["node_count"] == 21
            assert row["region"] == "Singapore"
    _run(_test())

def test_ingest_ssic_2020_idempotent(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM equivalence WHERE source_system=$1 OR target_system=$1", "ssic_2020")
            await conn.execute("DELETE FROM classification_node WHERE system_id=$1", "ssic_2020")
            await conn.execute("DELETE FROM classification_system WHERE id=$1", "ssic_2020")
            c1 = await ingest_ssic_2020(conn)
            c2 = await ingest_ssic_2020(conn)
            assert c1 == c2 == 21
    _run(_test())
