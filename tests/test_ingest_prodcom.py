"""Tests for PRODCOM ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.prodcom import (
    PRODCOM_SECTIONS,
    PRODCOM_DIVISIONS,
    ingest_prodcom,
)


class TestProdcomSections:
    def test_non_empty(self):
        assert len(PRODCOM_SECTIONS) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in PRODCOM_SECTIONS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for code, title in PRODCOM_SECTIONS:
            assert title.strip()

    def test_no_em_dashes(self):
        for code, title in PRODCOM_SECTIONS:
            assert "\u2014" not in title

    def test_has_manufacturing_section(self):
        codes = [n[0] for n in PRODCOM_SECTIONS]
        assert "C" in codes


class TestProdcomDivisions:
    def test_non_empty(self):
        assert len(PRODCOM_DIVISIONS) > 0

    def test_minimum_divisions(self):
        assert len(PRODCOM_DIVISIONS) >= 30

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in PRODCOM_DIVISIONS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for code, title, parent in PRODCOM_DIVISIONS:
            assert title.strip()

    def test_no_em_dashes(self):
        for code, title, parent in PRODCOM_DIVISIONS:
            assert "\u2014" not in title

    def test_parents_are_sections(self):
        section_codes = {n[0] for n in PRODCOM_SECTIONS}
        for code, title, parent in PRODCOM_DIVISIONS:
            assert parent in section_codes


def test_prodcom_module_importable():
    assert callable(ingest_prodcom)
    assert isinstance(PRODCOM_SECTIONS, list)
    assert isinstance(PRODCOM_DIVISIONS, list)


def test_ingest_prodcom(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_prodcom(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'prodcom'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_prodcom_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_prodcom(conn)
            count2 = await ingest_prodcom(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
