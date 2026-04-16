"""Tests for EU NUTS 2021 ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.eu_nuts_2021 import (
    NUTS_COUNTRIES,
    NUTS1,
    ingest_eu_nuts_2021,
)


class TestNutsCountries:
    def test_non_empty(self):
        assert len(NUTS_COUNTRIES) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in NUTS_COUNTRIES]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for code, title in NUTS_COUNTRIES:
            assert title.strip()

    def test_no_em_dashes(self):
        for code, title in NUTS_COUNTRIES:
            assert "\u2014" not in title

    def test_has_de_germany(self):
        codes = [n[0] for n in NUTS_COUNTRIES]
        assert "DE" in codes

    def test_has_fr_france(self):
        codes = [n[0] for n in NUTS_COUNTRIES]
        assert "FR" in codes

    def test_minimum_countries(self):
        assert len(NUTS_COUNTRIES) >= 27


class TestNuts1:
    def test_non_empty(self):
        assert len(NUTS1) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in NUTS1]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for code, title, parent in NUTS1:
            assert title.strip()

    def test_parents_are_countries(self):
        country_codes = {n[0] for n in NUTS_COUNTRIES}
        for code, title, parent in NUTS1:
            assert parent in country_codes

    def test_minimum_nuts1_regions(self):
        assert len(NUTS1) >= 80


def test_eu_nuts_2021_module_importable():
    assert callable(ingest_eu_nuts_2021)
    assert isinstance(NUTS_COUNTRIES, list)
    assert isinstance(NUTS1, list)


def test_ingest_eu_nuts_2021(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_eu_nuts_2021(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'eu_nuts_2021'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_eu_nuts_2021_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_eu_nuts_2021(conn)
            count2 = await ingest_eu_nuts_2021(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
