"""Tests for Aviation Service domain taxonomy ingester.

Aviation Service taxonomy organizes aviation service sector types:
  Commercial Airlines (das_airline*) - full-service, low-cost, regional
  Cargo Aviation      (das_cargo*)   - integrator, freighter, belly, cold chain
  General Aviation    (das_general*) - private, flight training, aerial, agri
  Airport Operations  (das_airport*) - ground handling, terminal, ATC, fuel
  Aircraft MRO        (das_mro*)     - airframe, engine, component, line

Source: NAICS 4811/4812 aviation service structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_aviation_service import (
    NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_aviation_service,
)


class TestDetermineLevel:
    def test_airline_category_is_level_1(self):
        assert _determine_level("das_airline") == 1

    def test_airline_type_is_level_2(self):
        assert _determine_level("das_airline_full") == 2

    def test_cargo_category_is_level_1(self):
        assert _determine_level("das_cargo") == 1

    def test_cargo_type_is_level_2(self):
        assert _determine_level("das_cargo_integrator") == 2

    def test_mro_category_is_level_1(self):
        assert _determine_level("das_mro") == 1

    def test_mro_type_is_level_2(self):
        assert _determine_level("das_mro_engine") == 2


class TestDetermineParent:
    def test_airline_category_has_no_parent(self):
        assert _determine_parent("das_airline") is None

    def test_airline_full_parent_is_airline(self):
        assert _determine_parent("das_airline_full") == "das_airline"

    def test_cargo_integrator_parent_is_cargo(self):
        assert _determine_parent("das_cargo_integrator") == "das_cargo"

    def test_mro_engine_parent_is_mro(self):
        assert _determine_parent("das_mro_engine") == "das_mro"


class TestNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(NODES) > 0

    def test_minimum_node_count(self):
        assert len(NODES) >= 20

    def test_has_airline_category(self):
        codes = [n[0] for n in NODES]
        assert "das_airline" in codes

    def test_has_cargo_category(self):
        codes = [n[0] for n in NODES]
        assert "das_cargo" in codes

    def test_has_mro_category(self):
        codes = [n[0] for n in NODES]
        assert "das_mro" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in NODES:
            if level == 2:
                assert parent is not None

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_aviation_service_module_importable():
    assert callable(ingest_domain_aviation_service)
    assert isinstance(NODES, list)


def test_ingest_domain_aviation_service(db_pool):
    """Integration test: aviation service taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_aviation_service(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_aviation_service'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_aviation_service_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_aviation_service(conn)
            count2 = await ingest_domain_aviation_service(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
