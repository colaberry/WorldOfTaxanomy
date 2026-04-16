"""Tests for SCIAN 2018 (Mexico) ingester.

Covers unit tests for sector data and integration test with the database.
SCIAN - Sistema de Clasificacion Industrial de America del Norte (Mexican NAICS).
2018 edition published by INEGI.
SCIAN is the Spanish-language version of NAICS, co-developed by Mexico, Canada, and USA.
"""

import asyncio
import pytest

from world_of_taxonomy.ingest.scian_2018 import (
    SCIAN_SECTORS,
    SCIAN_TO_NAICS_MAPPING,
    ingest_scian_2018,
)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# -- Unit tests: SCIAN_SECTORS ------------------------------------------------


class TestScianSectors:
    def test_at_least_20_sectors(self):
        """SCIAN has 20 two-digit NAICS-equivalent sectors."""
        assert len(SCIAN_SECTORS) >= 20

    def test_all_codes_are_two_digits(self):
        for code in SCIAN_SECTORS:
            assert len(code) == 2 and code.isdigit(), f"Code '{code}' should be 2-digit numeric"

    def test_all_titles_non_empty(self):
        for code, title in SCIAN_SECTORS.items():
            assert len(title) > 0, f"Sector {code} has empty title"

    def test_agriculture_sector_11_exists(self):
        assert "11" in SCIAN_SECTORS
        title = SCIAN_SECTORS["11"]
        assert "Agr" in title or "agr" in title

    def test_manufacturing_sector_31_or_33_exists(self):
        assert "31" in SCIAN_SECTORS or "33" in SCIAN_SECTORS

    def test_no_duplicate_titles(self):
        titles = list(SCIAN_SECTORS.values())
        assert len(titles) == len(set(titles))


# -- Unit tests: SCIAN_TO_NAICS_MAPPING --------------------------------------


class TestScianToNaicsMapping:
    def test_mapping_keys_are_valid_sectors(self):
        for code in SCIAN_TO_NAICS_MAPPING:
            assert code in SCIAN_SECTORS, f"Mapping key '{code}' not a valid SCIAN sector"

    def test_at_least_20_sectors_mapped(self):
        assert len(SCIAN_TO_NAICS_MAPPING) >= 20

    def test_match_types_are_valid(self):
        valid = {"exact", "broad", "narrow", "related"}
        for scian_code, targets in SCIAN_TO_NAICS_MAPPING.items():
            for naics_code, match_type in targets:
                assert match_type in valid

    def test_naics_codes_are_two_digits(self):
        for scian_code, targets in SCIAN_TO_NAICS_MAPPING.items():
            for naics_code, _ in targets:
                assert len(naics_code) == 2 and naics_code.isdigit(), (
                    f"NAICS code '{naics_code}' should be 2 digits"
                )

    def test_sector_11_maps_to_naics_11(self):
        if "11" in SCIAN_TO_NAICS_MAPPING:
            targets = SCIAN_TO_NAICS_MAPPING["11"]
            naics_codes = [t[0] for t in targets]
            assert "11" in naics_codes


# -- Integration test ---------------------------------------------------------


def test_ingest_scian_2018(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM equivalence WHERE source_system = 'scian_2018' OR target_system = 'scian_2018'"
            )
            await conn.execute(
                "DELETE FROM classification_node WHERE system_id = 'scian_2018'"
            )
            await conn.execute(
                "DELETE FROM classification_system WHERE id = 'scian_2018'"
            )

            count = await ingest_scian_2018(conn)

            assert count >= 20

            row = await conn.fetchrow(
                "SELECT * FROM classification_system WHERE id = 'scian_2018'"
            )
            assert row is not None
            assert row["name"] == "SCIAN 2018"
            assert row["node_count"] == count
            assert row["region"] == "Mexico"

            nodes = await conn.fetch(
                "SELECT code FROM classification_node WHERE system_id = 'scian_2018' AND level = 0"
            )
            assert len(nodes) >= 20

            forward_edges = await conn.fetch(
                "SELECT * FROM equivalence WHERE source_system = 'scian_2018' AND target_system = 'naics_2022'"
            )
            assert len(forward_edges) > 0

            reverse_edges = await conn.fetch(
                "SELECT * FROM equivalence WHERE source_system = 'naics_2022' AND target_system = 'scian_2018'"
            )
            assert len(reverse_edges) > 0

            assert len(forward_edges) == len(reverse_edges)

    _run(_test())


def test_ingest_scian_2018_idempotent(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM equivalence WHERE source_system = 'scian_2018' OR target_system = 'scian_2018'"
            )
            await conn.execute(
                "DELETE FROM classification_node WHERE system_id = 'scian_2018'"
            )
            await conn.execute(
                "DELETE FROM classification_system WHERE id = 'scian_2018'"
            )

            count1 = await ingest_scian_2018(conn)
            count2 = await ingest_scian_2018(conn)

            assert count1 == count2

            node_count = await conn.fetchval(
                "SELECT COUNT(*) FROM classification_node WHERE system_id = 'scian_2018'"
            )
            assert node_count == count1

    _run(_test())
