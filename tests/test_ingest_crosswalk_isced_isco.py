"""Tests for ISCED 2011 / ISCO-08 crosswalk (Phase 4-B).

Maps ISCED 2011 education levels to ISCO-08 major groups that
typically require those education levels.
~25 hand-coded edges.
"""
from __future__ import annotations

import asyncio
import pytest
from world_of_taxanomy.ingest.crosswalk_isced_isco import (
    ISCED_ISCO_EDGES,
    ingest_crosswalk_isced_isco,
)


class TestIsced2011IscoEdges:
    def test_edges_is_list(self):
        assert isinstance(ISCED_ISCO_EDGES, list)

    def test_at_least_20_edges(self):
        assert len(ISCED_ISCO_EDGES) >= 20

    def test_each_edge_is_four_tuple(self):
        for edge in ISCED_ISCO_EDGES:
            assert len(edge) == 4, f"Expected 4-tuple, got {len(edge)}: {edge}"

    def test_isced_codes_start_with_isced(self):
        for isced_code, _isco_code, _match_type, _note in ISCED_ISCO_EDGES:
            assert isced_code.startswith("ISCED"), f"Bad ISCED code: {isced_code}"

    def test_isco_codes_are_digits(self):
        for _isced_code, isco_code, _match_type, _note in ISCED_ISCO_EDGES:
            assert isco_code.isdigit(), f"ISCO code should be numeric: {isco_code}"

    def test_match_types_are_valid(self):
        valid = {"exact", "narrow", "broad", "partial"}
        for _isced, _isco, match_type, _note in ISCED_ISCO_EDGES:
            assert match_type in valid, f"Invalid match_type: {match_type}"

    def test_doctoral_level_links_to_professional_occupations(self):
        """ISCED8 (Doctoral) should link to ISCO major group 2 (Professionals)."""
        doctoral_edges = [e for e in ISCED_ISCO_EDGES if e[0] == "ISCED8"]
        isco_codes = {e[1] for e in doctoral_edges}
        assert "2" in isco_codes, "ISCED8 should link to ISCO major group 2"

    def test_no_em_dashes_in_notes(self):
        for _isced, _isco, _mt, note in ISCED_ISCO_EDGES:
            if note:
                assert "\u2014" not in note, f"Em-dash in note: {note}"


def test_ingest_crosswalk_isced_isco(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            # Seed prerequisite systems
            from world_of_taxanomy.ingest.isced_2011 import ingest_isced_2011
            from world_of_taxanomy.ingest.isco_08 import ingest_isco_08
            await ingest_isced_2011(conn)
            await ingest_isco_08(conn)

            count = await ingest_crosswalk_isced_isco(conn)
            assert count >= 20

            # Verify edges exist in equivalence table
            rows = await conn.fetch(
                """SELECT * FROM equivalence
                   WHERE system_id_1 = 'isced_2011' AND system_id_2 = 'isco_08'
                   LIMIT 5"""
            )
            assert len(rows) > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_crosswalk_isced_isco_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            from world_of_taxanomy.ingest.isced_2011 import ingest_isced_2011
            from world_of_taxanomy.ingest.isco_08 import ingest_isco_08
            await ingest_isced_2011(conn)
            await ingest_isco_08(conn)

            count1 = await ingest_crosswalk_isced_isco(conn)
            count2 = await ingest_crosswalk_isced_isco(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
