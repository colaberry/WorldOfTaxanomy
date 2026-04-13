"""Tests for ISCO-08 / ISIC Rev 4 crosswalk (Phase 3-B).

Maps ISCO-08 major occupation groups to ISIC Rev 4 sections where
those occupations predominantly work. Hand-coded ~45 edges.
"""
from __future__ import annotations

import asyncio
import pytest
from world_of_taxanomy.ingest.crosswalk_isco_isic import (
    ISCO_ISIC_EDGES,
    ingest_crosswalk_isco_isic,
)


class TestIscoIsicEdges:
    def test_edges_is_list(self):
        assert isinstance(ISCO_ISIC_EDGES, list)

    def test_at_least_30_edges(self):
        assert len(ISCO_ISIC_EDGES) >= 30

    def test_each_edge_is_four_tuple(self):
        for edge in ISCO_ISIC_EDGES:
            assert len(edge) == 4, f"Expected 4-tuple, got {len(edge)}: {edge}"

    def test_isco_codes_are_single_digits(self):
        """ISCO major group codes are 1-digit (1-9 plus 0 for armed forces)."""
        for isco_code, _isic_code, _match_type, _note in ISCO_ISIC_EDGES:
            assert isco_code.isdigit(), f"ISCO code should be digit: {isco_code}"
            assert len(isco_code) == 1, f"ISCO major group should be 1-digit: {isco_code}"

    def test_isic_codes_are_valid(self):
        """ISIC Rev 4 sections are single letters A-U."""
        for _isco_code, isic_code, _match_type, _note in ISCO_ISIC_EDGES:
            assert isic_code.isalpha(), f"ISIC section code should be alpha: {isic_code}"
            assert len(isic_code) == 1, f"ISIC section should be 1-char: {isic_code}"

    def test_match_types_are_valid(self):
        valid = {"exact", "narrow", "broad", "partial"}
        for _isco, _isic, match_type, _note in ISCO_ISIC_EDGES:
            assert match_type in valid, f"Invalid match_type: {match_type}"

    def test_professionals_link_to_professional_services(self):
        """ISCO 2 (Professionals) should link to ISIC M (Professional Services)."""
        prof_edges = [e for e in ISCO_ISIC_EDGES if e[0] == "2"]
        isic_codes = {e[1] for e in prof_edges}
        assert "M" in isic_codes, "Professionals should link to ISIC M"

    def test_agricultural_workers_link_to_isic_a(self):
        """ISCO 6 (Agricultural workers) should link to ISIC A (Agriculture)."""
        ag_edges = [e for e in ISCO_ISIC_EDGES if e[0] == "6"]
        isic_codes = {e[1] for e in ag_edges}
        assert "A" in isic_codes, "Agricultural workers should link to ISIC A"

    def test_no_em_dashes_in_notes(self):
        for _isco, _isic, _mt, note in ISCO_ISIC_EDGES:
            if note:
                assert "\u2014" not in note, f"Em-dash in note: {note}"


def test_ingest_crosswalk_isco_isic(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            from world_of_taxanomy.ingest.isco_08 import ingest_isco_08
            from world_of_taxanomy.ingest.isic import ingest_isic_rev4
            await ingest_isco_08(conn)
            await ingest_isic_rev4(conn)

            count = await ingest_crosswalk_isco_isic(conn)
            assert count >= 30

            rows = await conn.fetch(
                """SELECT * FROM equivalence
                   WHERE source_system = 'isco_08' AND target_system = 'isic_rev4'
                   LIMIT 5"""
            )
            assert len(rows) > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_crosswalk_isco_isic_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            from world_of_taxanomy.ingest.isco_08 import ingest_isco_08
            from world_of_taxanomy.ingest.isic import ingest_isic_rev4
            await ingest_isco_08(conn)
            await ingest_isic_rev4(conn)

            count1 = await ingest_crosswalk_isco_isic(conn)
            count2 = await ingest_crosswalk_isco_isic(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
