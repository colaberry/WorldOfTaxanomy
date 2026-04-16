"""Tests for ISO 31000 Risk Framework ingester.

RED tests - written before any implementation exists.

ISO 31000 = Risk Management - Guidelines (ISO/IEC 31000:2018).
Published by the International Organization for Standardization.
Open (hand-coded from publicly available standard structure).
Reference: https://www.iso.org/standard/65694.html

Hierarchy (2 levels):
  Clause    (level 1, e.g. 'iso31000_cl_4')    - 5 main clauses
  Sub-clause (level 2, e.g. 'iso31000_cl_4_1') - sub-clause (leaf)

Codes: 'iso31000_cl_{N}' for clauses, 'iso31000_cl_{N}_{M}' for sub-clauses.

The risk management process (Clause 6) is the central part:
  6.1 Communication and consultation
  6.2 Scope, context, criteria
  6.3 Risk assessment (6.3.1 General, 6.3.2 Risk identification,
      6.3.3 Risk analysis, 6.3.4 Risk evaluation)
  6.4 Risk treatment
  6.5 Monitoring and review
  6.6 Recording and reporting
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.iso31000 import (
    ISO31000_NODES,
    _determine_level,
    _determine_parent,
    ingest_iso31000,
)


class TestDetermineLevel:
    def test_clause_is_level_1(self):
        assert _determine_level("iso31000_cl_4") == 1

    def test_sub_clause_is_level_2(self):
        assert _determine_level("iso31000_cl_4_1") == 2

    def test_clause_6_is_level_1(self):
        assert _determine_level("iso31000_cl_6") == 1

    def test_sub_clause_6_3_is_level_2(self):
        assert _determine_level("iso31000_cl_6_3") == 2


class TestDetermineParent:
    def test_clause_has_no_parent(self):
        assert _determine_parent("iso31000_cl_4") is None

    def test_sub_clause_parent_is_clause(self):
        assert _determine_parent("iso31000_cl_4_1") == "iso31000_cl_4"

    def test_sub_clause_6_3_parent_is_cl_6(self):
        assert _determine_parent("iso31000_cl_6_3") == "iso31000_cl_6"


class TestIso31000Nodes:
    def test_nodes_list_is_non_empty(self):
        assert len(ISO31000_NODES) > 0

    def test_all_codes_start_with_iso31000(self):
        for code, title, level, parent in ISO31000_NODES:
            assert code.startswith("iso31000_"), f"Code '{code}' bad prefix"

    def test_all_titles_non_empty(self):
        for code, title, level, parent in ISO31000_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in ISO31000_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_nodes_have_no_parent(self):
        for code, title, level, parent in ISO31000_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_nodes_have_parent(self):
        for code, title, level, parent in ISO31000_NODES:
            if level == 2:
                assert parent is not None


def test_iso31000_module_importable():
    assert callable(ingest_iso31000)
    assert isinstance(ISO31000_NODES, list)


def test_ingest_iso31000(db_pool):
    """Integration test: ingest ISO 31000 taxonomy."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_iso31000(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system "
                "WHERE id = 'iso_31000'"
            )
            assert row is not None
            assert row["node_count"] == count

            # Top-level clause: level=1, no parent, not leaf
            clause = await conn.fetchrow(
                "SELECT level, parent_code, is_leaf FROM classification_node "
                "WHERE system_id = 'iso_31000' AND level = 1 LIMIT 1"
            )
            assert clause["level"] == 1
            assert clause["parent_code"] is None
            assert clause["is_leaf"] is False

            # Sub-clause: level=2, has parent, is leaf
            sub = await conn.fetchrow(
                "SELECT level, is_leaf FROM classification_node "
                "WHERE system_id = 'iso_31000' AND level = 2 LIMIT 1"
            )
            assert sub["level"] == 2
            assert sub["is_leaf"] is True

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_iso31000_idempotent(db_pool):
    """Running ingest twice returns same count."""
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_iso31000(conn)
            count2 = await ingest_iso31000(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
