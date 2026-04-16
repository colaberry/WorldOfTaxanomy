"""Tests for EAR regulatory taxonomy ingester.

Export Administration Regulations (15 CFR Parts 730-774).
Total: 31 nodes.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.reg_ear import (
    REG_EAR_NODES,
    ingest_reg_ear,
)


class TestRegEarNodes:
    def test_total_node_count(self):
        assert len(REG_EAR_NODES) == 31

    def test_all_titles_non_empty(self):
        for code, title, level, parent in REG_EAR_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in REG_EAR_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_nodes_have_no_parent(self):
        for code, title, level, parent in REG_EAR_NODES:
            if level == 1:
                assert parent is None, f"Level-1 node '{code}' has parent '{parent}'"

    def test_level_2_plus_have_parent(self):
        for code, title, level, parent in REG_EAR_NODES:
            if level > 1:
                assert parent is not None, f"Level-{level} node '{code}' has no parent"

    def test_parent_references_valid(self):
        codes = {n[0] for n in REG_EAR_NODES}
        for code, title, level, parent in REG_EAR_NODES:
            if parent is not None:
                assert parent in codes, f"Node '{code}' references non-existent parent '{parent}'"

    def test_has_level_1_nodes(self):
        l1 = [n for n in REG_EAR_NODES if n[2] == 1]
        assert len(l1) >= 1


def test_reg_ear_module_importable():
    assert callable(ingest_reg_ear)
    assert isinstance(REG_EAR_NODES, list)


def test_ingest_reg_ear(db_pool):
    """Integration test: ingest EAR taxonomy."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_reg_ear(conn)
            assert count == 31

            row = await conn.fetchrow(
                "SELECT id, node_count, data_provenance, license "
                "FROM classification_system WHERE id = $1",
                "reg_ear",
            )
            assert row is not None
            assert row["node_count"] == 31
            assert row["data_provenance"] == "manual_transcription"
            assert row["license"] == "Public Domain"

            # Check a level-1 node
            l1 = await conn.fetchrow(
                "SELECT level, parent_code, is_leaf FROM classification_node "
                "WHERE system_id = $1 AND code = $2",
                "reg_ear", "part_730",
            )
            assert l1["level"] == 1
            assert l1["parent_code"] is None

            # Check a leaf node
            leaf = await conn.fetchrow(
                "SELECT level, is_leaf FROM classification_node "
                "WHERE system_id = $1 AND code = $2",
                "reg_ear", "part_730",
            )
            assert leaf["is_leaf"] is True

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_reg_ear_idempotent(db_pool):
    """Running ingest twice returns same count."""
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_reg_ear(conn)
            count2 = await ingest_reg_ear(conn)
            assert count1 == count2 == 31

    asyncio.get_event_loop().run_until_complete(_run())
