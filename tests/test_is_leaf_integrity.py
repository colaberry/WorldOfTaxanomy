"""Integrity tests: every node with no children must have is_leaf=True.

Tests a representative sample of problematic systems where hardcoded
`is_leaf = level == 2` incorrectly marks orphaned parent nodes as non-leaves.

TDD Red step: these tests will FAIL until the is_leaf logic is fixed in each
ingester (replacing `is_leaf = level == 2` with
`is_leaf = code not in codes_with_children`).
"""
from __future__ import annotations

import asyncio
import importlib

import pytest


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


@pytest.mark.parametrize(
    "module,fn,orphaned_code",
    [
        ("world_of_taxonomy.ingest.sfdr",       "ingest_sfdr",       "SFDR-RTS"),
        ("world_of_taxonomy.ingest.cfi_iso10962","ingest_cfi_iso10962","M"),
        ("world_of_taxonomy.ingest.msc_2020",   "ingest_msc_2020",   "01"),
        ("world_of_taxonomy.ingest.pacs",        "ingest_pacs",       "00"),
        ("world_of_taxonomy.ingest.lcc",         "ingest_lcc",        "C"),
        # icd10_pcs removed: now a file-based ingester where code "1" has children
    ],
)
class TestIsLeafIntegrity:

    def test_orphaned_node_is_leaf(self, db_pool, module, fn, orphaned_code):
        """A node with no children in the same system must have is_leaf=True."""
        mod = importlib.import_module(module)
        ingest_fn = getattr(mod, fn)
        system_id = mod.SYSTEM_ID

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_fn(conn)
                row = await conn.fetchrow(
                    "SELECT is_leaf FROM classification_node "
                    "WHERE system_id = $1 AND code = $2",
                    system_id,
                    orphaned_code,
                )
                assert row is not None, (
                    f"Node {orphaned_code!r} not found in {system_id}"
                )
                assert row["is_leaf"] is True, (
                    f"Node {orphaned_code!r} in {system_id} has is_leaf=False "
                    f"but has no children - should be True"
                )

        _run(_test())

    def test_node_with_children_is_not_leaf(self, db_pool, module, fn, orphaned_code):
        """A node that has at least one child must have is_leaf=False."""
        mod = importlib.import_module(module)
        ingest_fn = getattr(mod, fn)
        system_id = mod.SYSTEM_ID

        # Find a code that does have children defined in NODES
        codes_with_children = {
            parent
            for (_, _, _, parent) in mod.NODES
            if parent is not None
        }
        if not codes_with_children:
            pytest.skip(f"{system_id} has no parent-child relationships")
        parent_code = next(iter(codes_with_children))

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_fn(conn)
                row = await conn.fetchrow(
                    "SELECT is_leaf FROM classification_node "
                    "WHERE system_id = $1 AND code = $2",
                    system_id,
                    parent_code,
                )
                assert row is not None, (
                    f"Node {parent_code!r} not found in {system_id}"
                )
                assert row["is_leaf"] is False, (
                    f"Node {parent_code!r} in {system_id} has children but "
                    f"is_leaf=True - should be False"
                )

        _run(_test())
