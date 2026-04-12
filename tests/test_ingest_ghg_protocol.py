"""Tests for GHG Protocol ingester.

RED tests - written before any implementation exists.

GHG Protocol = Greenhouse Gas Protocol Corporate Accounting and Reporting Standard.
Source: hand-coded (World Resources Institute / WBCSD - open)
License: open

Hierarchy (2 levels):
  Scope      level=1  e.g. "scope_1", "scope_2", "scope_3"
  Category   level=2  e.g. "scope_3_cat_1" ... "scope_3_cat_15"

3 scopes + 17 categories (2 under scope 1/2, 15 under scope 3) = 20 nodes.
All categories are leaves. Scopes are not leaves (they have children).
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.ghg_protocol import (
    GHG_NODES,
    ingest_ghg_protocol,
)


class TestGhgNodes:
    def test_exactly_20_nodes(self):
        assert len(GHG_NODES) == 20

    def test_three_scopes_present(self):
        codes = [n[0] for n in GHG_NODES]
        assert "scope_1" in codes
        assert "scope_2" in codes
        assert "scope_3" in codes

    def test_scope_3_has_15_categories(self):
        s3_cats = [n for n in GHG_NODES if n[0].startswith("scope_3_cat_")]
        assert len(s3_cats) == 15

    def test_scope_1_is_level_1(self):
        node = next(n for n in GHG_NODES if n[0] == "scope_1")
        assert node[2] == 1  # level

    def test_scope_3_cat_1_is_level_2(self):
        node = next(n for n in GHG_NODES if n[0] == "scope_3_cat_1")
        assert node[2] == 2  # level

    def test_scope_1_has_no_parent(self):
        node = next(n for n in GHG_NODES if n[0] == "scope_1")
        assert node[3] is None  # parent_code

    def test_scope_3_cat_1_parent_is_scope_3(self):
        node = next(n for n in GHG_NODES if n[0] == "scope_3_cat_1")
        assert node[3] == "scope_3"  # parent_code

    def test_all_titles_non_empty(self):
        for code, title, level, parent in GHG_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in GHG_NODES]
        assert len(codes) == len(set(codes))


def test_ghg_protocol_module_importable():
    assert callable(ingest_ghg_protocol)
    assert isinstance(GHG_NODES, list)


def test_ingest_ghg_protocol(db_pool):
    """Integration test: ingest GHG Protocol nodes."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_ghg_protocol(conn)
            assert count == 20, f"Expected 20 GHG nodes, got {count}"

            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system "
                "WHERE id = 'ghg_protocol'"
            )
            assert row is not None
            assert row["node_count"] == 20

            # Scope 1 - level 1, no parent, not a leaf
            scope1 = await conn.fetchrow(
                "SELECT level, parent_code, is_leaf, sector_code "
                "FROM classification_node "
                "WHERE system_id = 'ghg_protocol' AND code = 'scope_1'"
            )
            assert scope1["level"] == 1
            assert scope1["parent_code"] is None
            assert scope1["is_leaf"] is False
            assert scope1["sector_code"] == "scope_1"

            # Scope 3 Category 1 - level 2, leaf
            cat1 = await conn.fetchrow(
                "SELECT level, parent_code, is_leaf FROM classification_node "
                "WHERE system_id = 'ghg_protocol' AND code = 'scope_3_cat_1'"
            )
            assert cat1["level"] == 2
            assert cat1["parent_code"] == "scope_3"
            assert cat1["is_leaf"] is True

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_ghg_protocol_idempotent(db_pool):
    """Running ingest twice returns 20 both times."""
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_ghg_protocol(conn)
            count2 = await ingest_ghg_protocol(conn)
            assert count1 == count2 == 20

    asyncio.get_event_loop().run_until_complete(_run())
