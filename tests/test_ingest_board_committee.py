"""Tests for Corporate Board Committee Types ingester."""
import asyncio
from world_of_taxonomy.ingest.board_committee import NODES, ingest_board_committee


class TestStandaloneBoardCommitteeNodes:
    def test_total_node_count(self):
        assert len(NODES) == 14

    def test_all_titles_non_empty(self):
        for code, title, level, parent in NODES:
            assert title, f"Empty title for {code}"

    def test_no_duplicate_codes(self):
        codes = [c for c, *_ in NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_nodes_have_no_parent(self):
        for code, title, level, parent in NODES:
            if level == 1:
                assert parent is None, f"{code} level-1 has parent"

    def test_level_2_plus_have_parent(self):
        for code, title, level, parent in NODES:
            if level >= 2:
                assert parent is not None, f"{code} level-{level} missing parent"

    def test_parent_references_valid(self):
        codes = {c for c, *_ in NODES}
        for code, title, level, parent in NODES:
            if parent is not None:
                assert parent in codes, f"{code} parent {parent} not in codes"

    def test_has_level_1_nodes(self):
        assert any(level == 1 for _, _, level, _ in NODES)


def test_board_committee_module_importable():
    from world_of_taxonomy.ingest.board_committee import ingest_board_committee as fn
    assert callable(fn)


def test_ingest_board_committee(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_board_committee(conn)
            assert count == 14
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_board_committee_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            c1 = await ingest_board_committee(conn)
            c2 = await ingest_board_committee(conn)
            assert c1 == c2
    asyncio.get_event_loop().run_until_complete(_run())
