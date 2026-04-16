"""Tests for Web of Science Research Areas ingester."""
import asyncio
from world_of_taxonomy.ingest.wos_categories import NODES, ingest_wos_categories


class TestWosCategoriesNodes:
    def test_total_node_count(self):
        assert len(NODES) == 25

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


def test_wos_categories_module_importable():
    from world_of_taxonomy.ingest.wos_categories import ingest_wos_categories as fn
    assert callable(fn)


def test_ingest_wos_categories(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_wos_categories(conn)
            assert count == 25
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_wos_categories_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            c1 = await ingest_wos_categories(conn)
            c2 = await ingest_wos_categories(conn)
            assert c1 == c2
    asyncio.get_event_loop().run_until_complete(_run())
