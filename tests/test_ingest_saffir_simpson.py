"""Tests for Saffir-Simpson Hurricane Wind Scale ingester."""
import asyncio
from world_of_taxonomy.ingest.saffir_simpson import NODES, ingest_saffir_simpson


class TestStandaloneSaffirSimpsonNodes:
    def test_total_node_count(self):
        assert len(NODES) == 12

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


def test_saffir_simpson_module_importable():
    from world_of_taxonomy.ingest.saffir_simpson import ingest_saffir_simpson as fn
    assert callable(fn)


def test_ingest_saffir_simpson(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_saffir_simpson(conn)
            assert count == 12
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_saffir_simpson_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            c1 = await ingest_saffir_simpson(conn)
            c2 = await ingest_saffir_simpson(conn)
            assert c1 == c2
    asyncio.get_event_loop().run_until_complete(_run())
