"""Tests for e-CF v4 (e-Competence Framework) ingester."""
import asyncio
from world_of_taxonomy.ingest.ecf_v4 import NODES, ingest_ecf_v4


class TestEcfV4Nodes:
    def test_total_node_count(self):
        assert len(NODES) == 35

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


def test_ecf_v4_module_importable():
    from world_of_taxonomy.ingest.ecf_v4 import ingest_ecf_v4 as fn
    assert callable(fn)


def test_ingest_ecf_v4(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_ecf_v4(conn)
            assert count == 35
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_ecf_v4_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            c1 = await ingest_ecf_v4(conn)
            c2 = await ingest_ecf_v4(conn)
            assert c1 == c2
    asyncio.get_event_loop().run_until_complete(_run())
