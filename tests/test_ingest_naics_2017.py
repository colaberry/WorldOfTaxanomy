"""Tests for North American Industry Classification System 2017 ingester."""
import asyncio
from world_of_taxonomy.ingest.naics_2017 import NODES, ingest_naics_2017


class TestStandaloneNaics2017Nodes:
    def test_total_node_count(self):
        assert len(NODES) == 21

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


def test_naics_2017_module_importable():
    from world_of_taxonomy.ingest.naics_2017 import ingest_naics_2017 as fn
    assert callable(fn)


def test_ingest_naics_2017(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_naics_2017(conn)
            assert count == 21
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_naics_2017_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            c1 = await ingest_naics_2017(conn)
            c2 = await ingest_naics_2017(conn)
            assert c1 == c2
    asyncio.get_event_loop().run_until_complete(_run())
