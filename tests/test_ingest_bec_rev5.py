"""Tests for BEC Rev 5 ingester."""
import asyncio, pytest
from world_of_taxonomy.ingest.bec_rev5 import BEC_NODES, ingest_bec_rev5

def _run(c): return asyncio.get_event_loop().run_until_complete(c)

class TestBecNodes:
    def test_count(self): assert len(BEC_NODES) == 29
    def test_has_7_top_level(self):
        assert sum(1 for _, p, _ in BEC_NODES if p is None) == 7
    def test_capital_goods_present(self):
        assert any(c == "4" for c, _, _ in BEC_NODES)
    def test_consumer_goods_present(self):
        assert any(c == "6" for c, _, _ in BEC_NODES)
    def test_all_titles_non_empty(self):
        for _, _, t in BEC_NODES: assert len(t) > 0

def test_bec_importable(): assert callable(ingest_bec_rev5)

def test_ingest_bec_rev5(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM classification_node WHERE system_id='bec_rev5'")
            await conn.execute("DELETE FROM classification_system WHERE id='bec_rev5'")
            count = await ingest_bec_rev5(conn)
            assert count == 29
            row = await conn.fetchrow("SELECT node_count FROM classification_system WHERE id='bec_rev5'")
            assert row["node_count"] == 29
    _run(_test())

def test_ingest_bec_rev5_idempotent(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM classification_node WHERE system_id='bec_rev5'")
            await conn.execute("DELETE FROM classification_system WHERE id='bec_rev5'")
            c1 = await ingest_bec_rev5(conn)
            c2 = await ingest_bec_rev5(conn)
            assert c1 == c2 == 29
    _run(_test())
