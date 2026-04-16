"""Tests for Key Management Types ingester."""
import asyncio
from world_of_taxonomy.ingest.domain_key_mgmt import NODES, ingest_domain_key_mgmt


class TestDomainKeyMgmtNodes:
    def test_total_node_count(self):
        assert len(NODES) == 15

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


def test_domain_key_mgmt_module_importable():
    from world_of_taxonomy.ingest.domain_key_mgmt import ingest_domain_key_mgmt as fn
    assert callable(fn)


def test_ingest_domain_key_mgmt(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_key_mgmt(conn)
            assert count == 15
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_key_mgmt_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            c1 = await ingest_domain_key_mgmt(conn)
            c2 = await ingest_domain_key_mgmt(conn)
            assert c1 == c2
    asyncio.get_event_loop().run_until_complete(_run())
