"""Tests for EU AI Act regulatory taxonomy ingester. Total: 27 nodes."""
import asyncio, pytest
from world_of_taxonomy.ingest.reg_eu_ai_act import REG_EU_AI_ACT_NODES, ingest_reg_eu_ai_act

class TestRegEuAiActNodes:
    def test_total_node_count(self):
        assert len(REG_EU_AI_ACT_NODES) == 27
    def test_all_titles_non_empty(self):
        for code, title, level, parent in REG_EU_AI_ACT_NODES:
            assert title.strip(), f"Empty title for '{code}'"
    def test_no_duplicate_codes(self):
        codes = [n[0] for n in REG_EU_AI_ACT_NODES]
        assert len(codes) == len(set(codes))
    def test_level_1_nodes_have_no_parent(self):
        for code, title, level, parent in REG_EU_AI_ACT_NODES:
            if level == 1:
                assert parent is None, f"Level-1 node '{code}' has parent '{parent}'"
    def test_level_2_plus_have_parent(self):
        for code, title, level, parent in REG_EU_AI_ACT_NODES:
            if level > 1:
                assert parent is not None, f"Level-{level} node '{code}' has no parent"
    def test_parent_references_valid(self):
        codes = {n[0] for n in REG_EU_AI_ACT_NODES}
        for code, title, level, parent in REG_EU_AI_ACT_NODES:
            if parent is not None:
                assert parent in codes, f"Node '{code}' references non-existent parent '{parent}'"
    def test_has_level_1_nodes(self):
        assert len([n for n in REG_EU_AI_ACT_NODES if n[2] == 1]) >= 1

def test_reg_eu_ai_act_module_importable():
    assert callable(ingest_reg_eu_ai_act)

def test_ingest_reg_eu_ai_act(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_reg_eu_ai_act(conn)
            assert count == 27
            row = await conn.fetchrow("SELECT id, node_count, data_provenance FROM classification_system WHERE id = $1", "reg_eu_ai_act")
            assert row is not None
            assert row["node_count"] == 27
            assert row["data_provenance"] == "manual_transcription"
            l1 = await conn.fetchrow("SELECT level, parent_code FROM classification_node WHERE system_id = $1 AND code = $2", "reg_eu_ai_act", "title_1")
            assert l1["level"] == 1
            assert l1["parent_code"] is None
            leaf = await conn.fetchrow("SELECT is_leaf FROM classification_node WHERE system_id = $1 AND code = $2", "reg_eu_ai_act", "title_1")
            assert leaf["is_leaf"] is True
    asyncio.get_event_loop().run_until_complete(_run())

def test_ingest_reg_eu_ai_act_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            c1 = await ingest_reg_eu_ai_act(conn)
            c2 = await ingest_reg_eu_ai_act(conn)
            assert c1 == c2 == 27
    asyncio.get_event_loop().run_until_complete(_run())
