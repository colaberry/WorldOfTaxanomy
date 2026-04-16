"""Tests for SITC Rev 4 ingester."""
import asyncio, pytest
from world_of_taxonomy.ingest.sitc_rev4 import SECTIONS, DIVISIONS, ingest_sitc_rev4

def _run(c): return asyncio.get_event_loop().run_until_complete(c)

class TestSitcSections:
    def test_exactly_10_sections(self): assert len(SECTIONS) == 10
    def test_all_titles_non_empty(self):
        for k, v in SECTIONS: assert len(v) > 0
    def test_section_7_is_machinery(self):
        titles = dict(SECTIONS)
        assert "Machinery" in titles["7"]
    def test_section_5_is_chemicals(self):
        titles = dict(SECTIONS)
        assert "Chemical" in titles["5"]

class TestSitcDivisions:
    def test_at_least_60_divisions(self): assert len(DIVISIONS) >= 60
    def test_no_duplicate_codes(self):
        codes = [d[1] for d in DIVISIONS]
        assert len(codes) == len(set(codes))
    def test_all_sections_valid(self):
        sec_codes = {s for s, _ in SECTIONS}
        for sec, _, _ in DIVISIONS: assert sec in sec_codes
    def test_has_pharmaceutical_division(self):
        codes = [d[1] for d in DIVISIONS]
        assert "54" in codes

def test_sitc_importable(): assert callable(ingest_sitc_rev4)

def test_ingest_sitc_rev4(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM equivalence WHERE source_system='sitc_rev4' OR target_system='sitc_rev4'")
            await conn.execute("DELETE FROM classification_node WHERE system_id='sitc_rev4'")
            await conn.execute("DELETE FROM classification_system WHERE id='sitc_rev4'")
            count = await ingest_sitc_rev4(conn)
            assert count == 77
            row = await conn.fetchrow("SELECT node_count FROM classification_system WHERE id='sitc_rev4'")
            assert row["node_count"] == 77
    _run(_test())

def test_ingest_sitc_rev4_idempotent(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM equivalence WHERE source_system='sitc_rev4' OR target_system='sitc_rev4'")
            await conn.execute("DELETE FROM classification_node WHERE system_id='sitc_rev4'")
            await conn.execute("DELETE FROM classification_system WHERE id='sitc_rev4'")
            c1 = await ingest_sitc_rev4(conn)
            c2 = await ingest_sitc_rev4(conn)
            assert c1 == c2 == 77
    _run(_test())
