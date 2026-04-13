"""Tests for Manufacturing Process Type domain taxonomy ingester.

RED tests - written before any implementation exists.

Manufacturing process taxonomy organizes production methods:
  Discrete Mfg  (dfp_discrete*) - machining, forming, assembly, fabrication
  Process Mfg   (dfp_process*)  - chemical, refining, mixing, continuous flow
  Additive       (dfp_add*)     - 3D printing, SLA, SLS, DMLS
  Hybrid/Other   (dfp_other*)   - casting, forging, molding, extrusion

Source: NIST manufacturing process classifications. Public domain.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.domain_mfg_process import (
    MFG_PROCESS_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_mfg_process,
)


class TestDetermineLevel:
    def test_discrete_category_is_level_1(self):
        assert _determine_level("dfp_discrete") == 1

    def test_machining_is_level_2(self):
        assert _determine_level("dfp_discrete_machine") == 2

    def test_process_category_is_level_1(self):
        assert _determine_level("dfp_process") == 1

    def test_chemical_is_level_2(self):
        assert _determine_level("dfp_process_chem") == 2


class TestDetermineParent:
    def test_discrete_category_has_no_parent(self):
        assert _determine_parent("dfp_discrete") is None

    def test_machining_parent_is_discrete(self):
        assert _determine_parent("dfp_discrete_machine") == "dfp_discrete"

    def test_chemical_parent_is_process(self):
        assert _determine_parent("dfp_process_chem") == "dfp_process"


class TestMfgProcessNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(MFG_PROCESS_NODES) > 0

    def test_has_discrete_category(self):
        codes = [n[0] for n in MFG_PROCESS_NODES]
        assert "dfp_discrete" in codes

    def test_has_process_category(self):
        codes = [n[0] for n in MFG_PROCESS_NODES]
        assert "dfp_process" in codes

    def test_has_additive_category(self):
        codes = [n[0] for n in MFG_PROCESS_NODES]
        assert "dfp_add" in codes

    def test_has_machining(self):
        codes = [n[0] for n in MFG_PROCESS_NODES]
        assert "dfp_discrete_machine" in codes

    def test_has_chemical(self):
        codes = [n[0] for n in MFG_PROCESS_NODES]
        assert "dfp_process_chem" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in MFG_PROCESS_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in MFG_PROCESS_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in MFG_PROCESS_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in MFG_PROCESS_NODES:
            if level == 2:
                assert parent is not None


def test_domain_mfg_process_module_importable():
    assert callable(ingest_domain_mfg_process)
    assert isinstance(MFG_PROCESS_NODES, list)


def test_ingest_domain_mfg_process(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_mfg_process(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_mfg_process'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_mfg_process_idempotent(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_mfg_process(conn)
            count2 = await ingest_domain_mfg_process(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
