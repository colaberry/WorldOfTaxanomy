"""Tests for CIP 2020 <-> SOC 2018 crosswalk ingester.

RED tests - written before any implementation exists.

Source: NCES CIP 2020 / SOC 2018 Crosswalk (official, public domain)
  https://nces.ed.gov/ipeds/cipcode/Files/CIP2020_SOC2018_Crosswalk.xlsx

Sheet used: 'CIP-SOC' (CIP2020Code, CIP2020Title, SOC2018Code, SOC2018Title).
'NO MATCH' rows and codes not in our soc_2018 system are excluded.
Match type: 'broad' (many-to-many official mapping).
~5,917 pairs -> ~11,834 bidirectional edges (filtered to soc_2018 DB codes).
"""
import asyncio
import pytest
from pathlib import Path

from world_of_taxonomy.ingest.crosswalk_cip_soc import ingest_crosswalk_cip_soc


def test_crosswalk_cip_soc_module_importable():
    """Function is importable and callable."""
    assert callable(ingest_crosswalk_cip_soc)


def test_ingest_crosswalk_cip_soc(db_pool):
    """Integration test - inserts bidirectional CIP 2020 <-> SOC 2018 edges."""
    cw_path = Path("data/cip2020_soc2018.xlsx")
    cip_path = Path("data/cip_2020.csv")
    soc_path = Path("data/soc_2018.csv")
    if not all(p.exists() for p in [cw_path, cip_path, soc_path]):
        pytest.skip("Download cip2020_soc2018.xlsx, cip_2020.csv, soc_2018.csv first")

    async def _run():
        from world_of_taxonomy.ingest.cip_2020 import ingest_cip_2020
        from world_of_taxonomy.ingest.soc_2018 import ingest_soc_2018
        async with db_pool.acquire() as conn:
            await ingest_cip_2020(conn, path=str(cip_path))
            await ingest_soc_2018(conn, path=str(soc_path))
            count = await ingest_crosswalk_cip_soc(conn, path=str(cw_path))
            # ~5917 pairs x 2 = ~11834 edges
            assert count >= 10000, f"Expected >= 10000 edges, got {count}"

            # Forward: CIP "01.0000" -> SOC "19-1011" (Animal Scientists)
            fwd = await conn.fetchrow(
                "SELECT match_type FROM equivalence "
                "WHERE source_system = 'cip_2020' AND source_code = '01.0000' "
                "AND target_system = 'soc_2018' AND target_code = '19-1011'"
            )
            assert fwd is not None, "Forward edge cip_2020:01.0000 -> soc_2018:19-1011 missing"
            assert fwd["match_type"] == "broad"

            # Reverse: SOC "19-1011" -> CIP "01.0000"
            rev = await conn.fetchrow(
                "SELECT match_type FROM equivalence "
                "WHERE source_system = 'soc_2018' AND source_code = '19-1011' "
                "AND target_system = 'cip_2020' AND target_code = '01.0000'"
            )
            assert rev is not None, "Reverse edge soc_2018:19-1011 -> cip_2020:01.0000 missing"
            assert rev["match_type"] == "broad"

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_crosswalk_cip_soc_idempotent(db_pool):
    """Running ingest twice returns consistent count."""
    cw_path = Path("data/cip2020_soc2018.xlsx")
    cip_path = Path("data/cip_2020.csv")
    soc_path = Path("data/soc_2018.csv")
    if not all(p.exists() for p in [cw_path, cip_path, soc_path]):
        pytest.skip("Download data files first")

    async def _run():
        from world_of_taxonomy.ingest.cip_2020 import ingest_cip_2020
        from world_of_taxonomy.ingest.soc_2018 import ingest_soc_2018
        async with db_pool.acquire() as conn:
            await ingest_cip_2020(conn, path=str(cip_path))
            await ingest_soc_2018(conn, path=str(soc_path))
            count1 = await ingest_crosswalk_cip_soc(conn, path=str(cw_path))
            count2 = await ingest_crosswalk_cip_soc(conn, path=str(cw_path))
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())


def test_crosswalk_excludes_no_match_rows(db_pool):
    """Rows with SOC code '99.9999' (NO MATCH) are not inserted."""
    cw_path = Path("data/cip2020_soc2018.xlsx")
    cip_path = Path("data/cip_2020.csv")
    soc_path = Path("data/soc_2018.csv")
    if not all(p.exists() for p in [cw_path, cip_path, soc_path]):
        pytest.skip("Download data files first")

    async def _run():
        from world_of_taxonomy.ingest.cip_2020 import ingest_cip_2020
        from world_of_taxonomy.ingest.soc_2018 import ingest_soc_2018
        async with db_pool.acquire() as conn:
            await ingest_cip_2020(conn, path=str(cip_path))
            await ingest_soc_2018(conn, path=str(soc_path))
            await ingest_crosswalk_cip_soc(conn, path=str(cw_path))

            no_match = await conn.fetchval(
                "SELECT COUNT(*) FROM equivalence "
                "WHERE source_system = 'cip_2020' AND target_code = '99.9999'"
            )
            assert no_match == 0, f"Found {no_match} NO MATCH rows in equivalence table"

    asyncio.get_event_loop().run_until_complete(_run())
