"""Tests for ESCO Occupations <-> ISCO-08 crosswalk ingester.

RED tests - written before any implementation exists.

ESCO occupations are specialisations of ISCO-08 unit groups.
The `code` column in occupations_en.csv (or iscoGroup URI in JSON-LD)
contains the ISCO-08 unit group code.

Crosswalk is derived from:
  - occupations_en.csv (ESCO v1.1.1, CC BY 4.0), OR
  - ESCO dataset JSON-LD ZIP (v1.2.1, CC BY 4.0)

Edge semantics:
  esco_occupations -> isco_08: match_type='narrow' (ESCO is more specific)
  isco_08 -> esco_occupations: match_type='broad' (ISCO is broader)
"""
import asyncio
import os
import pytest

from world_of_taxonomy.ingest.crosswalk_esco_isco import ingest_crosswalk_esco_isco

_OCC_PATH = "data/esco_occupations_en.csv"
_JSONLD_ZIP_PATH = "data/ESCO dataset - v1.2.1 - classification -  - json-ld.zip"
_DATA_AVAILABLE = os.path.exists(_OCC_PATH) or os.path.exists(_JSONLD_ZIP_PATH)


def test_crosswalk_esco_isco_module_importable():
    assert callable(ingest_crosswalk_esco_isco)


@pytest.mark.skipif(
    not _DATA_AVAILABLE,
    reason=f"Neither ESCO CSV ({_OCC_PATH}) nor JSON-LD ZIP ({_JSONLD_ZIP_PATH}) found.",
)
def test_ingest_crosswalk_esco_isco(db_pool):
    """Integration test: creates bidirectional ESCO <-> ISCO-08 edges."""
    async def _run():
        from world_of_taxonomy.ingest.esco_occupations import ingest_esco_occupations
        from world_of_taxonomy.ingest.isco_08 import ingest_isco_08
        async with db_pool.acquire() as conn:
            # Setup: ingest prerequisite systems (auto-detects CSV or JSON-LD)
            if os.path.exists(_OCC_PATH):
                await ingest_esco_occupations(conn, path=_OCC_PATH)
            else:
                await ingest_esco_occupations(conn)
            await ingest_isco_08(conn)

            count = await ingest_crosswalk_esco_isco(conn)
            # ~2,942 occupations x 2 directions = ~5,884 edges
            assert count >= 5000, f"Expected >= 5000 edges, got {count}"
            assert count <= 8000, f"Expected <= 8000 edges, got {count}"

            # Check that forward edges have match_type='narrow'
            fwd_sample = await conn.fetchrow(
                "SELECT match_type FROM equivalence "
                "WHERE source_system = 'esco_occupations' "
                "AND target_system = 'isco_08' "
                "LIMIT 1"
            )
            assert fwd_sample is not None
            assert fwd_sample["match_type"] == "narrow"

            # Check that reverse edges have match_type='broad'
            rev_sample = await conn.fetchrow(
                "SELECT match_type FROM equivalence "
                "WHERE source_system = 'isco_08' "
                "AND target_system = 'esco_occupations' "
                "LIMIT 1"
            )
            assert rev_sample is not None
            assert rev_sample["match_type"] == "broad"

    asyncio.get_event_loop().run_until_complete(_run())


@pytest.mark.skipif(
    not _DATA_AVAILABLE,
    reason=f"Neither ESCO CSV nor JSON-LD ZIP found.",
)
def test_ingest_crosswalk_esco_isco_idempotent(db_pool):
    """Running ingest twice returns consistent count."""
    async def _run():
        from world_of_taxonomy.ingest.esco_occupations import ingest_esco_occupations
        from world_of_taxonomy.ingest.isco_08 import ingest_isco_08
        async with db_pool.acquire() as conn:
            if os.path.exists(_OCC_PATH):
                await ingest_esco_occupations(conn, path=_OCC_PATH)
            else:
                await ingest_esco_occupations(conn)
            await ingest_isco_08(conn)
            count1 = await ingest_crosswalk_esco_isco(conn)
            count2 = await ingest_crosswalk_esco_isco(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
