"""Tests for UN M.49 / ISO 3166-1 crosswalk ingester.

RED tests - written before any implementation exists.

Links:
  un_m49 country codes (numeric M.49, e.g. "840" = USA)
  <-> iso_3166_1 country codes (alpha-2, e.g. "US")

Source: iso3166_all.csv has both country-code (M.49) and alpha-2 columns.
Match type: exact (same country, different code format).
"""
import pytest
from world_of_taxonomy.ingest.crosswalk_un_m49_iso3166 import (
    ingest_crosswalk_un_m49_iso3166,
)


def test_crosswalk_un_m49_iso3166_module_importable():
    """Module and function are importable."""
    assert callable(ingest_crosswalk_un_m49_iso3166)


def test_ingest_crosswalk_un_m49_iso3166(db_pool):
    """Integration test - inserts bidirectional edges between un_m49 and iso_3166_1."""
    import asyncio
    from pathlib import Path

    data_path = Path("data/iso3166_all.csv")
    if not data_path.exists():
        pytest.skip(f"Download {data_path} first")

    async def _run():
        async with db_pool.acquire() as conn:
            # Seed both systems so FK constraints pass
            await conn.execute(
                """INSERT INTO classification_system
                       (id, name, full_name, version, region, authority, node_count)
                   VALUES ($1,$2,$3,$4,$5,$6,0)
                   ON CONFLICT (id) DO NOTHING""",
                "un_m49", "UN M.49", "UN M.49", "2023", "Global", "UN",
            )
            await conn.execute(
                """INSERT INTO classification_system
                       (id, name, full_name, version, region, authority, node_count)
                   VALUES ($1,$2,$3,$4,$5,$6,0)
                   ON CONFLICT (id) DO NOTHING""",
                "iso_3166_1", "ISO 3166-1", "ISO 3166-1", "2023", "Global", "ISO",
            )
            # Insert a sample M.49 country node (840 = USA)
            await conn.execute(
                """INSERT INTO classification_node
                       (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                   ON CONFLICT DO NOTHING""",
                "un_m49", "840", "United States of America", 3, "021", "019", True, 1,
            )
            # Insert corresponding ISO 3166-1 alpha-2 node
            await conn.execute(
                """INSERT INTO classification_node
                       (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                   ON CONFLICT DO NOTHING""",
                "iso_3166_1", "US", "United States of America", 2, "021", "019", True, 1,
            )

            count = await ingest_crosswalk_un_m49_iso3166(
                conn, path=str(data_path)
            )
            assert count >= 2, f"Expected at least 2 edges (both directions for US), got {count}"

            # Check forward edge: un_m49:840 -> iso_3166_1:US
            row = await conn.fetchrow(
                """SELECT match_type FROM equivalence
                   WHERE source_system = 'un_m49' AND source_code = '840'
                   AND target_system = 'iso_3166_1' AND target_code = 'US'"""
            )
            assert row is not None, "Expected edge un_m49:840 -> iso_3166_1:US"
            assert row["match_type"] == "exact"

            # Check reverse edge: iso_3166_1:US -> un_m49:840
            row2 = await conn.fetchrow(
                """SELECT match_type FROM equivalence
                   WHERE source_system = 'iso_3166_1' AND source_code = 'US'
                   AND target_system = 'un_m49' AND target_code = '840'"""
            )
            assert row2 is not None, "Expected edge iso_3166_1:US -> un_m49:840"
            assert row2["match_type"] == "exact"

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_crosswalk_un_m49_iso3166_idempotent(db_pool):
    """Running twice produces the same count (ON CONFLICT DO NOTHING)."""
    import asyncio
    from pathlib import Path

    data_path = Path("data/iso3166_all.csv")
    if not data_path.exists():
        pytest.skip(f"Download {data_path} first")

    async def _run():
        async with db_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO classification_system
                       (id, name, full_name, version, region, authority, node_count)
                   VALUES ($1,$2,$3,$4,$5,$6,0)
                   ON CONFLICT (id) DO NOTHING""",
                "un_m49", "UN M.49", "UN M.49", "2023", "Global", "UN",
            )
            await conn.execute(
                """INSERT INTO classification_system
                       (id, name, full_name, version, region, authority, node_count)
                   VALUES ($1,$2,$3,$4,$5,$6,0)
                   ON CONFLICT (id) DO NOTHING""",
                "iso_3166_1", "ISO 3166-1", "ISO 3166-1", "2023", "Global", "ISO",
            )
            await conn.execute(
                """INSERT INTO classification_node
                       (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                   ON CONFLICT DO NOTHING""",
                "un_m49", "840", "United States of America", 3, "021", "019", True, 1,
            )
            await conn.execute(
                """INSERT INTO classification_node
                       (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                   ON CONFLICT DO NOTHING""",
                "iso_3166_1", "US", "United States of America", 2, "021", "019", True, 1,
            )

            count1 = await ingest_crosswalk_un_m49_iso3166(conn, path=str(data_path))
            count2 = await ingest_crosswalk_un_m49_iso3166(conn, path=str(data_path))
            assert count1 == count2, f"Expected idempotent counts, got {count1} then {count2}"

    asyncio.get_event_loop().run_until_complete(_run())
