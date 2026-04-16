"""Tests for HS 2022 / ISIC Rev 4 crosswalk ingester.

RED tests - written before any implementation exists.

Source: World Bank WITS HS 2012 -> ISIC Rev 3 concordance, filtered to
        codes present in both hs_2022 and isic_rev4 systems.
Match type: broad (version differences: HS 2012 vs 2022, ISIC Rev 3 vs Rev 4).
Edges: ~3,010 (1,505 pairs x 2 directions).
"""
import pytest
from world_of_taxonomy.ingest.crosswalk_hs_isic import (
    ingest_crosswalk_hs_isic,
)


def test_crosswalk_hs_isic_module_importable():
    """Module and function are importable."""
    assert callable(ingest_crosswalk_hs_isic)


def test_ingest_crosswalk_hs_isic(db_pool):
    """Integration test - inserts bidirectional edges between hs_2022 and isic_rev4."""
    import asyncio
    from pathlib import Path

    data_path = Path("data/hs_isic_wits.csv")
    if not data_path.exists():
        pytest.skip(f"Download {data_path} first: see world_of_taxonomy/ingest/crosswalk_hs_isic.py")

    async def _run():
        async with db_pool.acquire() as conn:
            # Seed both systems
            await conn.execute(
                """INSERT INTO classification_system
                       (id, name, full_name, version, region, authority, node_count)
                   VALUES ($1,$2,$3,$4,$5,$6,0)
                   ON CONFLICT (id) DO NOTHING""",
                "hs_2022", "HS 2022", "HS 2022", "2022", "Global", "WCO",
            )
            await conn.execute(
                """INSERT INTO classification_system
                       (id, name, full_name, version, region, authority, node_count)
                   VALUES ($1,$2,$3,$4,$5,$6,0)
                   ON CONFLICT (id) DO NOTHING""",
                "isic_rev4", "ISIC Rev 4", "ISIC Rev 4", "4", "Global", "UN",
            )
            # Seed one HS subheading and one ISIC class that are in the WITS concordance
            await conn.execute(
                """INSERT INTO classification_node
                       (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                   ON CONFLICT DO NOTHING""",
                "hs_2022", "010121", "Pure-bred breeding horses", 4, "0101", "I", True, 1,
            )
            await conn.execute(
                """INSERT INTO classification_node
                       (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                   ON CONFLICT DO NOTHING""",
                "isic_rev4", "0121", "Cattle farming", 4, "012", "A", True, 1,
            )

            count = await ingest_crosswalk_hs_isic(conn, path=str(data_path))
            assert count >= 2, f"Expected at least 2 edges (both directions for 010121/0121), got {count}"

            # Check forward edge: hs_2022:010121 -> isic_rev4:0121
            row = await conn.fetchrow(
                """SELECT match_type FROM equivalence
                   WHERE source_system = 'hs_2022' AND source_code = '010121'
                   AND target_system = 'isic_rev4' AND target_code = '0121'"""
            )
            assert row is not None, "Expected edge hs_2022:010121 -> isic_rev4:0121"
            assert row["match_type"] == "broad"

            # Check reverse edge: isic_rev4:0121 -> hs_2022:010121
            row2 = await conn.fetchrow(
                """SELECT match_type FROM equivalence
                   WHERE source_system = 'isic_rev4' AND source_code = '0121'
                   AND target_system = 'hs_2022' AND target_code = '010121'"""
            )
            assert row2 is not None, "Expected edge isic_rev4:0121 -> hs_2022:010121"
            assert row2["match_type"] == "broad"

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_crosswalk_hs_isic_idempotent(db_pool):
    """Running twice produces the same count (ON CONFLICT DO NOTHING)."""
    import asyncio
    from pathlib import Path

    data_path = Path("data/hs_isic_wits.csv")
    if not data_path.exists():
        pytest.skip(f"Download {data_path} first")

    async def _run():
        async with db_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO classification_system (id, name, full_name, version, region, authority, node_count)
                   VALUES ($1,$2,$3,$4,$5,$6,0) ON CONFLICT (id) DO NOTHING""",
                "hs_2022", "HS 2022", "HS 2022", "2022", "Global", "WCO",
            )
            await conn.execute(
                """INSERT INTO classification_system (id, name, full_name, version, region, authority, node_count)
                   VALUES ($1,$2,$3,$4,$5,$6,0) ON CONFLICT (id) DO NOTHING""",
                "isic_rev4", "ISIC Rev 4", "ISIC Rev 4", "4", "Global", "UN",
            )
            await conn.execute(
                """INSERT INTO classification_node
                       (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING""",
                "hs_2022", "010121", "Pure-bred breeding horses", 4, "0101", "I", True, 1,
            )
            await conn.execute(
                """INSERT INTO classification_node
                       (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING""",
                "isic_rev4", "0121", "Cattle farming", 4, "012", "A", True, 1,
            )

            count1 = await ingest_crosswalk_hs_isic(conn, path=str(data_path))
            count2 = await ingest_crosswalk_hs_isic(conn, path=str(data_path))
            assert count1 == count2, f"Expected idempotent, got {count1} then {count2}"

    asyncio.get_event_loop().run_until_complete(_run())
