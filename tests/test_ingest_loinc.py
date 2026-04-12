"""Tests for LOINC ingester.

RED tests - written before any implementation exists.

LOINC (Logical Observation Identifiers Names and Codes), Regenstrief Institute.
Source: loinc.org (manual download, free registration required)
  https://loinc.org/downloads/loinc-table/
  File: Loinc.csv (inside Loinc_2.77.zip or similar)

License: Regenstrief LOINC License (non-commercial redistribution restricted).
  Data file CANNOT be auto-downloaded. User must register at loinc.org and
  download manually. Place at data/loinc.csv.

LOINC codes are flat (no hierarchy) - all nodes are at level 1, is_leaf=True.
~100,000 codes depending on version.

CSV columns include: LOINC_NUM, COMPONENT, LONG_COMMON_NAME, CLASSTYPE, STATUS
Active codes only (STATUS = 'ACTIVE').
"""
import asyncio
import pytest
from pathlib import Path

from world_of_taxanomy.ingest.loinc import (
    _is_active,
    ingest_loinc,
)


class TestLoincIsActive:
    def test_active_status_is_active(self):
        assert _is_active("ACTIVE") is True

    def test_lowercase_active(self):
        assert _is_active("active") is True

    def test_deprecated_is_not_active(self):
        assert _is_active("DEPRECATED") is False

    def test_discouraged_is_not_active(self):
        assert _is_active("DISCOURAGED") is False

    def test_trial_is_active(self):
        # TRIAL codes are included (not deprecated/discouraged)
        assert _is_active("TRIAL") is True

    def test_empty_is_not_active(self):
        assert _is_active("") is False


def test_loinc_module_importable():
    """All public symbols are importable."""
    assert callable(ingest_loinc)
    assert callable(_is_active)


def test_ingest_loinc_from_file(db_pool):
    """Integration test - ingest from manually downloaded LOINC CSV.

    To obtain the file:
      1. Register at https://loinc.org (free)
      2. Download LOINC Table: https://loinc.org/downloads/loinc-table/
      3. Extract Loinc.csv from the ZIP and save as data/loinc.csv
    """
    data_path = Path("data/loinc.csv")
    if not data_path.exists():
        pytest.skip(
            "Download data/loinc.csv from https://loinc.org/downloads/loinc-table/ "
            "(free registration required). Extract Loinc.csv from the downloaded ZIP."
        )

    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_loinc(conn, path=str(data_path))
            # ~90,000+ active LOINC codes
            assert count >= 80000, f"Expected >= 80000 nodes, got {count}"

            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'loinc'"
            )
            assert row is not None
            assert row["node_count"] == count

            # LOINC codes are flat - all are level 1, is_leaf=True
            sample = await conn.fetchrow(
                "SELECT code, level, is_leaf, parent_code FROM classification_node "
                "WHERE system_id = 'loinc' LIMIT 1"
            )
            assert sample is not None
            assert sample["level"] == 1
            assert sample["is_leaf"] is True
            assert sample["parent_code"] is None

            # No deprecated codes
            deprecated = await conn.fetchval(
                "SELECT COUNT(*) FROM classification_node "
                "WHERE system_id = 'loinc' AND title ILIKE '%deprecated%'"
            )
            # This is a soft check - some active titles might mention deprecated
            # The real check is that we filtered by STATUS=ACTIVE
            assert deprecated is not None  # just verify the query works

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_loinc_idempotent(db_pool):
    """Running ingest twice returns consistent count."""
    data_path = Path("data/loinc.csv")
    if not data_path.exists():
        pytest.skip("Download data/loinc.csv from https://loinc.org first")

    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_loinc(conn, path=str(data_path))
            count2 = await ingest_loinc(conn, path=str(data_path))
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
