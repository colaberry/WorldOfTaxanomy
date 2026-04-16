"""Tests for ICD-11 parquet-based ingestion.

Uses data/icd11_synonyms.parquet (already on disk, no download required).
14,202 unique ICD-11 codes + 21 chapter nodes = 14,223 total nodes.
"""
from __future__ import annotations

import asyncio
import pytest
from pathlib import Path
from world_of_taxonomy.ingest.icd_11 import (
    ingest_icd_11_from_parquet,
    _derive_icd11_parent,
    _derive_icd11_level,
)

PARQUET_PATH = Path("data/icd11_synonyms.parquet")


class TestIcd11DeriveParent:
    def test_base_code_returns_none(self):
        assert _derive_icd11_parent("1A00") is None

    def test_dot_code_returns_base(self):
        assert _derive_icd11_parent("1A00.0") == "1A00"

    def test_another_dot_code(self):
        assert _derive_icd11_parent("XY01.Z") == "XY01"

    def test_empty_string_returns_none(self):
        assert _derive_icd11_parent("") is None

    def test_chapter_code_returns_none(self):
        assert _derive_icd11_parent("CH01") is None


class TestIcd11DeriveLevel:
    def test_chapter_code_is_level_1(self):
        assert _derive_icd11_level("CH01") == 1

    def test_base_code_is_level_2(self):
        assert _derive_icd11_level("1A00") == 2

    def test_dot_code_is_level_3(self):
        assert _derive_icd11_level("1A00.0") == 3

    def test_another_base_code(self):
        assert _derive_icd11_level("XY01") == 2

    def test_another_dot_code(self):
        assert _derive_icd11_level("8B92.2") == 3


def test_ingest_icd_11_from_parquet(db_pool):
    """Integration test using the on-disk parquet file."""
    if not PARQUET_PATH.exists():
        pytest.skip(f"Parquet file not found at {PARQUET_PATH}")
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_icd_11_from_parquet(conn, path=str(PARQUET_PATH))
            assert count > 14000, f"Expected >14000 nodes, got {count}"
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'icd_11'"
            )
            assert row is not None
            assert row["node_count"] == count
            # Check a known code
            node = await conn.fetchrow(
                "SELECT code, title FROM classification_node "
                "WHERE system_id = 'icd_11' AND code = '1A00'"
            )
            assert node is not None
            assert "Cholera" in node["title"]
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_icd_11_from_parquet_idempotent(db_pool):
    if not PARQUET_PATH.exists():
        pytest.skip(f"Parquet file not found at {PARQUET_PATH}")
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_icd_11_from_parquet(conn, path=str(PARQUET_PATH))
            count2 = await ingest_icd_11_from_parquet(conn, path=str(PARQUET_PATH))
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
