"""Tests for data provenance framework.

RED tests - verifies that classification_system carries auditable
provenance metadata: source_url, source_date, data_provenance, license.
"""
from __future__ import annotations

import asyncio
import pytest

from httpx import AsyncClient, ASGITransport
from world_of_taxonomy.api.app import create_app


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# -- Schema-level tests (provenance columns exist) --


class TestProvenanceColumnsExist:
    def test_source_url_column(self, db_pool):
        async def _test():
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    """SELECT column_name FROM information_schema.columns
                       WHERE table_name = 'classification_system'
                         AND column_name = 'source_url'"""
                )
                assert row is not None, "source_url column missing from classification_system"
        _run(_test())

    def test_source_date_column(self, db_pool):
        async def _test():
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    """SELECT column_name FROM information_schema.columns
                       WHERE table_name = 'classification_system'
                         AND column_name = 'source_date'"""
                )
                assert row is not None, "source_date column missing from classification_system"
        _run(_test())

    def test_data_provenance_column(self, db_pool):
        async def _test():
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    """SELECT column_name FROM information_schema.columns
                       WHERE table_name = 'classification_system'
                         AND column_name = 'data_provenance'"""
                )
                assert row is not None, "data_provenance column missing from classification_system"
        _run(_test())

    def test_license_column(self, db_pool):
        async def _test():
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    """SELECT column_name FROM information_schema.columns
                       WHERE table_name = 'classification_system'
                         AND column_name = 'license'"""
                )
                assert row is not None, "license column missing from classification_system"
        _run(_test())


class TestProvenanceConstraints:
    def test_valid_provenance_value_accepted(self, db_pool):
        async def _test():
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO classification_system
                        (id, name, data_provenance, license)
                    VALUES ('test_prov', 'Test Provenance', 'official_download', 'Public Domain')
                """)
                row = await conn.fetchrow(
                    "SELECT data_provenance FROM classification_system WHERE id = 'test_prov'"
                )
                assert row["data_provenance"] == "official_download"
        _run(_test())

    def test_all_four_provenance_tiers_valid(self, db_pool):
        valid_tiers = [
            "official_download",
            "structural_derivation",
            "manual_transcription",
            "expert_curated",
        ]
        async def _test():
            async with db_pool.acquire() as conn:
                for i, tier in enumerate(valid_tiers):
                    await conn.execute(
                        """INSERT INTO classification_system
                               (id, name, data_provenance)
                           VALUES ($1, $2, $3)""",
                        f"test_tier_{i}", f"Test {tier}", tier,
                    )
                    row = await conn.fetchrow(
                        "SELECT data_provenance FROM classification_system WHERE id = $1",
                        f"test_tier_{i}",
                    )
                    assert row["data_provenance"] == tier
        _run(_test())

    def test_invalid_provenance_value_rejected(self, db_pool):
        async def _test():
            async with db_pool.acquire() as conn:
                with pytest.raises(Exception):
                    await conn.execute("""
                        INSERT INTO classification_system
                            (id, name, data_provenance)
                        VALUES ('test_bad', 'Bad', 'made_up_tier')
                    """)
        _run(_test())

    def test_null_provenance_allowed(self, db_pool):
        """Null is allowed for backward compatibility during backfill."""
        async def _test():
            async with db_pool.acquire() as conn:
                # isic_rev4 and sic_1987 seed data have NULL provenance
                row = await conn.fetchrow(
                    "SELECT data_provenance FROM classification_system WHERE id = 'isic_rev4'"
                )
                assert row["data_provenance"] is None
        _run(_test())


# -- API-level tests (provenance fields in API response) --


@pytest.fixture
def app(db_pool):
    application = create_app()
    application.state.pool = db_pool
    return application


@pytest.fixture
def client(app):
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


class TestProvenanceInAPI:
    def test_system_list_includes_provenance_fields(self, client, db_pool):
        async def _test():
            # First set provenance on a seeded system
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE classification_system
                    SET data_provenance = 'official_download',
                        source_url = 'https://example.com/data.xlsx',
                        source_date = '2024-01-15',
                        license = 'Public Domain'
                    WHERE id = 'naics_2022'
                """)
            resp = await client.get("/api/v1/systems")
            assert resp.status_code == 200
            systems = resp.json()
            naics = [s for s in systems if s["id"] == "naics_2022"][0]
            assert "data_provenance" in naics
            assert naics["data_provenance"] == "official_download"
            assert "source_url" in naics
            assert "source_date" in naics
            assert "license" in naics
        _run(_test())

    def test_system_detail_includes_provenance_fields(self, client, db_pool):
        async def _test():
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE classification_system
                    SET data_provenance = 'official_download',
                        license = 'Public Domain'
                    WHERE id = 'naics_2022'
                """)
            resp = await client.get("/api/v1/systems/naics_2022")
            assert resp.status_code == 200
            data = resp.json()
            assert "data_provenance" in data
            assert data["data_provenance"] == "official_download"
            assert "license" in data
        _run(_test())
