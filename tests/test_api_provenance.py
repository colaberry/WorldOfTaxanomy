"""Tests for provenance metadata in node responses and the audit endpoint."""
import asyncio

import pytest
from httpx import AsyncClient, ASGITransport

from world_of_taxonomy.api.app import create_app


@pytest.fixture
def app(db_pool):
    application = create_app()
    application.state.pool = db_pool
    return application


@pytest.fixture
def client(app):
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Node responses carry provenance
# ---------------------------------------------------------------------------


class TestNodeProvenance:
    """Every node response must carry system-level provenance fields."""

    def test_get_node_has_provenance(self, client):
        async def _test():
            resp = await client.get("/api/v1/systems/naics_2022/nodes/62")
            assert resp.status_code == 200
            data = resp.json()
            assert data["data_provenance"] == "official_download"
            assert data["license"] == "Public Domain (US Government)"
            assert data["source_url"] == "https://www.census.gov/naics/"
            assert data["source_file_hash"] == "abc123hash"
        _run(_test())

    def test_get_children_have_provenance(self, client):
        async def _test():
            resp = await client.get("/api/v1/systems/naics_2022/nodes/62/children")
            assert resp.status_code == 200
            data = resp.json()
            assert len(data) >= 1
            for child in data:
                assert child["data_provenance"] == "official_download"
                assert child["source_url"] is not None
        _run(_test())

    def test_get_ancestors_have_provenance(self, client):
        async def _test():
            resp = await client.get("/api/v1/systems/naics_2022/nodes/621/ancestors")
            assert resp.status_code == 200
            data = resp.json()
            for ancestor in data:
                assert ancestor["data_provenance"] == "official_download"
        _run(_test())

    def test_system_detail_roots_have_provenance(self, client):
        async def _test():
            resp = await client.get("/api/v1/systems/naics_2022")
            assert resp.status_code == 200
            data = resp.json()
            for root in data["roots"]:
                assert root["data_provenance"] == "official_download"
                assert root["license"] == "Public Domain (US Government)"
        _run(_test())

    def test_search_results_have_provenance(self, client):
        async def _test():
            resp = await client.get("/api/v1/search?q=health")
            assert resp.status_code == 200
            data = resp.json()
            assert len(data) >= 1
            for node in data:
                assert "data_provenance" in node
                assert "license" in node
                assert "source_url" in node
        _run(_test())

    def test_system_without_provenance_returns_null(self, client):
        """Systems with no provenance set should return null fields."""
        async def _test():
            resp = await client.get("/api/v1/systems/isic_rev4/nodes/A")
            assert resp.status_code == 200
            data = resp.json()
            # ISIC seed has no provenance set
            assert data["data_provenance"] is None
            assert data["source_file_hash"] is None
        _run(_test())


# ---------------------------------------------------------------------------
# Audit endpoint
# ---------------------------------------------------------------------------


class TestAuditEndpoint:
    """The /api/v1/audit/provenance endpoint returns aggregate audit data."""

    def test_audit_provenance_returns_200(self, client):
        async def _test():
            resp = await client.get("/api/v1/audit/provenance")
            assert resp.status_code == 200
        _run(_test())

    def test_audit_provenance_has_totals(self, client):
        async def _test():
            resp = await client.get("/api/v1/audit/provenance")
            data = resp.json()
            assert "total_systems" in data
            assert "total_nodes" in data
            assert data["total_systems"] >= 3  # naics, isic, sic
            assert data["total_nodes"] >= 20
        _run(_test())

    def test_audit_provenance_has_tiers(self, client):
        async def _test():
            resp = await client.get("/api/v1/audit/provenance")
            data = resp.json()
            tiers = data["provenance_tiers"]
            assert isinstance(tiers, list)
            assert len(tiers) >= 1
            for tier in tiers:
                assert "data_provenance" in tier
                assert "system_count" in tier
                assert "node_count" in tier
        _run(_test())

    def test_audit_provenance_has_skeleton_systems(self, client):
        async def _test():
            resp = await client.get("/api/v1/audit/provenance")
            data = resp.json()
            skeletons = data["skeleton_systems"]
            assert isinstance(skeletons, list)
            # All test seed systems have < 30 nodes, so all should appear
            assert len(skeletons) >= 3
            for s in skeletons:
                assert s["node_count"] < 30
        _run(_test())

    def test_audit_provenance_structural_derivation(self, client):
        async def _test():
            resp = await client.get("/api/v1/audit/provenance")
            data = resp.json()
            assert "structural_derivation_count" in data
            assert "structural_derivation_nodes" in data
        _run(_test())

    def test_audit_provenance_missing_hash(self, client):
        async def _test():
            resp = await client.get("/api/v1/audit/provenance")
            data = resp.json()
            missing = data["official_missing_hash"]
            assert isinstance(missing, list)
            # NAICS has a hash set, so it should NOT appear here
            ids = [s["id"] for s in missing]
            assert "naics_2022" not in ids
        _run(_test())
