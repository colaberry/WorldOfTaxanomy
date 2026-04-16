"""Tests for /api/v1/systems/{source}/crosswalk/{target}/graph endpoint.

TDD RED phase: these tests define the contract for the crosswalk graph router.
"""

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
    import asyncio
    return asyncio.get_event_loop().run_until_complete(coro)


class TestCrosswalkGraph:
    """Tests for the crosswalk graph endpoint."""

    def test_graph_returns_nodes_and_edges(self, client):
        """GET graph for known crosswalk returns nodes + edges."""
        resp = _run(client.get(
            "/api/v1/systems/naics_2022/crosswalk/isic_rev4/graph"
        ))
        assert resp.status_code == 200
        data = resp.json()

        assert data["source_system"] == "naics_2022"
        assert data["target_system"] == "isic_rev4"
        assert isinstance(data["nodes"], list)
        assert isinstance(data["edges"], list)
        assert len(data["nodes"]) > 0
        assert len(data["edges"]) > 0

        # Each node must have id, system, code, title
        node = data["nodes"][0]
        assert "id" in node
        assert "system" in node
        assert "code" in node
        assert "title" in node

        # Each edge must have source, target, match_type
        edge = data["edges"][0]
        assert "source" in edge
        assert "target" in edge
        assert "match_type" in edge

    def test_graph_has_correct_node_count(self, client):
        """Nodes include both source and target system codes involved in edges."""
        resp = _run(client.get(
            "/api/v1/systems/naics_2022/crosswalk/isic_rev4/graph"
        ))
        data = resp.json()
        systems = {n["system"] for n in data["nodes"]}
        assert "naics_2022" in systems
        assert "isic_rev4" in systems

    def test_graph_edges_are_deduplicated(self, client):
        """Edges should be deduplicated (no A->B and B->A both present)."""
        resp = _run(client.get(
            "/api/v1/systems/naics_2022/crosswalk/isic_rev4/graph"
        ))
        data = resp.json()
        # Seed data has 3 unique pairs (6 rows bidirectional)
        assert data["total_edges"] == 3
        assert len(data["edges"]) == 3

    def test_graph_unknown_pair_returns_empty(self, client):
        """GET graph for unknown pair returns empty nodes and edges."""
        resp = _run(client.get(
            "/api/v1/systems/naics_2022/crosswalk/sic_1987/graph"
        ))
        assert resp.status_code == 200
        data = resp.json()
        assert data["source_system"] == "naics_2022"
        assert data["target_system"] == "sic_1987"
        assert data["nodes"] == []
        assert data["edges"] == []
        assert data["total_edges"] == 0
        assert data["truncated"] is False

    def test_graph_limit_caps_results(self, client):
        """Limit parameter caps edges returned and sets truncated flag."""
        resp = _run(client.get(
            "/api/v1/systems/naics_2022/crosswalk/isic_rev4/graph?limit=2"
        ))
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["edges"]) == 2
        assert data["total_edges"] == 3
        assert data["truncated"] is True

    def test_graph_response_shape(self, client):
        """Response matches expected schema shape."""
        resp = _run(client.get(
            "/api/v1/systems/naics_2022/crosswalk/isic_rev4/graph"
        ))
        data = resp.json()
        assert set(data.keys()) == {
            "source_system", "target_system",
            "nodes", "edges",
            "total_edges", "truncated",
        }
