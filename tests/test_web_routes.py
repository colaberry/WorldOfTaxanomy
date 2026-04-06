"""Tests for web frontend routes.

TDD RED phase: these tests define the contract for the web UI routes.
Each route should return HTML with expected elements from the observatory design.
"""

import asyncio
import pytest
from httpx import AsyncClient, ASGITransport

from world_of_taxanomy.api.app import create_app


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


# ── Galaxy View (Home) ────────────────────────────────────────


def test_home_page_returns_html(client):
    async def _test():
        resp = await client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
    _run(_test())


def test_home_page_has_title(client):
    async def _test():
        resp = await client.get("/")
        assert "WorldOfTaxanomy" in resp.text
    _run(_test())


def test_home_page_has_galaxy_container(client):
    async def _test():
        resp = await client.get("/")
        assert "galaxy" in resp.text.lower()
    _run(_test())


def test_home_page_loads_d3(client):
    async def _test():
        resp = await client.get("/")
        assert "d3" in resp.text.lower() or "d3.v7" in resp.text
    _run(_test())


def test_home_page_has_observatory_css(client):
    async def _test():
        resp = await client.get("/")
        assert "observatory.css" in resp.text
    _run(_test())


# ── System View (Sector Treemap) ──────────────────────────────


def test_system_page_returns_html(client):
    async def _test():
        resp = await client.get("/system/naics_2022")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
    _run(_test())


def test_system_page_has_system_name(client):
    async def _test():
        resp = await client.get("/system/naics_2022")
        assert "NAICS" in resp.text
    _run(_test())


def test_system_page_has_treemap_container(client):
    async def _test():
        resp = await client.get("/system/naics_2022")
        assert "treemap" in resp.text.lower() or "sector" in resp.text.lower()
    _run(_test())


def test_system_page_not_found(client):
    async def _test():
        resp = await client.get("/system/nonexistent")
        assert resp.status_code == 404
    _run(_test())


# ── Node View ────────────────────────────────────────────────


def test_node_page_returns_html(client):
    async def _test():
        resp = await client.get("/system/naics_2022/62")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
    _run(_test())


def test_node_page_has_code_and_title(client):
    async def _test():
        resp = await client.get("/system/naics_2022/62")
        text = resp.text
        assert "62" in text
        assert "Health Care" in text
    _run(_test())


def test_node_page_has_children_section(client):
    async def _test():
        resp = await client.get("/system/naics_2022/62")
        # Should show children like 621, 622, etc.
        assert "621" in resp.text or "children" in resp.text.lower()
    _run(_test())


def test_node_page_has_breadcrumb(client):
    async def _test():
        resp = await client.get("/system/naics_2022/621")
        # Should show ancestor path: 62 → 621
        assert "62" in resp.text
        assert "621" in resp.text
    _run(_test())


def test_node_page_not_found(client):
    async def _test():
        resp = await client.get("/system/naics_2022/99999")
        assert resp.status_code == 404
    _run(_test())


# ── Static Files ─────────────────────────────────────────────


def test_css_served(client):
    async def _test():
        resp = await client.get("/static/css/observatory.css")
        assert resp.status_code == 200
        assert "text/css" in resp.headers.get("content-type", "")
    _run(_test())


def test_js_served(client):
    async def _test():
        resp = await client.get("/static/js/api-client.js")
        assert resp.status_code == 200
        assert "javascript" in resp.headers.get("content-type", "")
    _run(_test())
