"""Tests for the /api/v1/metrics Prometheus endpoint."""

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from world_of_taxonomy.api.app import create_app


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


@pytest.fixture
def app(db_pool, monkeypatch):
    monkeypatch.delenv("METRICS_TOKEN", raising=False)
    application = create_app()
    application.state.pool = db_pool
    return application


@pytest.fixture
def client(app):
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


def test_metrics_endpoint_returns_prometheus_text(client):
    async def _test():
        await client.get("/api/v1/systems")
        resp = await client.get("/api/v1/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        body = resp.text
        assert "wot_http_requests_total" in body
        assert "wot_http_request_latency_seconds" in body
        await client.aclose()

    _run(_test())


def test_metrics_endpoint_requires_token_when_set(db_pool, monkeypatch):
    async def _test():
        monkeypatch.setenv("METRICS_TOKEN", "secret-shh")
        application = create_app()
        application.state.pool = db_pool
        transport = ASGITransport(app=application)
        async with AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            resp = await c.get("/api/v1/metrics")
            assert resp.status_code == 401

            resp = await c.get(
                "/api/v1/metrics", headers={"X-Metrics-Token": "secret-shh"}
            )
            assert resp.status_code == 200

    _run(_test())


def test_metrics_do_not_self_increment(client):
    async def _test():
        first = (await client.get("/api/v1/metrics")).text
        second = (await client.get("/api/v1/metrics")).text
        # /metrics path must not be a route label.
        assert "/api/v1/metrics" not in first
        assert "/api/v1/metrics" not in second
        await client.aclose()

    _run(_test())
