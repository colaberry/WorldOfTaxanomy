"""Tests for the CSP violation report sink."""

import asyncio
import json

import pytest
from httpx import ASGITransport, AsyncClient

from world_of_taxonomy.api.app import create_app
from world_of_taxonomy.api.csp_report import CSP_REPORTS


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def _value(directive: str) -> float:
    return CSP_REPORTS.labels(directive=directive)._value.get()


def test_csp_report_legacy_format_counts(db_pool):
    async def _test():
        application = create_app()
        application.state.pool = db_pool
        transport = ASGITransport(app=application)
        async with AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            before = _value("script-src")
            body = {
                "csp-report": {
                    "document-uri": "https://worldoftaxonomy.com/",
                    "violated-directive": "script-src 'self'",
                    "effective-directive": "script-src",
                    "blocked-uri": "inline",
                }
            }
            resp = await c.post(
                "/api/v1/csp-report",
                content=json.dumps(body),
                headers={"Content-Type": "application/csp-report"},
            )
            assert resp.status_code == 204
            assert _value("script-src") == before + 1

    _run(_test())


def test_csp_report_reporting_api_format_counts(db_pool):
    async def _test():
        application = create_app()
        application.state.pool = db_pool
        transport = ASGITransport(app=application)
        async with AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            before = _value("img-src")
            body = [
                {
                    "type": "csp-violation",
                    "body": {
                        "documentURL": "https://worldoftaxonomy.com/",
                        "effectiveDirective": "img-src",
                        "blockedURL": "https://evil.example/tracker.gif",
                    },
                }
            ]
            resp = await c.post(
                "/api/v1/csp-report",
                content=json.dumps(body),
                headers={"Content-Type": "application/reports+json"},
            )
            assert resp.status_code == 204
            assert _value("img-src") == before + 1

    _run(_test())


def test_csp_report_unknown_directive_bucketed(db_pool):
    async def _test():
        application = create_app()
        application.state.pool = db_pool
        transport = ASGITransport(app=application)
        async with AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            before = _value("other")
            body = {"csp-report": {"effective-directive": "fake-invented-directive"}}
            resp = await c.post(
                "/api/v1/csp-report",
                content=json.dumps(body),
                headers={"Content-Type": "application/csp-report"},
            )
            assert resp.status_code == 204
            assert _value("other") == before + 1

    _run(_test())


def test_csp_report_malformed_body_still_204(db_pool):
    async def _test():
        application = create_app()
        application.state.pool = db_pool
        transport = ASGITransport(app=application)
        async with AsyncClient(
            transport=transport, base_url="http://test"
        ) as c:
            resp = await c.post(
                "/api/v1/csp-report",
                content=b"not json at all",
                headers={"Content-Type": "application/csp-report"},
            )
            assert resp.status_code == 204

    _run(_test())
