"""Tests for MCP wiki integration.

TDD RED phase: verifies instructions in initialize response,
wiki resources in resources/list, and wiki resource reading.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from world_of_taxonomy.mcp.protocol import (
    build_resources_list,
    handle_jsonrpc_request,
)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


class TestMCPWikiContext:
    def test_initialize_includes_instructions(self):
        async def _test():
            request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "0.1"},
                },
            }
            response = await handle_jsonrpc_request(request, conn=None)
            assert "result" in response
            result = response["result"]
            assert "instructions" in result, "instructions field missing from initialize response"
            assert len(result["instructions"]) > 100

        _run(_test())

    def test_instructions_mentions_systems(self):
        async def _test():
            request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "0.1"},
                },
            }
            response = await handle_jsonrpc_request(request, conn=None)
            instructions = response["result"]["instructions"]
            assert "classification" in instructions.lower()

        _run(_test())

    def test_instructions_mentions_crosswalks(self):
        async def _test():
            request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "0.1"},
                },
            }
            response = await handle_jsonrpc_request(request, conn=None)
            instructions = response["result"]["instructions"]
            assert "crosswalk" in instructions.lower()

        _run(_test())


class TestMCPWikiResources:
    def test_resources_include_wiki_entries(self):
        resources = build_resources_list()
        uris = {r["uri"] for r in resources}
        wiki_uris = {u for u in uris if u.startswith("taxonomy://wiki/")}
        assert len(wiki_uris) > 0, "No taxonomy://wiki/* resources found"

    def test_wiki_resource_count(self):
        from world_of_taxonomy.wiki import load_wiki_meta
        meta = load_wiki_meta()
        resources = build_resources_list()
        wiki_resources = [r for r in resources if r["uri"].startswith("taxonomy://wiki/")]
        assert len(wiki_resources) == len(meta)

    def test_read_wiki_resource(self, db_pool):
        async def _test():
            async with db_pool.acquire() as conn:
                request = {
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "resources/read",
                    "params": {"uri": "taxonomy://wiki/getting-started"},
                }
                response = await handle_jsonrpc_request(request, conn=conn)
                assert response["id"] == 10
                assert "result" in response
                contents = response["result"]["contents"]
                assert len(contents) == 1
                assert contents[0]["mimeType"] == "text/markdown"
                assert len(contents[0]["text"]) > 100

        _run(_test())

    def test_read_wiki_resource_not_found(self, db_pool):
        async def _test():
            async with db_pool.acquire() as conn:
                request = {
                    "jsonrpc": "2.0",
                    "id": 11,
                    "method": "resources/read",
                    "params": {"uri": "taxonomy://wiki/nonexistent-page"},
                }
                response = await handle_jsonrpc_request(request, conn=conn)
                assert "error" in response

        _run(_test())
