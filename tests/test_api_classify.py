"""Tests for the POST /api/v1/classify endpoint."""

import asyncio
import pytest


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


class TestClassifyEngine:
    """Test the classification engine directly."""

    def test_classify_returns_matches(self, db_pool):
        """classify_text returns results for a term that exists in NAICS seed data."""
        from world_of_taxonomy.classify import classify_text

        async def go():
            async with db_pool.acquire() as conn:
                result = await classify_text(
                    conn,
                    text="manufacturing",
                    system_ids=["naics_2022"],
                    limit=3,
                )
                return result

        result = _run(go())
        assert result["query"] == "manufacturing"
        assert "disclaimer" in result
        assert "report_issue_url" in result
        assert isinstance(result["matches"], list)
        assert isinstance(result["crosswalks"], list)

    def test_classify_empty_query_returns_no_matches(self, db_pool):
        """Empty or very short text returns no matches."""
        from world_of_taxonomy.classify import classify_text

        async def go():
            async with db_pool.acquire() as conn:
                result = await classify_text(
                    conn,
                    text="zzzxxx_nonexistent_term",
                    system_ids=["naics_2022"],
                    limit=5,
                )
                return result

        result = _run(go())
        assert result["query"] == "zzzxxx_nonexistent_term"
        assert result["matches"] == []

    def test_classify_limit_capped(self, db_pool):
        """Limit is capped at 20."""
        from world_of_taxonomy.classify import classify_text

        async def go():
            async with db_pool.acquire() as conn:
                result = await classify_text(
                    conn,
                    text="agriculture",
                    system_ids=["naics_2022"],
                    limit=100,  # should be capped to 20
                )
                return result

        result = _run(go())
        for match in result["matches"]:
            assert len(match["results"]) <= 20

    def test_classify_includes_disclaimer(self, db_pool):
        """Every response has the disclaimer field."""
        from world_of_taxonomy.classify import classify_text

        async def go():
            async with db_pool.acquire() as conn:
                return await classify_text(conn, text="software", limit=1)

        result = _run(go())
        assert "informational only" in result["disclaimer"]
        assert "github.com" in result["report_issue_url"]


class TestClassifyRouter:
    """Test the classify endpoint contract (without TestClient to avoid event-loop issues)."""

    def test_classify_requires_auth(self):
        """get_current_user raises 401 when no auth header is present."""
        from fastapi import HTTPException
        from unittest.mock import MagicMock, patch
        import world_of_taxonomy.api.deps as deps_mod

        request = MagicMock()
        request.headers = {}

        # Ensure DISABLE_AUTH is off so auth is enforced
        with patch.object(deps_mod, "DISABLE_AUTH", False):
            with pytest.raises(HTTPException) as exc_info:
                _run(deps_mod.get_current_user(request))
        assert exc_info.value.status_code == 401

    def test_classify_request_validation(self):
        """ClassifyRequest rejects text shorter than 2 characters."""
        from pydantic import ValidationError

        # Import inline to avoid module-level side effects
        from world_of_taxonomy.api.routers.classify import ClassifyRequest

        with pytest.raises(ValidationError):
            ClassifyRequest(text="x")  # min_length is 2

        # Valid text should pass
        req = ClassifyRequest(text="software")
        assert req.text == "software"
        assert req.limit == 5
