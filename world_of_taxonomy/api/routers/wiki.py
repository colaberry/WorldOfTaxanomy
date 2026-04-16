"""Wiki API router - serves curated guide pages."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from world_of_taxonomy.wiki import load_wiki_meta, load_wiki_page
from world_of_taxonomy.api.schemas import WikiPageSummary, WikiPageDetail

router = APIRouter(prefix="/api/v1/wiki", tags=["wiki"])


@router.get("", response_model=list[WikiPageSummary])
async def list_wiki_pages():
    """List all wiki guide pages."""
    meta = load_wiki_meta()
    return [
        WikiPageSummary(
            slug=entry["slug"],
            title=entry["title"],
            description=entry["description"],
        )
        for entry in meta
    ]


@router.get("/{slug}", response_model=WikiPageDetail)
async def get_wiki_page(slug: str):
    """Get a single wiki page by slug."""
    meta = load_wiki_meta()
    entry = next((e for e in meta if e["slug"] == slug), None)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Wiki page '{slug}' not found")

    content = load_wiki_page(slug)
    if content is None:
        raise HTTPException(status_code=404, detail=f"Wiki page '{slug}' not found")

    return WikiPageDetail(
        slug=entry["slug"],
        title=entry["title"],
        description=entry["description"],
        content_markdown=content,
    )
