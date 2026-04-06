"""FastAPI application factory."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from world_of_taxanomy.api.routers import systems, nodes, search, equivalences


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="WorldOfTaxanomy",
        description=(
            "Unified global industry classification knowledge graph. "
            "Federation model connecting NAICS, ISIC, NACE, and more."
        ),
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # Static files
    static_dir = Path(__file__).parent.parent / "web" / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # API routers
    app.include_router(systems.router)
    app.include_router(nodes.router)
    app.include_router(search.router)
    app.include_router(equivalences.router)

    # Web frontend routes (must be after API routes to avoid path conflicts)
    from world_of_taxanomy.web.routes import router as web_router
    app.include_router(web_router)

    return app
