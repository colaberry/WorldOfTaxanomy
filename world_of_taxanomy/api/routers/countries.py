"""Country taxonomy profile API router.

GET /api/v1/countries/{code}  - full taxonomy profile for a country
"""
from fastapi import APIRouter, HTTPException

from world_of_taxanomy.api.deps import get_conn
from world_of_taxanomy.query.browse import (
    get_country_sector_strengths,
    get_systems_for_country,
)

from fastapi import Depends

router = APIRouter(prefix="/countries", tags=["countries"])


@router.get("/{country_code}")
async def get_country_profile(
    country_code: str,
    conn=Depends(get_conn),
):
    """Return taxonomy profile for a country.

    Includes:
    - Country metadata (from iso_3166_1 if ingested)
    - Applicable classification systems ordered by relevance
      (official national system, regional bloc system, UN recommended, historical)
    - Known sector strengths (from the geo-sector crosswalk)

    country_code: ISO 3166-1 alpha-2 code (e.g. DE, PK, MX, ID, US)
    """
    code = country_code.upper()
    if len(code) != 2 or not code.isalpha():
        raise HTTPException(status_code=400, detail="country_code must be a 2-letter ISO 3166-1 alpha-2 code")

    # Country metadata from iso_3166_1 (best-effort - may not be present if not ingested)
    country_row = await conn.fetchrow(
        """SELECT code, title, parent_code
           FROM classification_node
           WHERE system_id = 'iso_3166_1' AND code = $1""",
        code,
    )

    # Applicable classification systems
    systems = await get_systems_for_country(conn, code)

    if not systems and country_row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for country code '{code}'. "
                   "Ensure iso_3166_1 and crosswalk_country_system have been ingested.",
        )

    # Sector strengths from geo-sector crosswalk
    sector_strengths = await get_country_sector_strengths(conn, code)

    country_info = {
        "code": code,
        "title": country_row["title"] if country_row else None,
        "parent_region": country_row["parent_code"] if country_row else None,
    }

    return {
        "country": country_info,
        "classification_systems": systems,
        "sector_strengths": sector_strengths,
    }
