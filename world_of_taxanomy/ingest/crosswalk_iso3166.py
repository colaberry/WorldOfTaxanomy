"""ISO 3166 crosswalk ingester.

Links iso_3166_1 country nodes to iso_3166_2 country stub nodes.
Both systems use ISO 3166-1 alpha-2 codes for countries (e.g. "US", "DE").
Every country code present in both systems gets a bidirectional exact edge.

~249 countries x 2 directions = ~498 equivalence edges.
"""


async def ingest_crosswalk_iso3166(conn) -> int:
    """Insert equivalence edges linking iso_3166_1 and iso_3166_2 country codes.

    Returns total number of edges inserted.
    """
    # Fetch all alpha-2 country codes present in iso_3166_1 (level 2 nodes)
    rows_1 = await conn.fetch(
        "SELECT code FROM classification_node WHERE system_id = 'iso_3166_1' AND level = 2"
    )
    codes_1 = {r["code"] for r in rows_1}

    # Fetch all alpha-2 country stub codes present in iso_3166_2 (level 0 nodes)
    rows_2 = await conn.fetch(
        "SELECT code FROM classification_node WHERE system_id = 'iso_3166_2' AND level = 0"
    )
    codes_2 = {r["code"] for r in rows_2}

    # Intersection - codes present in both
    common = sorted(codes_1 & codes_2)

    count = 0
    for code in common:
        # iso_3166_1 -> iso_3166_2
        await conn.execute(
            """INSERT INTO equivalence
                   (source_system, source_code, target_system, target_code, match_type)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT DO NOTHING""",
            "iso_3166_1", code, "iso_3166_2", code, "exact",
        )
        count += 1

        # iso_3166_2 -> iso_3166_1 (reverse)
        await conn.execute(
            """INSERT INTO equivalence
                   (source_system, source_code, target_system, target_code, match_type)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT DO NOTHING""",
            "iso_3166_2", code, "iso_3166_1", code, "exact",
        )
        count += 1

    return count
