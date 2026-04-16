"""US FIPS State and County Codes ingester.

Federal Information Processing Standards Publication (FIPS) state and county
numeric codes maintained by NIST and the US Census Bureau.

Two-level structure: 56 state/territory codes (2-digit) and major county
codes (5-digit: 2-digit state + 3-digit county).

Source: NIST FIPS 5-2, US Census Bureau - public domain US government work
"""
from __future__ import annotations

US_STATES: list[tuple[str, str]] = [
    ("01", "Alabama"),
    ("02", "Alaska"),
    ("04", "Arizona"),
    ("05", "Arkansas"),
    ("06", "California"),
    ("08", "Colorado"),
    ("09", "Connecticut"),
    ("10", "Delaware"),
    ("11", "District of Columbia"),
    ("12", "Florida"),
    ("13", "Georgia"),
    ("15", "Hawaii"),
    ("16", "Idaho"),
    ("17", "Illinois"),
    ("18", "Indiana"),
    ("19", "Iowa"),
    ("20", "Kansas"),
    ("21", "Kentucky"),
    ("22", "Louisiana"),
    ("23", "Maine"),
    ("24", "Maryland"),
    ("25", "Massachusetts"),
    ("26", "Michigan"),
    ("27", "Minnesota"),
    ("28", "Mississippi"),
    ("29", "Missouri"),
    ("30", "Montana"),
    ("31", "Nebraska"),
    ("32", "Nevada"),
    ("33", "New Hampshire"),
    ("34", "New Jersey"),
    ("35", "New Mexico"),
    ("36", "New York"),
    ("37", "North Carolina"),
    ("38", "North Dakota"),
    ("39", "Ohio"),
    ("40", "Oklahoma"),
    ("41", "Oregon"),
    ("42", "Pennsylvania"),
    ("44", "Rhode Island"),
    ("45", "South Carolina"),
    ("46", "South Dakota"),
    ("47", "Tennessee"),
    ("48", "Texas"),
    ("49", "Utah"),
    ("50", "Vermont"),
    ("51", "Virginia"),
    ("53", "Washington"),
    ("54", "West Virginia"),
    ("55", "Wisconsin"),
    ("56", "Wyoming"),
    ("60", "American Samoa"),
    ("66", "Guam"),
    ("69", "Northern Mariana Islands"),
    ("72", "Puerto Rico"),
    ("78", "Virgin Islands"),
]

MAJOR_COUNTIES: list[tuple[str, str, str]] = [
    ("06037", "Los Angeles County, California", "06"),
    ("17031", "Cook County, Illinois", "17"),
    ("48201", "Harris County, Texas", "48"),
    ("04013", "Maricopa County, Arizona", "04"),
    ("06073", "San Diego County, California", "06"),
    ("06059", "Orange County, California", "06"),
    ("12086", "Miami-Dade County, Florida", "12"),
    ("06065", "Riverside County, California", "06"),
    ("48113", "Dallas County, Texas", "48"),
    ("36047", "Kings County (Brooklyn), New York", "36"),
    ("36061", "New York County (Manhattan), New York", "36"),
    ("36081", "Queens County, New York", "36"),
    ("36005", "Bronx County, New York", "36"),
    ("36085", "Richmond County (Staten Island), New York", "36"),
    ("06085", "Santa Clara County, California", "06"),
    ("53033", "King County, Washington", "53"),
    ("25025", "Suffolk County (Boston area), Massachusetts", "25"),
    ("11001", "District of Columbia", "11"),
    ("24510", "Baltimore City, Maryland", "24"),
    ("42101", "Philadelphia County, Pennsylvania", "42"),
    ("32003", "Clark County (Las Vegas), Nevada", "32"),
    ("35001", "Bernalillo County (Albuquerque), New Mexico", "35"),
    ("08031", "Denver County, Colorado", "08"),
    ("27053", "Hennepin County (Minneapolis), Minnesota", "27"),
    ("55079", "Milwaukee County, Wisconsin", "55"),
    ("39035", "Cuyahoga County (Cleveland), Ohio", "39"),
    ("37119", "Mecklenburg County (Charlotte), North Carolina", "37"),
    ("13121", "Fulton County (Atlanta), Georgia", "13"),
    ("12057", "Hillsborough County (Tampa), Florida", "12"),
    ("12095", "Orange County (Orlando), Florida", "12"),
]


async def ingest_us_fips(conn) -> int:
    """Ingest US FIPS state and county codes.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "us_fips",
        "US FIPS",
        "US Federal Information Processing Standards State and County Codes",
        "2023",
        "United States",
        "NIST / US Census Bureau",
    )

    count = 0
    for seq, (code, title) in enumerate(US_STATES, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "us_fips", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(MAJOR_COUNTIES, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "us_fips", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'us_fips'",
        count,
    )

    return count
