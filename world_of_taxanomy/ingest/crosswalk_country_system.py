"""Country-to-Classification-System applicability crosswalk.

Maps ISO 3166-1 alpha-2 country codes to classification system IDs,
indicating which systems are officially used, regionally mandated,
globally recommended, or historically referenced per country.

relevance values:
  'official'     - the country's own national standard (WZ 2008 for DE, NIC 2008 for IN)
  'regional'     - part of a regional adoption bloc (NACE Rev 2 for all EU/EEA members)
  'recommended'  - UN/international standard applicable globally (ISIC Rev 4 for all)
  'historical'   - legacy system, still widely referenced (SIC 1987 for US/UK)

Hand-coded from ISO, UN, Eurostat, and national statistics bureau documentation.
"""
from __future__ import annotations

from typing import Optional

# (country_code, system_id, relevance, notes)
COUNTRY_SYSTEM_LINKS: list[tuple[str, str, str, Optional[str]]] = [

    # -----------------------------------------------------------------------
    # NAICS 2022 - North America (official for US, CA, MX - co-developed)
    # -----------------------------------------------------------------------
    ("US", "naics_2022", "official",
     "USA co-developed NAICS with Canada and Mexico; primary US federal statistical standard"),
    ("CA", "naics_2022", "official",
     "Canada co-developed NAICS; Statistics Canada uses it for the Business Register"),
    ("MX", "naics_2022", "official",
     "Mexico uses SCIAN (Sistema de Clasificacion Industrial), the Spanish NAICS adaptation"),

    # -----------------------------------------------------------------------
    # SIC 1987 - historical (US SEC filings, UK Companies House)
    # -----------------------------------------------------------------------
    ("US", "sic_1987", "historical",
     "US SEC still accepts SIC codes; predecessor to NAICS"),
    ("GB", "sic_1987", "historical",
     "UK Companies House uses SIC 2007 (UK SIC), closely aligned to SIC 1987"),

    # -----------------------------------------------------------------------
    # NACE Rev 2 - EU 27 member states (mandatory Eurostat reporting)
    # -----------------------------------------------------------------------
    ("AT", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("BE", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("BG", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("CY", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("CZ", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("DE", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("DK", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("EE", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("ES", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("FI", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("FR", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("GR", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("HR", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("HU", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("IE", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("IT", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("LT", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("LU", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("LV", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("MT", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("NL", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("PL", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("PT", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("RO", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("SE", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("SI", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    ("SK", "nace_rev2", "regional", "EU member - Eurostat NACE Rev 2 mandatory"),
    # EEA non-EU members also use NACE
    ("IS", "nace_rev2", "regional", "EEA member - aligned to NACE Rev 2"),
    ("LI", "nace_rev2", "regional", "EEA member - aligned to NACE Rev 2"),
    ("NO", "nace_rev2", "regional", "EEA member - aligned to NACE Rev 2"),
    # EU candidate/aligned countries
    ("CH", "nace_rev2", "regional", "Switzerland aligned to NACE via bilateral agreements"),
    ("GB", "nace_rev2", "regional", "UK SIC 2007 is a direct NACE Rev 2 adaptation"),

    # -----------------------------------------------------------------------
    # National NACE adaptations (official for their country, regional still applies)
    # -----------------------------------------------------------------------
    ("DE", "wz_2008",   "official", "German national adaptation of NACE Rev 2 (Wirtschaftszweige)"),
    ("AT", "onace_2008","official", "Austrian national adaptation of NACE Rev 2 (ONACE 2008)"),
    ("CH", "noga_2008", "official", "Swiss national adaptation of NACE Rev 2 (NOGA 2008)"),

    # -----------------------------------------------------------------------
    # ANZSIC 2006 - Australia and New Zealand (official)
    # -----------------------------------------------------------------------
    ("AU", "anzsic_2006", "official",
     "Australian and New Zealand Standard Industrial Classification - ABS official standard"),
    ("NZ", "anzsic_2006", "official",
     "New Zealand co-maintains ANZSIC with Australia; Stats NZ uses it"),

    # -----------------------------------------------------------------------
    # NIC 2008 - India (official)
    # -----------------------------------------------------------------------
    ("IN", "nic_2008", "official",
     "National Industrial Classification 2008 - India's official standard, ISIC-aligned"),

    # -----------------------------------------------------------------------
    # JSIC 2013 - Japan (official)
    # -----------------------------------------------------------------------
    ("JP", "jsic_2013", "official",
     "Japan Standard Industrial Classification - Statistics Bureau of Japan official standard"),

    # -----------------------------------------------------------------------
    # ISIC Rev 4 - UN global recommended standard (all countries)
    # Every country in ISO 3166-1 should map here with 'recommended'
    # -----------------------------------------------------------------------
    ("AD", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AE", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AF", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AG", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AI", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AL", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AO", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AQ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AR", "isic_rev4", "recommended", "Argentina uses CLAE aligned to ISIC Rev 4"),
    ("AS", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AT", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AU", "isic_rev4", "recommended", "ANZSIC 2006 is aligned to ISIC Rev 4"),
    ("AW", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AX", "isic_rev4", "recommended", "UN recommended global standard"),
    ("AZ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BA", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BB", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BD", "isic_rev4", "recommended", "Bangladesh BSIC aligned to ISIC Rev 4"),
    ("BE", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BF", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BG", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BH", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BI", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BJ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BL", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BN", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BO", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BQ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BR", "isic_rev4", "recommended", "Brazil CNAE 2.0 is ISIC Rev 4 aligned"),
    ("BS", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BT", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BV", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BW", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BY", "isic_rev4", "recommended", "UN recommended global standard"),
    ("BZ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CA", "isic_rev4", "recommended", "NAICS and ISIC are crosswalked; Canada reports to OECD using ISIC"),
    ("CC", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CD", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CF", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CG", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CH", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CI", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CK", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CL", "isic_rev4", "recommended", "Chile CIIU is ISIC Rev 4 aligned"),
    ("CM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CN", "isic_rev4", "recommended", "China CSIC is ISIC Rev 4 aligned"),
    ("CO", "isic_rev4", "recommended", "Colombia CIIU Rev 4 is ISIC Rev 4 aligned"),
    ("CR", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CU", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CV", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CW", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CX", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CY", "isic_rev4", "recommended", "UN recommended global standard"),
    ("CZ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("DE", "isic_rev4", "recommended", "UN recommended global standard"),
    ("DJ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("DK", "isic_rev4", "recommended", "UN recommended global standard"),
    ("DM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("DO", "isic_rev4", "recommended", "UN recommended global standard"),
    ("DZ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("EC", "isic_rev4", "recommended", "Ecuador CIIU Rev 4 is ISIC Rev 4 aligned"),
    ("EE", "isic_rev4", "recommended", "UN recommended global standard"),
    ("EG", "isic_rev4", "recommended", "Egypt ISIC-aligned national system"),
    ("EH", "isic_rev4", "recommended", "UN recommended global standard"),
    ("ER", "isic_rev4", "recommended", "UN recommended global standard"),
    ("ES", "isic_rev4", "recommended", "UN recommended global standard"),
    ("ET", "isic_rev4", "recommended", "UN recommended global standard"),
    ("FI", "isic_rev4", "recommended", "UN recommended global standard"),
    ("FJ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("FK", "isic_rev4", "recommended", "UN recommended global standard"),
    ("FM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("FO", "isic_rev4", "recommended", "UN recommended global standard"),
    ("FR", "isic_rev4", "recommended", "France NAF is NACE/ISIC aligned"),
    ("GA", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GB", "isic_rev4", "recommended", "UK SIC 2007 is ISIC Rev 4 aligned"),
    ("GD", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GE", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GF", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GG", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GH", "isic_rev4", "recommended", "Ghana GSIC is ISIC aligned"),
    ("GI", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GL", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GN", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GP", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GQ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GR", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GS", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GT", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GU", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GW", "isic_rev4", "recommended", "UN recommended global standard"),
    ("GY", "isic_rev4", "recommended", "UN recommended global standard"),
    ("HK", "isic_rev4", "recommended", "Hong Kong uses HSIC based on ISIC Rev 4"),
    ("HM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("HN", "isic_rev4", "recommended", "UN recommended global standard"),
    ("HR", "isic_rev4", "recommended", "UN recommended global standard"),
    ("HT", "isic_rev4", "recommended", "UN recommended global standard"),
    ("HU", "isic_rev4", "recommended", "UN recommended global standard"),
    ("ID", "isic_rev4", "recommended", "Indonesia KBLI 2020 is ISIC Rev 4 aligned"),
    ("IE", "isic_rev4", "recommended", "UN recommended global standard"),
    ("IL", "isic_rev4", "recommended", "Israel ISIC-aligned classification"),
    ("IM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("IN", "isic_rev4", "recommended", "NIC 2008 is India's ISIC Rev 4 aligned system"),
    ("IO", "isic_rev4", "recommended", "UN recommended global standard"),
    ("IQ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("IR", "isic_rev4", "recommended", "UN recommended global standard"),
    ("IS", "isic_rev4", "recommended", "UN recommended global standard"),
    ("IT", "isic_rev4", "recommended", "Italy ATECO is NACE/ISIC aligned"),
    ("JE", "isic_rev4", "recommended", "UN recommended global standard"),
    ("JM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("JO", "isic_rev4", "recommended", "UN recommended global standard"),
    ("JP", "isic_rev4", "recommended", "Japan JSIC aligned to ISIC; Japan reports ISIC to UN"),
    ("KE", "isic_rev4", "recommended", "Kenya KSIC is ISIC Rev 4 aligned"),
    ("KG", "isic_rev4", "recommended", "UN recommended global standard"),
    ("KH", "isic_rev4", "recommended", "UN recommended global standard"),
    ("KI", "isic_rev4", "recommended", "UN recommended global standard"),
    ("KM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("KN", "isic_rev4", "recommended", "UN recommended global standard"),
    ("KP", "isic_rev4", "recommended", "UN recommended global standard"),
    ("KR", "isic_rev4", "recommended", "South Korea KSIC is ISIC Rev 4 aligned"),
    ("KW", "isic_rev4", "recommended", "UN recommended global standard"),
    ("KY", "isic_rev4", "recommended", "UN recommended global standard"),
    ("KZ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("LA", "isic_rev4", "recommended", "UN recommended global standard"),
    ("LB", "isic_rev4", "recommended", "UN recommended global standard"),
    ("LC", "isic_rev4", "recommended", "UN recommended global standard"),
    ("LI", "isic_rev4", "recommended", "UN recommended global standard"),
    ("LK", "isic_rev4", "recommended", "Sri Lanka SIC is ISIC aligned"),
    ("LR", "isic_rev4", "recommended", "UN recommended global standard"),
    ("LS", "isic_rev4", "recommended", "UN recommended global standard"),
    ("LT", "isic_rev4", "recommended", "UN recommended global standard"),
    ("LU", "isic_rev4", "recommended", "UN recommended global standard"),
    ("LV", "isic_rev4", "recommended", "UN recommended global standard"),
    ("LY", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MA", "isic_rev4", "recommended", "Morocco CIMA is ISIC aligned"),
    ("MC", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MD", "isic_rev4", "recommended", "UN recommended global standard"),
    ("ME", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MF", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MG", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MH", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MK", "isic_rev4", "recommended", "UN recommended global standard"),
    ("ML", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MN", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MO", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MP", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MQ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MR", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MS", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MT", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MU", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MV", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MW", "isic_rev4", "recommended", "UN recommended global standard"),
    ("MX", "isic_rev4", "recommended", "Mexico SCIAN/NAICS crosswalked to ISIC for OECD reporting"),
    ("MY", "isic_rev4", "recommended", "Malaysia MSIC is ISIC Rev 4 aligned"),
    ("MZ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("NA", "isic_rev4", "recommended", "UN recommended global standard"),
    ("NC", "isic_rev4", "recommended", "UN recommended global standard"),
    ("NE", "isic_rev4", "recommended", "UN recommended global standard"),
    ("NF", "isic_rev4", "recommended", "UN recommended global standard"),
    ("NG", "isic_rev4", "recommended", "Nigeria ISIC-aligned national classification"),
    ("NI", "isic_rev4", "recommended", "UN recommended global standard"),
    ("NL", "isic_rev4", "recommended", "UN recommended global standard"),
    ("NO", "isic_rev4", "recommended", "UN recommended global standard"),
    ("NP", "isic_rev4", "recommended", "Nepal NSIC is ISIC aligned"),
    ("NR", "isic_rev4", "recommended", "UN recommended global standard"),
    ("NU", "isic_rev4", "recommended", "UN recommended global standard"),
    ("NZ", "isic_rev4", "recommended", "ANZSIC 2006 is ISIC Rev 4 aligned"),
    ("OM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("PA", "isic_rev4", "recommended", "UN recommended global standard"),
    ("PE", "isic_rev4", "recommended", "Peru CIIU Rev 4 is ISIC aligned"),
    ("PF", "isic_rev4", "recommended", "UN recommended global standard"),
    ("PG", "isic_rev4", "recommended", "UN recommended global standard"),
    ("PH", "isic_rev4", "recommended", "Philippines PSIC is ISIC Rev 4 aligned"),
    ("PK", "isic_rev4", "recommended",
     "Pakistan PSIC 2010 is ISIC Rev 4 aligned - use isic_rev4 codes for Pakistani companies"),
    ("PL", "isic_rev4", "recommended", "UN recommended global standard"),
    ("PM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("PN", "isic_rev4", "recommended", "UN recommended global standard"),
    ("PR", "isic_rev4", "recommended", "Puerto Rico uses NAICS as US territory"),
    ("PS", "isic_rev4", "recommended", "UN recommended global standard"),
    ("PT", "isic_rev4", "recommended", "Portugal CAE is NACE/ISIC aligned"),
    ("PW", "isic_rev4", "recommended", "UN recommended global standard"),
    ("PY", "isic_rev4", "recommended", "UN recommended global standard"),
    ("QA", "isic_rev4", "recommended", "UN recommended global standard"),
    ("RE", "isic_rev4", "recommended", "UN recommended global standard"),
    ("RO", "isic_rev4", "recommended", "UN recommended global standard"),
    ("RS", "isic_rev4", "recommended", "UN recommended global standard"),
    ("RU", "isic_rev4", "recommended", "Russia OKVED 2 is NACE/ISIC aligned"),
    ("RW", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SA", "isic_rev4", "recommended", "Saudi Arabia ISIC-aligned classification"),
    ("SB", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SC", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SD", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SE", "isic_rev4", "recommended", "Sweden SNI is NACE/ISIC aligned"),
    ("SG", "isic_rev4", "recommended", "Singapore SSIC is ISIC Rev 4 aligned"),
    ("SH", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SI", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SJ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SK", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SL", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SN", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SO", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SR", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SS", "isic_rev4", "recommended", "UN recommended global standard"),
    ("ST", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SV", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SX", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SY", "isic_rev4", "recommended", "UN recommended global standard"),
    ("SZ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TC", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TD", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TF", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TG", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TH", "isic_rev4", "recommended", "Thailand TSIC is ISIC Rev 4 aligned"),
    ("TJ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TK", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TL", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TN", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TO", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TR", "isic_rev4", "recommended", "Turkey NACE Rev 2 aligned national system"),
    ("TT", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TV", "isic_rev4", "recommended", "UN recommended global standard"),
    ("TW", "isic_rev4", "recommended", "Taiwan CSIC is ISIC Rev 4 aligned"),
    ("TZ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("UA", "isic_rev4", "recommended", "Ukraine KVED is NACE/ISIC aligned"),
    ("UG", "isic_rev4", "recommended", "UN recommended global standard"),
    ("UM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("US", "isic_rev4", "recommended", "NAICS and ISIC crosswalked; USA reports to OECD/UN using ISIC"),
    ("UY", "isic_rev4", "recommended", "UN recommended global standard"),
    ("UZ", "isic_rev4", "recommended", "UN recommended global standard"),
    ("VA", "isic_rev4", "recommended", "UN recommended global standard"),
    ("VC", "isic_rev4", "recommended", "UN recommended global standard"),
    ("VE", "isic_rev4", "recommended", "UN recommended global standard"),
    ("VG", "isic_rev4", "recommended", "UN recommended global standard"),
    ("VI", "isic_rev4", "recommended", "US Virgin Islands uses NAICS as US territory"),
    ("VN", "isic_rev4", "recommended", "Vietnam VSIC is ISIC Rev 4 aligned"),
    ("VU", "isic_rev4", "recommended", "UN recommended global standard"),
    ("WF", "isic_rev4", "recommended", "UN recommended global standard"),
    ("WS", "isic_rev4", "recommended", "UN recommended global standard"),
    ("YE", "isic_rev4", "recommended", "UN recommended global standard"),
    ("YT", "isic_rev4", "recommended", "UN recommended global standard"),
    ("ZA", "isic_rev4", "recommended", "South Africa SIC is ISIC Rev 4 aligned"),
    ("ZM", "isic_rev4", "recommended", "UN recommended global standard"),
    ("ZW", "isic_rev4", "recommended", "UN recommended global standard"),
]


async def ingest_crosswalk_country_system(conn) -> int:
    """Ingest Country-to-Classification-System applicability crosswalk.

    Populates the country_system_link table which maps ISO 3166-1 alpha-2
    country codes to classification system IDs, indicating which systems
    are officially used, regionally mandated, recommended, or historical
    references for each country.

    Only inserts rows where the system_id exists in classification_system.
    Rows referencing systems not yet ingested are silently skipped via
    the ON CONFLICT DO NOTHING + FK constraint pattern.

    Returns count of rows inserted.
    """
    # Filter to only systems that exist in the DB (avoids FK violation)
    existing = {
        row["id"]
        for row in await conn.fetch("SELECT id FROM classification_system")
    }

    rows = [
        (cc, sid, rel, note or "")
        for cc, sid, rel, note in COUNTRY_SYSTEM_LINKS
        if sid in existing
    ]

    if rows:
        await conn.executemany(
            """INSERT INTO country_system_link
                   (country_code, system_id, relevance, notes)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (country_code, system_id) DO NOTHING""",
            rows,
        )

    return len(COUNTRY_SYSTEM_LINKS)
