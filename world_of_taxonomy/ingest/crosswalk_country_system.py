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

    # -----------------------------------------------------------------------
    # US official systems beyond industry
    # 'supplemental' = official in its own domain but not the primary
    # industry classifier; excluded from primary_system_id map coloring.
    # -----------------------------------------------------------------------
    ("US", "soc_2018",     "official", "US Bureau of Labor Statistics Standard Occupational Classification"),
    ("US", "onet_soc",     "official", "US DOL O*NET - detailed occupational information system"),
    ("US", "cip_2020",     "official", "US NCES Classification of Instructional Programs"),
    ("US", "cfr_title_49", "official", "US Code of Federal Regulations Title 49 - Transportation"),
    ("US", "fmcsa_regs",   "official", "US FMCSA motor carrier safety regulations"),
    ("US", "loinc",        "recommended",  "LOINC de facto standard for clinical observations in US healthcare"),
    ("US", "patent_cpc",   "recommended",  "USPTO uses CPC jointly with EPO for patent classification"),

    # -----------------------------------------------------------------------
    # Australia / New Zealand occupational
    # 'supplemental' so anzsic_2006 remains the primary industry color.
    # -----------------------------------------------------------------------
    ("AU", "anzsco_2022", "official", "Australian and New Zealand Standard Classification of Occupations"),
    ("NZ", "anzsco_2022", "official", "New Zealand co-maintains ANZSCO with Australia"),

    # -----------------------------------------------------------------------
    # EU / EEA - GDPR and ESCO (regional)
    # -----------------------------------------------------------------------
    ("AT", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("BE", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("BG", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("CY", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("CZ", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("DE", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("DK", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("EE", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("ES", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("FI", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("FR", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("GR", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("HR", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("HU", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("IE", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("IT", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("LT", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("LU", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("LV", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("MT", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("NL", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("PL", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("PT", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("RO", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("SE", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("SI", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("SK", "gdpr_articles", "regional", "EU member - GDPR applies directly"),
    ("GB", "gdpr_articles", "historical", "UK GDPR - retained post-Brexit with equivalent provisions"),
    ("IS", "gdpr_articles", "regional", "EEA member - GDPR applies via EEA Agreement"),
    ("LI", "gdpr_articles", "regional", "EEA member - GDPR applies via EEA Agreement"),
    ("NO", "gdpr_articles", "regional", "EEA member - GDPR applies via EEA Agreement"),

    ("AT", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("BE", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("BG", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("CY", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("CZ", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("DE", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("DK", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("EE", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("ES", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("FI", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("FR", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("GR", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("HR", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("HU", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("IE", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("IT", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("LT", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("LU", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("LV", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("MT", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("NL", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("PL", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("PT", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("RO", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("SE", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("SI", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),
    ("SK", "esco_occupations", "regional", "EU member - ESCO is the EU multilingual occupational taxonomy"),

    ("AT", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("BE", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("BG", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("CY", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("CZ", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("DE", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("DK", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("EE", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("ES", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("FI", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("FR", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("GR", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("HR", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("HU", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("IE", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("IT", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("LT", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("LU", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("LV", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("MT", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("NL", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("PL", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("PT", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("RO", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("SE", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("SI", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),
    ("SK", "esco_skills", "regional", "EU member - ESCO skills taxonomy"),

    # -----------------------------------------------------------------------
    # National industry classification systems (new countries)
    # -----------------------------------------------------------------------
    ("BR", "cnae_2012", "official",
     "CNAE 2.0 is Brazil's official national industrial classification, aligned to ISIC Rev 4"),
    ("CN", "csic_2017", "official",
     "CSIC GB/T 4754-2017 is China's official national industrial classification"),
    ("RU", "okved_2",   "official",
     "OKVED-2 (OK 029-2014) is Russia's official classification of economic activities, NACE/ISIC aligned"),
    ("ID", "kbli_2020", "official",
     "KBLI 2020 is Indonesia's official industrial classification, aligned to ISIC Rev 4"),
    ("MX", "scian_2018", "official",
     "SCIAN 2018 is Mexico's official industrial classification, the Spanish NAICS adaptation"),
    ("ZA", "sic_sa",    "official",
     "SIC-SA version 5 (2012) is South Africa's official industrial classification, ISIC Rev 4 aligned"),

    # -----------------------------------------------------------------------
    # EU national NACE adaptations - official for their country
    # -----------------------------------------------------------------------
    ("IT", "ateco_2007", "official",
     "ATECO 2007 is Italy's official national adaptation of NACE Rev 2 (ISTAT)"),
    ("FR", "naf_rev2",   "official",
     "NAF Rev 2 is France's official national adaptation of NACE Rev 2 (INSEE)"),
    ("PL", "pkd_2007",   "official",
     "PKD 2007 is Poland's official national adaptation of NACE Rev 2 (GUS)"),
    ("NL", "sbi_2008",   "official",
     "SBI 2008 is the Netherlands' official national adaptation of NACE Rev 2 (CBS)"),
    ("SE", "sni_2007",   "official",
     "SNI 2007 is Sweden's official national adaptation of NACE Rev 2 (Statistics Sweden)"),
    ("DK", "db07",       "official",
     "DB07 is Denmark's official national adaptation of NACE Rev 2 (Danmarks Statistik)"),
    ("FI", "tol_2008",   "official",
     "TOL 2008 is Finland's official national adaptation of NACE Rev 2 (Statistics Finland)"),
    ("PT", "cae_rev3",   "official",
     "CAE Rev 3 is Portugal's official national adaptation of NACE Rev 2 (INE Portugal)"),
    ("CZ", "cz_nace",    "official",
     "CZ-NACE is the Czech Republic's official national adaptation of NACE Rev 2 (CZSO)"),
    ("HU", "teaor_2008", "official",
     "TEAOR 2008 is Hungary's official national adaptation of NACE Rev 2 (KSH)"),
    ("RO", "caen_rev2",  "official",
     "CAEN Rev 2 is Romania's official national adaptation of NACE Rev 2 (INS Romania)"),
    ("HR", "nkd_2007",   "official",
     "NKD 2007 is Croatia's official national adaptation of NACE Rev 2 (DZS Croatia)"),
    ("SK", "sk_nace",    "official",
     "SK NACE Rev 2 is Slovakia's official national adaptation of NACE Rev 2 (Statistics Slovakia)"),
    ("BG", "nkid",       "official",
     "NKID is Bulgaria's official national adaptation of NACE Rev 2 (NSI Bulgaria)"),
    ("EE", "emtak",      "official",
     "EMTAK is Estonia's official national adaptation of NACE Rev 2 (Statistics Estonia)"),
    ("LT", "nace_lt",    "official",
     "EVRK is Lithuania's official national adaptation of NACE Rev 2 (Statistics Lithuania)"),
    ("LV", "nk_lv",      "official",
     "NK is Latvia's official national adaptation of NACE Rev 2 (CSB Latvia)"),
    ("TR", "nace_tr",    "official",
     "Turkey's official industrial classification is a direct NACE Rev 2 adaptation (TurkStat)"),

    # -----------------------------------------------------------------------
    # LATAM ISIC-derived systems - official for their country
    # -----------------------------------------------------------------------
    ("CO", "ciiu_co",  "official",
     "CIIU Rev 4 AC is Colombia's official national industrial classification (DANE)"),
    ("AR", "ciiu_ar",  "official",
     "CLANAE Rev 4 is Argentina's official national industrial classification (INDEC)"),
    ("CL", "ciiu_cl",  "official",
     "CIIU Rev 4 is Chile's official national industrial classification (INE Chile)"),
    ("PE", "ciiu_pe",  "official",
     "CIIU Rev 4 is Peru's official national industrial classification (INEI)"),
    ("EC", "ciiu_ec",  "official",
     "CIIU Rev 4 is Ecuador's official national industrial classification (INEC Ecuador)"),
    ("BO", "caeb",     "official",
     "CAEB is Bolivia's official national industrial classification (INE Bolivia)"),
    ("VE", "ciiu_ve",  "official",
     "CIIU Rev 4 is Venezuela's official national industrial classification (INE Venezuela)"),
    ("CR", "ciiu_cr",  "official",
     "CIIU Rev 4 is Costa Rica's official national industrial classification (INEC Costa Rica)"),
    ("GT", "ciiu_gt",  "official",
     "CIIU Rev 4 is Guatemala's official national industrial classification (INE Guatemala)"),
    ("PA", "ciiu_pa",  "official",
     "CIIU Rev 4 is Panama's official national industrial classification (INEC Panama)"),

    # -----------------------------------------------------------------------
    # Asia ISIC-derived systems - official for their country
    # -----------------------------------------------------------------------
    ("KR", "ksic_2017",  "official",
     "KSIC 2017 is South Korea's official industrial classification (Statistics Korea)"),
    ("SG", "ssic_2020",  "official",
     "SSIC 2020 is Singapore's official industrial classification (DOS Singapore)"),
    ("MY", "msic_2008",  "official",
     "MSIC 2008 is Malaysia's official industrial classification (DOSM Malaysia)"),
    ("TH", "tsic_2009",  "official",
     "TSIC 2009 is Thailand's official industrial classification (NSO Thailand)"),
    ("PH", "psic_2009",  "official",
     "PSIC 2009 is the Philippines' official industrial classification (PSA Philippines)"),
    ("VN", "vsic_2018",  "official",
     "VSIC 2018 is Vietnam's official industrial classification (GSO Vietnam)"),
    ("BD", "bsic",       "official",
     "BSIC is Bangladesh's official industrial classification (BBS Bangladesh)"),
    ("PK", "psic_pk",    "official",
     "PSIC is Pakistan's official industrial classification (PBS Pakistan)"),

    # -----------------------------------------------------------------------
    # Africa and Middle East ISIC-derived systems - official for their country
    # -----------------------------------------------------------------------
    ("NG", "isic_ng",  "official",
     "Nigeria Standard Industrial Classification is Nigeria's official system (NBS Nigeria)"),
    ("KE", "isic_ke",  "official",
     "Kenya Standard Industrial Classification is Kenya's official system (KNBS)"),
    ("EG", "isic_eg",  "official",
     "Egypt Standard Industrial Classification is Egypt's official system (CAPMAS)"),
    ("SA", "isic_sa",  "official",
     "Saudi Arabia Standard Industrial Classification is Saudi Arabia's official system (GASTAT)"),
    ("AE", "isic_ae",  "official",
     "UAE Standard Industrial Classification is the UAE's official system (FCSC)"),

    # -----------------------------------------------------------------------
    # France - occupational classification
    # -----------------------------------------------------------------------
    ("FR", "rome_v4", "official",
     "ROME v4 is France's official occupational reference (France Travail / Pole Emploi)"),

    # -----------------------------------------------------------------------
    # Canada - occupational classification
    # -----------------------------------------------------------------------
    ("CA", "noc_2021", "official",
     "NOC 2021 is Canada's official National Occupational Classification (Statistics Canada / ESDC)"),

    # -----------------------------------------------------------------------
    # United Kingdom - occupational classification
    # -----------------------------------------------------------------------
    ("GB", "uksoc_2020", "official",
     "UK SOC 2020 is the United Kingdom's official Standard Occupational Classification (ONS)"),

    # -----------------------------------------------------------------------
    # Germany - occupational classification and clinical coding
    # -----------------------------------------------------------------------
    ("DE", "kldb_2010", "official",
     "KldB 2010 is Germany's official Classification of Occupations (Bundesagentur fuer Arbeit)"),
    ("DE", "icd10_gm", "official",
     "ICD-10-GM is Germany's official clinical modification of ICD-10 (BfArM)"),

    # -----------------------------------------------------------------------
    # Australia / New Zealand - research fields and clinical coding
    # -----------------------------------------------------------------------
    ("AU", "anzsrc_for_2020", "official",
     "ANZSRC FOR 2020 is Australia and New Zealand's official research field classification (ABS)"),
    ("NZ", "anzsrc_for_2020", "official",
     "ANZSRC FOR 2020 is co-maintained by Stats NZ for New Zealand research classification"),
    ("AU", "icd10_am", "official",
     "ICD-10-AM is Australia's official clinical modification of ICD-10 (AIHW)"),
    ("NZ", "icd10_am", "official",
     "ICD-10-AM is used in New Zealand clinical settings (aligned with Australian edition)"),

    # -----------------------------------------------------------------------
    # Asia-Pacific - ADB sector classification (recommended for ADB members)
    # -----------------------------------------------------------------------
    ("AF", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("AM", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("AZ", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("BD", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("BT", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("CK", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("CN", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("FJ", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("FM", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("GE", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("ID", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("IN", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("JP", "adb_sector", "recommended", "ADB member - Asia-Pacific sector classification"),
    ("KG", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("KH", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("KI", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("KR", "adb_sector", "recommended", "ADB member - Asia-Pacific sector classification"),
    ("KZ", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("LA", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("LK", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("MH", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("MM", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("MN", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("MV", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("MY", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("NP", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("NR", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("PG", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("PH", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("PK", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("PW", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("SB", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("SG", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("TH", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("TJ", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("TL", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("TM", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("TO", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("TV", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("UZ", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("VN", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("VU", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    ("WS", "adb_sector", "recommended", "ADB developing member - Asia-Pacific sector classification"),
    # ── ISIC-derived national systems (Phase 1-3: 59 countries) ──
    ("LB", "isic_lb", "official", "Lebanon national adaptation of ISIC Rev 4"),
    ("OM", "isic_om", "official", "Oman national adaptation of ISIC Rev 4"),
    ("QA", "isic_qa", "official", "Qatar national adaptation of ISIC Rev 4"),
    ("BH", "isic_bh", "official", "Bahrain national adaptation of ISIC Rev 4"),
    ("KW", "isic_kw", "official", "Kuwait national adaptation of ISIC Rev 4"),
    ("YE", "isic_ye", "official", "Yemen national adaptation of ISIC Rev 4"),
    ("IR", "isic_ir", "official", "Iran national adaptation of ISIC Rev 4"),
    ("LY", "isic_ly", "official", "Libya national adaptation of ISIC Rev 4"),
    ("IL", "isic_il", "official", "Israel national adaptation of ISIC Rev 4"),
    ("PS", "isic_ps", "official", "Palestine national adaptation of ISIC Rev 4"),
    ("SY", "isic_sy", "official", "Syria national adaptation of ISIC Rev 4"),
    ("KG", "isic_kg", "official", "Kyrgyzstan national adaptation of ISIC Rev 4"),
    ("TJ", "isic_tj", "official", "Tajikistan national adaptation of ISIC Rev 4"),
    ("TM", "isic_tm", "official", "Turkmenistan national adaptation of ISIC Rev 4"),
    ("CU", "isic_cu", "official", "Cuba national adaptation of ISIC Rev 4"),
    ("BB", "isic_bb", "official", "Barbados national adaptation of ISIC Rev 4"),
    ("BS", "isic_bs", "official", "Bahamas national adaptation of ISIC Rev 4"),
    ("GY", "isic_gy", "official", "Guyana national adaptation of ISIC Rev 4"),
    ("SR", "isic_sr", "official", "Suriname national adaptation of ISIC Rev 4"),
    ("BZ", "isic_bz", "official", "Belize national adaptation of ISIC Rev 4"),
    ("AG", "isic_ag", "official", "Antigua and Barbuda national adaptation of ISIC Rev 4"),
    ("LC", "isic_lc", "official", "Saint Lucia national adaptation of ISIC Rev 4"),
    ("GD", "isic_gd", "official", "Grenada national adaptation of ISIC Rev 4"),
    ("VC", "isic_vc", "official", "Saint Vincent and the Grenadines national adaptation of ISIC Rev 4"),
    ("DM", "isic_dm", "official", "Dominica national adaptation of ISIC Rev 4"),
    ("KN", "isic_kn", "official", "Saint Kitts and Nevis national adaptation of ISIC Rev 4"),
    ("SS", "isic_ss", "official", "South Sudan national adaptation of ISIC Rev 4"),
    ("SO", "isic_so", "official", "Somalia national adaptation of ISIC Rev 4"),
    ("GN", "isic_gn", "official", "Guinea national adaptation of ISIC Rev 4"),
    ("SL", "isic_sl", "official", "Sierra Leone national adaptation of ISIC Rev 4"),
    ("LR", "isic_lr", "official", "Liberia national adaptation of ISIC Rev 4"),
    ("TG", "isic_tg", "official", "Togo national adaptation of ISIC Rev 4"),
    ("BJ", "isic_bj", "official", "Benin national adaptation of ISIC Rev 4"),
    ("NE", "isic_ne", "official", "Niger national adaptation of ISIC Rev 4"),
    ("TD", "isic_td", "official", "Chad national adaptation of ISIC Rev 4"),
    ("CD", "isic_cd", "official", "Democratic Republic of the Congo national adaptation of ISIC Rev 4"),
    ("AO", "isic_ao", "official", "Angola national adaptation of ISIC Rev 4"),
    ("GA", "isic_ga", "official", "Gabon national adaptation of ISIC Rev 4"),
    ("GQ", "isic_gq", "official", "Equatorial Guinea national adaptation of ISIC Rev 4"),
    ("CG", "isic_cg", "official", "Republic of the Congo national adaptation of ISIC Rev 4"),
    ("KM", "isic_km", "official", "Comoros national adaptation of ISIC Rev 4"),
    ("DJ", "isic_dj", "official", "Djibouti national adaptation of ISIC Rev 4"),
    ("CV", "isic_cv", "official", "Cabo Verde national adaptation of ISIC Rev 4"),
    ("GM", "isic_gm", "official", "Gambia national adaptation of ISIC Rev 4"),
    ("GW", "isic_gw", "official", "Guinea-Bissau national adaptation of ISIC Rev 4"),
    ("MR", "isic_mr", "official", "Mauritania national adaptation of ISIC Rev 4"),
    ("SZ", "isic_sz", "official", "Eswatini national adaptation of ISIC Rev 4"),
    ("LS", "isic_ls", "official", "Lesotho national adaptation of ISIC Rev 4"),
    ("BI", "isic_bi", "official", "Burundi national adaptation of ISIC Rev 4"),
    ("ER", "isic_er", "official", "Eritrea national adaptation of ISIC Rev 4"),
    ("SC", "isic_sc", "official", "Seychelles national adaptation of ISIC Rev 4"),
    ("WS", "isic_ws", "official", "Samoa national adaptation of ISIC Rev 4"),
    ("TO", "isic_to", "official", "Tonga national adaptation of ISIC Rev 4"),
    ("VU", "isic_vu", "official", "Vanuatu national adaptation of ISIC Rev 4"),
    ("SB", "isic_sb", "official", "Solomon Islands national adaptation of ISIC Rev 4"),
    ("BN", "isic_bn", "official", "Brunei national adaptation of ISIC Rev 4"),
    ("TL", "isic_tl", "official", "East Timor national adaptation of ISIC Rev 4"),
    ("BT", "isic_bt", "official", "Bhutan national adaptation of ISIC Rev 4"),
    ("MV", "isic_mv", "official", "Maldives national adaptation of ISIC Rev 4"),
]

# ---------------------------------------------------------------------------
# Regional groupings used in the dynamic expansion inside the ingest function
# ---------------------------------------------------------------------------

# EU 27 member states
_EU_27: list[str] = [
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
    "FR", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT",
    "NL", "PL", "PT", "RO", "SE", "SI", "SK",
]

# EEA non-EU (also bound by most EU regulations / standards)
_EEA_EXTRA: list[str] = ["IS", "LI", "NO"]

# Combined EU + EEA (used for EU regulations that apply to EEA)
_EU_EEA: list[str] = _EU_27 + _EEA_EXTRA


async def ingest_crosswalk_country_system(conn) -> int:
    """Ingest Country-to-Classification-System applicability crosswalk.

    Populates the country_system_link table which maps ISO 3166-1 alpha-2
    country codes to classification system IDs, indicating which systems
    are officially used, regionally mandated, recommended, or historical
    references for each country.

    Combines two sources:
    1. COUNTRY_SYSTEM_LINKS - hand-coded specific/official links (takes precedence)
    2. Dynamic expansion based on classification_system.region:
       - 'Global*' region  -> recommended for every country that uses ISIC Rev 4
       - 'United States'   -> official for US
       - 'European Union'  -> regional for all EU 27 + EEA countries
       - 'Europe / Global' -> regional for EU 27 countries (e.g. ESCO)
       - 'Asia-Pacific'    -> recommended for ADB member countries

    Only inserts rows where the system_id exists in classification_system.
    Returns count of unique country-system pairs inserted/attempted.
    """
    # All systems in DB with their regions
    system_rows = await conn.fetch("SELECT id, region FROM classification_system")
    existing: dict[str, str] = {r["id"]: r["region"] or "" for r in system_rows}

    # Countries that use ISIC (our proxy for 'every country in the world')
    isic_countries: list[str] = [
        cc for cc, sid, _, _ in COUNTRY_SYSTEM_LINKS if sid == "isic_rev4"
    ]

    # Build dynamic links based on system region
    dynamic: list[tuple[str, str, str, str]] = []
    for sys_id, region in existing.items():
        if "Global" in region:
            # All Global-* systems recommended for every country
            for cc in isic_countries:
                dynamic.append((cc, sys_id, "recommended", f"{region} standard"))
        elif region == "United States":
            # All US-region systems are official for the US
            dynamic.append(("US", sys_id, "official",
                             "US official or primary classification system"))
        elif region in ("European Union", "European Union (27 countries)"):
            # EU regulations/standards apply to all EU + EEA members
            for cc in _EU_EEA:
                dynamic.append((cc, sys_id, "regional",
                                 "European Union regulation or standard"))
        elif region == "Europe / Global":
            # EU-origin standards (e.g. ESCO) apply to EU 27
            for cc in _EU_27:
                dynamic.append((cc, sys_id, "regional",
                                 "EU/European standard"))
        elif region == "Asia-Pacific":
            # ADB sector classification recommended for ADB member countries
            # (already hand-coded above for specific country codes, but this
            # handles any future Asia-Pacific systems automatically)
            pass  # handled via hand-coded COUNTRY_SYSTEM_LINKS for adb_sector

    # Combine: hand-coded first (takes precedence in deduplication), then dynamic
    all_links: list[tuple[str, str, str, str]] = list(COUNTRY_SYSTEM_LINKS) + dynamic

    # Deduplicate by (country_code, system_id) - first occurrence wins
    seen: set[tuple[str, str]] = set()
    rows: list[tuple[str, str, str, str]] = []
    for entry in all_links:
        cc, sid, rel, note = entry
        if (cc, sid) not in seen and sid in existing:
            seen.add((cc, sid))
            rows.append((cc, sid, rel, note or ""))

    if rows:
        await conn.executemany(
            """INSERT INTO country_system_link
                   (country_code, system_id, relevance, notes)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (country_code, system_id) DO NOTHING""",
            rows,
        )

    print(f"  Linked {len(rows)} country-system pairs "
          f"({len(COUNTRY_SYSTEM_LINKS)} hand-coded + {len(rows) - len(COUNTRY_SYSTEM_LINKS)} dynamic)")
    return len(rows)
