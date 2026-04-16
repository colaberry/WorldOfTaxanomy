"""EU NUTS 2021 (Nomenclature of Territorial Units for Statistics) ingester.

Eurostat NUTS 2021 classification - hierarchical regional breakdown of EU
and EEA member states for statistical purposes.

Two-level structure stored here: NUTS 0 (countries) and NUTS 1 (major
socio-economic regions). Full NUTS 2 and NUTS 3 require a data file download.

Source: Eurostat, NUTS 2021 regulation and classification tables
License: Eurostat open data (freely reusable with attribution)
"""
from __future__ import annotations

NUTS_COUNTRIES: list[tuple[str, str]] = [
    ("BE", "Belgium"),
    ("BG", "Bulgaria"),
    ("CZ", "Czechia"),
    ("DK", "Denmark"),
    ("DE", "Germany"),
    ("EE", "Estonia"),
    ("IE", "Ireland"),
    ("GR", "Greece"),
    ("ES", "Spain"),
    ("FR", "France"),
    ("HR", "Croatia"),
    ("IT", "Italy"),
    ("CY", "Cyprus"),
    ("LV", "Latvia"),
    ("LT", "Lithuania"),
    ("LU", "Luxembourg"),
    ("HU", "Hungary"),
    ("MT", "Malta"),
    ("NL", "Netherlands"),
    ("AT", "Austria"),
    ("PL", "Poland"),
    ("PT", "Portugal"),
    ("RO", "Romania"),
    ("SI", "Slovenia"),
    ("SK", "Slovakia"),
    ("FI", "Finland"),
    ("SE", "Sweden"),
    ("NO", "Norway"),
    ("CH", "Switzerland"),
    ("IS", "Iceland"),
    ("LI", "Liechtenstein"),
]

NUTS1: list[tuple[str, str, str]] = [
    ("BE1", "Bruxelles / Brussel (Brussels Capital Region)", "BE"),
    ("BE2", "Vlaams Gewest (Flemish Region)", "BE"),
    ("BE3", "Region Wallonne (Wallonia)", "BE"),
    ("BG3", "Severna i Yugoiztochna Bulgaria (North and Southeast Bulgaria)", "BG"),
    ("BG4", "Yugozapadna i Yuzhna Tsentralna Bulgaria (Southwest and South Central)", "BG"),
    ("CZ0", "Ceska republika (Czech Republic)", "CZ"),
    ("DK0", "Danmark (Denmark)", "DK"),
    ("DE1", "Baden-Wuerttemberg", "DE"),
    ("DE2", "Bayern (Bavaria)", "DE"),
    ("DE3", "Berlin", "DE"),
    ("DE4", "Brandenburg", "DE"),
    ("DE5", "Bremen", "DE"),
    ("DE6", "Hamburg", "DE"),
    ("DE7", "Hessen (Hesse)", "DE"),
    ("DE8", "Mecklenburg-Vorpommern", "DE"),
    ("DE9", "Niedersachsen (Lower Saxony)", "DE"),
    ("DEA", "Nordrhein-Westfalen (North Rhine-Westphalia)", "DE"),
    ("DEB", "Rheinland-Pfalz (Rhineland-Palatinate)", "DE"),
    ("DEC", "Saarland", "DE"),
    ("DED", "Sachsen (Saxony)", "DE"),
    ("DEE", "Sachsen-Anhalt (Saxony-Anhalt)", "DE"),
    ("DEF", "Schleswig-Holstein", "DE"),
    ("DEG", "Thueringen (Thuringia)", "DE"),
    ("EE0", "Eesti (Estonia)", "EE"),
    ("IE0", "Ireland", "IE"),
    ("EL3", "Attiki (Attica)", "GR"),
    ("EL4", "Nisia Aigaiou, Kriti (Aegean Islands, Crete)", "GR"),
    ("EL5", "Voreia Ellada (Northern Greece)", "GR"),
    ("EL6", "Kentriki Ellada (Central Greece)", "GR"),
    ("ES1", "Noroeste (Northwest Spain)", "ES"),
    ("ES2", "Noreste (Northeast Spain)", "ES"),
    ("ES3", "Comunidad de Madrid", "ES"),
    ("ES4", "Centro (Espana) (Central Spain)", "ES"),
    ("ES5", "Este (Eastern Spain)", "ES"),
    ("ES6", "Sur (Southern Spain)", "ES"),
    ("ES7", "Canarias (Canary Islands)", "ES"),
    ("FR1", "Ile-de-France", "FR"),
    ("FRB", "Centre-Val de Loire", "FR"),
    ("FRC", "Bourgogne-Franche-Comte", "FR"),
    ("FRD", "Normandie (Normandy)", "FR"),
    ("FRE", "Nord-Pas-de-Calais - Picardie (Hauts-de-France)", "FR"),
    ("FRF", "Alsace-Champagne-Ardenne-Lorraine (Grand Est)", "FR"),
    ("FRG", "Pays de la Loire", "FR"),
    ("FRH", "Bretagne (Brittany)", "FR"),
    ("FRI", "Aquitaine-Limousin-Poitou-Charentes (Nouvelle-Aquitaine)", "FR"),
    ("FRJ", "Languedoc-Roussillon-Midi-Pyrenees (Occitanie)", "FR"),
    ("FRK", "Auvergne-Rhone-Alpes", "FR"),
    ("FRL", "Provence-Alpes-Cote d\'Azur", "FR"),
    ("FRM", "Corse (Corsica)", "FR"),
    ("HR0", "Hrvatska (Croatia)", "HR"),
    ("ITC", "Nord-Ovest (Northwest Italy)", "IT"),
    ("ITH", "Nord-Est (Northeast Italy)", "IT"),
    ("ITI", "Centro (Central Italy)", "IT"),
    ("ITF", "Sud (Southern Italy)", "IT"),
    ("ITG", "Isole (Islands)", "IT"),
    ("CY0", "Kypros (Cyprus)", "CY"),
    ("LV0", "Latvija (Latvia)", "LV"),
    ("LT0", "Lietuva (Lithuania)", "LT"),
    ("LU0", "Luxembourg", "LU"),
    ("HU1", "Kozep-Magyarorszag (Central Hungary)", "HU"),
    ("HU2", "Dunantul (Transdanubia)", "HU"),
    ("HU3", "Alfold es Eszak (Northern Hungary and Great Plain)", "HU"),
    ("MT0", "Malta", "MT"),
    ("NL1", "Noord-Nederland (North Netherlands)", "NL"),
    ("NL2", "Oost-Nederland (East Netherlands)", "NL"),
    ("NL3", "West-Nederland (West Netherlands)", "NL"),
    ("NL4", "Zuid-Nederland (South Netherlands)", "NL"),
    ("AT1", "Ostosterreich (East Austria)", "AT"),
    ("AT2", "Sudosterreich (South Austria)", "AT"),
    ("AT3", "Westosterreich (West Austria)", "AT"),
    ("PL2", "Poludniowy (Southern Poland)", "PL"),
    ("PL4", "Polnocno-Zachodni (Northwest Poland)", "PL"),
    ("PL5", "Poludniowo-Zachodni (Southwest Poland)", "PL"),
    ("PL6", "Polnocny (Northern Poland)", "PL"),
    ("PL7", "Centralny (Central Poland)", "PL"),
    ("PL8", "Wschodni (Eastern Poland)", "PL"),
    ("PL9", "Mazowieckie (Masovian)", "PL"),
    ("PT1", "Continente (Continental Portugal)", "PT"),
    ("PT2", "Regiao Autonoma dos Acores (Azores)", "PT"),
    ("PT3", "Regiao Autonoma da Madeira (Madeira)", "PT"),
    ("RO1", "Macroregiunea unu (Macroregion 1)", "RO"),
    ("RO2", "Macroregiunea doi (Macroregion 2)", "RO"),
    ("RO3", "Macroregiunea trei (Macroregion 3)", "RO"),
    ("RO4", "Macroregiunea patru (Macroregion 4)", "RO"),
    ("SI0", "Slovenija (Slovenia)", "SI"),
    ("SK0", "Slovensko (Slovakia)", "SK"),
    ("FI1", "Manner-Suomi (Mainland Finland)", "FI"),
    ("FI2", "Aland", "FI"),
    ("SE1", "Ostra Sverige (Eastern Sweden)", "SE"),
    ("SE2", "Sodra Sverige (Southern Sweden)", "SE"),
    ("SE3", "Norra Sverige (Northern Sweden)", "SE"),
    ("NO0", "Norge (Norway)", "NO"),
    ("CH0", "Schweiz / Suisse / Svizzera (Switzerland)", "CH"),
]


async def ingest_eu_nuts_2021(conn) -> int:
    """Ingest EU NUTS 2021 into classification_system and classification_node.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "eu_nuts_2021",
        "EU NUTS 2021",
        "Nomenclature of Territorial Units for Statistics 2021 (Eurostat)",
        "2021",
        "European Union",
        "Eurostat",
    )

    count = 0
    for seq, (code, title) in enumerate(NUTS_COUNTRIES, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "eu_nuts_2021", code, title, 0, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(NUTS1, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "eu_nuts_2021", code, title, 1, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'eu_nuts_2021'",
        count,
    )

    # Crosswalk to ISO 3166-1 alpha-2 country codes
    for country_code, _ in NUTS_COUNTRIES:
        iso_exists = await conn.fetchval(
            "SELECT 1 FROM classification_node WHERE system_id = 'iso_3166_1' AND code = $1",
            country_code,
        )
        if iso_exists:
            await conn.execute(
                """INSERT INTO equivalence
                       (source_system, source_code, target_system, target_code, match_type, notes)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT (source_system, source_code, target_system, target_code) DO NOTHING""",
                "eu_nuts_2021", country_code, "iso_3166_1", country_code,
                "exact", "EU NUTS country code to ISO 3166-1 alpha-2",
            )

    return count
