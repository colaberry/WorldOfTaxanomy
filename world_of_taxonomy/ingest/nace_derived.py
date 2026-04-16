"""NACE-derived classification system ingesters.

These systems (WZ 2008, ONACE 2008, NOGA 2008) are national adaptations of
NACE Rev 2.  At the NACE-level granularity the codes are identical, so we
derive them by copying every node from the existing nace_rev2 data in the
database and creating exact-match equivalence edges back to nace_rev2.

National extensions (codes finer than the 4-digit NACE class level) can be
added later by parsing country-specific data files.

Must be called AFTER nace_rev2 has been ingested.
"""

from __future__ import annotations

from typing import NamedTuple


class _SystemMeta(NamedTuple):
    id: str
    name: str
    full_name: str
    region: str
    version: str
    authority: str
    tint_color: str


_WZ_2008 = _SystemMeta(
    id="wz_2008",
    name="WZ 2008",
    full_name="Klassifikation der Wirtschaftszweige 2008",
    region="Germany",
    version="2008",
    authority="Statistisches Bundesamt (Destatis)",
    tint_color="#EF4444",
)

_ONACE_2008 = _SystemMeta(
    id="onace_2008",
    name="ÖNACE 2008",
    full_name="Österreichische Systematik der Wirtschaftstätigkeiten 2008",
    region="Austria",
    version="2008",
    authority="Statistik Austria",
    tint_color="#DC2626",
)

_NOGA_2008 = _SystemMeta(
    id="noga_2008",
    name="NOGA 2008",
    full_name="Nomenclature Générale des Activités économiques 2008",
    region="Switzerland",
    version="2008",
    authority="Swiss Federal Statistical Office (BFS)",
    tint_color="#B91C1C",
)

_ATECO_2007 = _SystemMeta(
    id="ateco_2007",
    name="ATECO 2007",
    full_name="Classificazione delle Attivita Economiche ATECO 2007",
    region="Italy",
    version="2007",
    authority="Istituto Nazionale di Statistica (ISTAT)",
    tint_color="#16A34A",
)

_NAF_REV2 = _SystemMeta(
    id="naf_rev2",
    name="NAF Rev 2",
    full_name="Nomenclature des Activites Francaises Rev 2 (2008)",
    region="France",
    version="Rev 2",
    authority="Institut National de la Statistique et des Etudes Economiques (INSEE)",
    tint_color="#2563EB",
)

_PKD_2007 = _SystemMeta(
    id="pkd_2007",
    name="PKD 2007",
    full_name="Polska Klasyfikacja Dzialalnosci 2007",
    region="Poland",
    version="2007",
    authority="Glowny Urzad Statystyczny (GUS)",
    tint_color="#DC2626",
)

_SBI_2008 = _SystemMeta(
    id="sbi_2008",
    name="SBI 2008",
    full_name="Standaard Bedrijfsindeling 2008 (Dutch Standard Industrial Classification)",
    region="Netherlands",
    version="2008",
    authority="Centraal Bureau voor de Statistiek (CBS)",
    tint_color="#F97316",
)

_SNI_2007 = _SystemMeta(
    id="sni_2007",
    name="SNI 2007",
    full_name="Standard for Svensk Naringsgrensindelning 2007",
    region="Sweden",
    version="2007",
    authority="Statistics Sweden (SCB)",
    tint_color="#0EA5E9",
)

_DB07 = _SystemMeta(
    id="db07",
    name="DB07",
    full_name="Dansk Branchekode 2007 (Danish Industry Classification)",
    region="Denmark",
    version="2007",
    authority="Danmarks Statistik",
    tint_color="#8B5CF6",
)

_TOL_2008 = _SystemMeta(
    id="tol_2008",
    name="TOL 2008",
    full_name="Toimialaluokitus TOL 2008 (Finnish Standard Industrial Classification)",
    region="Finland",
    version="2008",
    authority="Statistics Finland (Tilastokeskus)",
    tint_color="#14B8A6",
)

_CAE_REV3 = _SystemMeta(
    id="cae_rev3",
    name="CAE Rev 3",
    full_name="Classificacao Portuguesa das Actividades Economicas, Revisao 3",
    region="Portugal",
    version="Rev 3",
    authority="Instituto Nacional de Estatistica (INE Portugal)",
    tint_color="#16A34A",
)

_CZ_NACE = _SystemMeta(
    id="cz_nace",
    name="CZ-NACE",
    full_name="Klasifikace ekonomickych cinnosti CZ-NACE",
    region="Czech Republic",
    version="2008",
    authority="Czech Statistical Office (CZSO)",
    tint_color="#DC2626",
)

_TEAOR_2008 = _SystemMeta(
    id="teaor_2008",
    name="TEAOR 2008",
    full_name="Tevekenysegek Egységes Agazati Osztalyozasi Rendszere 2008",
    region="Hungary",
    version="2008",
    authority="Kozponti Statisztikai Hivatal (KSH)",
    tint_color="#16A34A",
)

_CAEN_REV2 = _SystemMeta(
    id="caen_rev2",
    name="CAEN Rev 2",
    full_name="Clasificarea Activitatilor din Economia Nationala, Revizia 2",
    region="Romania",
    version="Rev 2",
    authority="Institutul National de Statistica (INS Romania)",
    tint_color="#FBBF24",
)

_NKD_2007 = _SystemMeta(
    id="nkd_2007",
    name="NKD 2007",
    full_name="Nacionalna Klasifikacija Djelatnosti 2007 (Croatian NACE)",
    region="Croatia",
    version="2007",
    authority="Drzavni zavod za statistiku (DZS)",
    tint_color="#2563EB",
)

_SK_NACE = _SystemMeta(
    id="sk_nace",
    name="SK NACE",
    full_name="Statisticka klasifikacia ekonomickych cinnosti SK NACE Rev. 2",
    region="Slovakia",
    version="Rev 2",
    authority="Statistical Office of the Slovak Republic (SO SR)",
    tint_color="#7C3AED",
)

_NKID = _SystemMeta(
    id="nkid",
    name="NKID",
    full_name="Natsionalna Klasifikatsiya na Ikonomicheskite Deynosti (Bulgarian NACE)",
    region="Bulgaria",
    version="2008",
    authority="National Statistical Institute of Bulgaria (NSI)",
    tint_color="#0891B2",
)

_EMTAK = _SystemMeta(
    id="emtak",
    name="EMTAK",
    full_name="Eesti Majanduse Tegevusalade Klassifikaator (Estonian NACE)",
    region="Estonia",
    version="2008",
    authority="Statistics Estonia (Statistikaamet)",
    tint_color="#059669",
)

_NACE_LT = _SystemMeta(
    id="nace_lt",
    name="NACE-LT",
    full_name="Ekonomines Veiklos Rusiu Klasifikatorius (Lithuanian NACE)",
    region="Lithuania",
    version="2008",
    authority="Statistics Lithuania (Statistikos departamentas)",
    tint_color="#F59E0B",
)

_NK_LV = _SystemMeta(
    id="nk_lv",
    name="NK",
    full_name="Saimniecisko darbibu statistiska klasifikacija (Latvian NACE)",
    region="Latvia",
    version="2008",
    authority="Central Statistical Bureau of Latvia (CSB)",
    tint_color="#DB2777",
)

_NACE_TR = _SystemMeta(
    id="nace_tr",
    name="NACE Rev 2 (Turkey)",
    full_name="Istatistiki Birim Siniflamasi - NACE Rev 2 (Turkish Adaptation)",
    region="Turkey",
    version="Rev 2",
    authority="Turkish Statistical Institute (TUIK)",
    tint_color="#EA580C",
)

# ── Remaining EU / EEA systems ────────────────────────────────

_CNAE_2009 = _SystemMeta(
    id="cnae_2009",
    name="CNAE 2009",
    full_name="Clasificacion Nacional de Actividades Economicas 2009",
    region="Spain",
    version="2009",
    authority="Instituto Nacional de Estadistica (INE Spain)",
    tint_color="#E11D48",
)

_NACE_BEL = _SystemMeta(
    id="nace_bel",
    name="NACE-BEL 2008",
    full_name="Nomenclature statistique des Activites economiques - Belgique 2008",
    region="Belgium",
    version="2008",
    authority="Statbel (Belgian Statistical Office)",
    tint_color="#F59E0B",
)

_NACE_LU = _SystemMeta(
    id="nace_lu",
    name="NACE-LU 2008",
    full_name="Nomenclature statistique des Activites economiques - Luxembourg 2008",
    region="Luxembourg",
    version="2008",
    authority="STATEC (Institut national de la statistique et des etudes economiques)",
    tint_color="#0284C7",
)

_NACE_IE = _SystemMeta(
    id="nace_ie",
    name="NACE Rev 2 (Ireland)",
    full_name="Statistical Classification of Economic Activities - Ireland (NACE Rev 2)",
    region="Ireland",
    version="Rev 2",
    authority="Central Statistics Office (CSO Ireland)",
    tint_color="#16A34A",
)

_STAKOD_08 = _SystemMeta(
    id="stakod_08",
    name="STAKOD 08",
    full_name="Statistiki Taxinomisi ton Oikonomikon Drastiriotiton - STAKOD 2008",
    region="Greece",
    version="2008",
    authority="Hellenic Statistical Authority (ELSTAT)",
    tint_color="#2563EB",
)

_NACE_CY = _SystemMeta(
    id="nace_cy",
    name="NACE Rev 2 (Cyprus)",
    full_name="Statistical Classification of Economic Activities - Cyprus (NACE Rev 2)",
    region="Cyprus",
    version="Rev 2",
    authority="Statistical Service of Cyprus (CYSTAT)",
    tint_color="#D97706",
)

_NACE_MT = _SystemMeta(
    id="nace_mt",
    name="NACE Rev 2 (Malta)",
    full_name="Statistical Classification of Economic Activities - Malta (NACE Rev 2)",
    region="Malta",
    version="Rev 2",
    authority="National Statistics Office Malta (NSO)",
    tint_color="#9333EA",
)

_SKD_2008 = _SystemMeta(
    id="skd_2008",
    name="SKD 2008",
    full_name="Standardna Klasifikacija Dejavnosti 2008 (Slovenian NACE)",
    region="Slovenia",
    version="2008",
    authority="Statistical Office of the Republic of Slovenia (SURS)",
    tint_color="#0891B2",
)

_SN_2007 = _SystemMeta(
    id="sn_2007",
    name="SN 2007",
    full_name="Standard for Norsk Naeringsgruppering 2007 (Norwegian NACE)",
    region="Norway",
    version="2007",
    authority="Statistics Norway (SSB)",
    tint_color="#DC2626",
)

_ISAT_2008 = _SystemMeta(
    id="isat_2008",
    name="ISAT 2008",
    full_name="Islensk Atvinnugreinaflokkun 2008 (Icelandic NACE)",
    region="Iceland",
    version="2008",
    authority="Statistics Iceland (Hagstofa Islands)",
    tint_color="#4F46E5",
)

# ── Balkans / Eastern Europe systems ──────────────────────────

_KD_RS = _SystemMeta(
    id="kd_rs",
    name="KD 2010 (Serbia)",
    full_name="Klasifikacija Delatnosti 2010 (Serbian NACE Rev 2)",
    region="Serbia",
    version="2010",
    authority="Statistical Office of the Republic of Serbia (RZS)",
    tint_color="#DC2626",
)

_NKD_MK = _SystemMeta(
    id="nkd_mk",
    name="NKD Rev 2 (North Macedonia)",
    full_name="Nacionalna Klasifikacija na Dejnosti Rev 2 (North Macedonia NACE)",
    region="North Macedonia",
    version="Rev 2",
    authority="State Statistical Office of North Macedonia (SSO)",
    tint_color="#16A34A",
)

_KD_BA = _SystemMeta(
    id="kd_ba",
    name="KD BiH 2010",
    full_name="Klasifikacija Djelatnosti Bosne i Hercegovine 2010 (BiH NACE)",
    region="Bosnia and Herzegovina",
    version="2010",
    authority="Agency for Statistics of Bosnia and Herzegovina (BHAS)",
    tint_color="#2563EB",
)

_KD_ME = _SystemMeta(
    id="kd_me",
    name="KD 2010 (Montenegro)",
    full_name="Klasifikacija Djelatnosti 2010 (Montenegrin NACE Rev 2)",
    region="Montenegro",
    version="2010",
    authority="Statistical Office of Montenegro (MONSTAT)",
    tint_color="#7C3AED",
)

_NVE_AL = _SystemMeta(
    id="nve_al",
    name="NVE Rev 2 (Albania)",
    full_name="Nomenklatura e Veprimtarive Ekonomike Rev 2 (Albanian NACE)",
    region="Albania",
    version="Rev 2",
    authority="Institute of Statistics Albania (INSTAT)",
    tint_color="#D97706",
)

_KD_XK = _SystemMeta(
    id="kd_xk",
    name="KD 2010 (Kosovo)",
    full_name="Klasifikimi i Aktiviteteve Ekonomike 2010 (Kosovo NACE Rev 2)",
    region="Kosovo",
    version="2010",
    authority="Kosovo Agency of Statistics (KAS)",
    tint_color="#0891B2",
)

_CAEM_MD = _SystemMeta(
    id="caem_md",
    name="CAEM Rev 2 (Moldova)",
    full_name="Clasificatorul Activitatilor din Economia Moldovei Rev 2",
    region="Moldova",
    version="Rev 2",
    authority="National Bureau of Statistics of Moldova (NBS)",
    tint_color="#059669",
)

_KVED_UA = _SystemMeta(
    id="kved_ua",
    name="KVED 2010 (Ukraine)",
    full_name="Klasyfikatsiya Vydiv Ekonomichnoyi Diyalnosti 2010 (Ukrainian NACE)",
    region="Ukraine",
    version="2010",
    authority="State Statistics Service of Ukraine (Ukrstat)",
    tint_color="#F59E0B",
)

_NACE_GE = _SystemMeta(
    id="nace_ge",
    name="NACE Rev 2 (Georgia)",
    full_name="Georgian Classification of Economic Activities (NACE Rev 2 based)",
    region="Georgia",
    version="Rev 2",
    authority="National Statistics Office of Georgia (Geostat)",
    tint_color="#E11D48",
)

_NACE_AM = _SystemMeta(
    id="nace_am",
    name="NACE Rev 2 (Armenia)",
    full_name="Armenian Classification of Economic Activities (NACE Rev 2 based)",
    region="Armenia",
    version="Rev 2",
    authority="Statistical Committee of the Republic of Armenia (Armstat)",
    tint_color="#9333EA",
)


# ── Core derivation logic ───────────────────────────────────────


async def _ingest_derived_from_nace(conn, meta: _SystemMeta) -> int:
    """Generic ingester that copies nace_rev2 nodes into a derived system.

    Steps:
      1. Register the classification_system.
      2. Copy every classification_node from nace_rev2 with the new system_id.
      3. Create bidirectional exact-match equivalence edges.
      4. Update node_count on the new system.

    Args:
        conn: asyncpg connection (search_path already set).
        meta: metadata for the derived system.

    Returns:
        Number of nodes ingested.
    """
    # 1. Register classification system
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        meta.id,
        meta.name,
        meta.full_name,
        meta.region,
        meta.version,
        meta.authority,
        meta.tint_color,
    )

    # 2. Copy nodes from nace_rev2
    nace_rows = await conn.fetch(
        """
        SELECT code, title, description, level, parent_code,
               sector_code, is_leaf, seq_order
        FROM classification_node
        WHERE system_id = 'nace_rev2'
        ORDER BY seq_order
        """
    )

    if not nace_rows:
        print(f"  WARNING: No nace_rev2 nodes found - {meta.id} will be empty")
        return 0

    count = 0
    for row in nace_rows:
        await conn.execute(
            """
            INSERT INTO classification_node
                (system_id, code, title, description, level,
                 parent_code, sector_code, is_leaf, seq_order)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (system_id, code) DO NOTHING
            """,
            meta.id,
            row["code"],
            row["title"],
            row["description"],
            row["level"],
            row["parent_code"],
            row["sector_code"],
            row["is_leaf"],
            row["seq_order"],
        )
        count += 1

    # 3. Create bidirectional exact-match equivalence edges
    for row in nace_rows:
        code = row["code"]
        # Forward: derived -> nace_rev2
        await conn.execute(
            """
            INSERT INTO equivalence
                (source_system, source_code, target_system, target_code, match_type)
            VALUES ($1, $2, 'nace_rev2', $3, 'exact')
            ON CONFLICT (source_system, source_code, target_system, target_code)
            DO NOTHING
            """,
            meta.id,
            code,
            code,
        )
        # Reverse: nace_rev2 -> derived
        await conn.execute(
            """
            INSERT INTO equivalence
                (source_system, source_code, target_system, target_code, match_type)
            VALUES ('nace_rev2', $1, $2, $3, 'exact')
            ON CONFLICT (source_system, source_code, target_system, target_code)
            DO NOTHING
            """,
            code,
            meta.id,
            code,
        )

    # 4. Update node_count
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count,
        meta.id,
    )

    print(f"  Ingested {count} {meta.name} codes (derived from NACE Rev 2)")
    return count


# ── Public API ──────────────────────────────────────────────────


async def ingest_wz_2008(conn) -> int:
    """Ingest German WZ 2008 (Klassifikation der Wirtschaftszweige).

    Derives all codes from NACE Rev 2 already present in the database.
    National 5-digit extensions can be added later from Destatis data.
    """
    return await _ingest_derived_from_nace(conn, _WZ_2008)


async def ingest_onace_2008(conn) -> int:
    """Ingest Austrian ONACE 2008.

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from Statistik Austria data.
    """
    return await _ingest_derived_from_nace(conn, _ONACE_2008)


async def ingest_noga_2008(conn) -> int:
    """Ingest Swiss NOGA 2008.

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from BFS data.
    """
    return await _ingest_derived_from_nace(conn, _NOGA_2008)


async def ingest_ateco_2007(conn) -> int:
    """Ingest Italian ATECO 2007 (Classificazione delle Attivita Economiche).

    Derives all codes from NACE Rev 2 already present in the database.
    National 6-digit extensions can be added later from ISTAT data files.
    """
    return await _ingest_derived_from_nace(conn, _ATECO_2007)


async def ingest_naf_rev2(conn) -> int:
    """Ingest French NAF Rev 2 (Nomenclature des Activites Francaises).

    Derives all codes from NACE Rev 2 already present in the database.
    National 6-character sub-class extensions can be added later from INSEE data.
    """
    return await _ingest_derived_from_nace(conn, _NAF_REV2)


async def ingest_pkd_2007(conn) -> int:
    """Ingest Polish PKD 2007 (Polska Klasyfikacja Dzialalnosci).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from GUS data files.
    """
    return await _ingest_derived_from_nace(conn, _PKD_2007)


async def ingest_sbi_2008(conn) -> int:
    """Ingest Dutch SBI 2008 (Standaard Bedrijfsindeling).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from CBS data files.
    """
    return await _ingest_derived_from_nace(conn, _SBI_2008)


async def ingest_sni_2007(conn) -> int:
    """Ingest Swedish SNI 2007 (Standard for Svensk Naringsgrensindelning).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from SCB data files.
    """
    return await _ingest_derived_from_nace(conn, _SNI_2007)


async def ingest_db07(conn) -> int:
    """Ingest Danish DB07 (Dansk Branchekode 2007).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from Danmarks Statistik data files.
    """
    return await _ingest_derived_from_nace(conn, _DB07)


async def ingest_tol_2008(conn) -> int:
    """Ingest Finnish TOL 2008 (Toimialaluokitus).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from Statistics Finland data files.
    """
    return await _ingest_derived_from_nace(conn, _TOL_2008)


async def ingest_cae_rev3(conn) -> int:
    """Ingest Portuguese CAE Rev 3 (Classificacao Portuguesa das Actividades Economicas).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from INE Portugal data files.
    """
    return await _ingest_derived_from_nace(conn, _CAE_REV3)


async def ingest_cz_nace(conn) -> int:
    """Ingest Czech CZ-NACE (Klasifikace ekonomickych cinnosti).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from CZSO data files.
    """
    return await _ingest_derived_from_nace(conn, _CZ_NACE)


async def ingest_teaor_2008(conn) -> int:
    """Ingest Hungarian TEAOR 2008 (Tevekenysegek Egységes Agazati Osztalyozasi Rendszere).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from KSH data files.
    """
    return await _ingest_derived_from_nace(conn, _TEAOR_2008)


async def ingest_caen_rev2(conn) -> int:
    """Ingest Romanian CAEN Rev 2 (Clasificarea Activitatilor din Economia Nationala).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from INS Romania data files.
    """
    return await _ingest_derived_from_nace(conn, _CAEN_REV2)


async def ingest_nkd_2007(conn) -> int:
    """Ingest Croatian NKD 2007 (Nacionalna Klasifikacija Djelatnosti).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from DZS data files.
    """
    return await _ingest_derived_from_nace(conn, _NKD_2007)


async def ingest_sk_nace(conn) -> int:
    """Ingest Slovak SK NACE Rev 2 (Statisticka klasifikacia ekonomickych cinnosti).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from SO SR data files.
    """
    return await _ingest_derived_from_nace(conn, _SK_NACE)


async def ingest_nkid(conn) -> int:
    """Ingest Bulgarian NKID (Natsionalna Klasifikatsiya na Ikonomicheskite Deynosti).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from NSI Bulgaria data files.
    """
    return await _ingest_derived_from_nace(conn, _NKID)


async def ingest_emtak(conn) -> int:
    """Ingest Estonian EMTAK (Eesti Majanduse Tegevusalade Klassifikaator).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from Statistics Estonia data files.
    """
    return await _ingest_derived_from_nace(conn, _EMTAK)


async def ingest_nace_lt(conn) -> int:
    """Ingest Lithuanian NACE-LT (Ekonomines Veiklos Rusiu Klasifikatorius).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from Statistics Lithuania data files.
    """
    return await _ingest_derived_from_nace(conn, _NACE_LT)


async def ingest_nk_lv(conn) -> int:
    """Ingest Latvian NK (Saimniecisko darbibu statistiska klasifikacija).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from CSB Latvia data files.
    """
    return await _ingest_derived_from_nace(conn, _NK_LV)


async def ingest_nace_tr(conn) -> int:
    """Ingest Turkish NACE Rev 2 adaptation (Istatistiki Birim Siniflamasi).

    Derives all codes from NACE Rev 2 already present in the database.
    Turkey officially adopted NACE Rev 2 with minor national annotations.
    """
    return await _ingest_derived_from_nace(conn, _NACE_TR)


async def ingest_cnae_2009(conn) -> int:
    """Ingest Spanish CNAE 2009 (Clasificacion Nacional de Actividades Economicas).

    Derives all codes from NACE Rev 2 already present in the database.
    National 5-digit extensions can be added later from INE Spain data.
    """
    return await _ingest_derived_from_nace(conn, _CNAE_2009)


async def ingest_nace_bel(conn) -> int:
    """Ingest Belgian NACE-BEL 2008 (Nomenclature statistique des Activites economiques).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from Statbel data files.
    """
    return await _ingest_derived_from_nace(conn, _NACE_BEL)


async def ingest_nace_lu(conn) -> int:
    """Ingest Luxembourg NACE-LU 2008.

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from STATEC data files.
    """
    return await _ingest_derived_from_nace(conn, _NACE_LU)


async def ingest_nace_ie(conn) -> int:
    """Ingest Irish NACE Rev 2 adaptation.

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from CSO Ireland data files.
    """
    return await _ingest_derived_from_nace(conn, _NACE_IE)


async def ingest_stakod_08(conn) -> int:
    """Ingest Greek STAKOD 08 (Statistiki Taxinomisi ton Oikonomikon Drastiriotiton).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from ELSTAT data files.
    """
    return await _ingest_derived_from_nace(conn, _STAKOD_08)


async def ingest_nace_cy(conn) -> int:
    """Ingest Cyprus NACE Rev 2 adaptation.

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from CYSTAT data files.
    """
    return await _ingest_derived_from_nace(conn, _NACE_CY)


async def ingest_nace_mt(conn) -> int:
    """Ingest Malta NACE Rev 2 adaptation.

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from NSO Malta data files.
    """
    return await _ingest_derived_from_nace(conn, _NACE_MT)


async def ingest_skd_2008(conn) -> int:
    """Ingest Slovenian SKD 2008 (Standardna Klasifikacija Dejavnosti).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from SURS data files.
    """
    return await _ingest_derived_from_nace(conn, _SKD_2008)


async def ingest_sn_2007(conn) -> int:
    """Ingest Norwegian SN 2007 (Standard for Norsk Naeringsgruppering).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from SSB data files.
    """
    return await _ingest_derived_from_nace(conn, _SN_2007)


async def ingest_isat_2008(conn) -> int:
    """Ingest Icelandic ISAT 2008 (Islensk Atvinnugreinaflokkun).

    Derives all codes from NACE Rev 2 already present in the database.
    National extensions can be added later from Statistics Iceland data files.
    """
    return await _ingest_derived_from_nace(conn, _ISAT_2008)


async def ingest_kd_rs(conn) -> int:
    """Ingest Serbian KD 2010 (Klasifikacija Delatnosti)."""
    return await _ingest_derived_from_nace(conn, _KD_RS)


async def ingest_nkd_mk(conn) -> int:
    """Ingest North Macedonia NKD Rev 2 (Nacionalna Klasifikacija na Dejnosti)."""
    return await _ingest_derived_from_nace(conn, _NKD_MK)


async def ingest_kd_ba(conn) -> int:
    """Ingest Bosnia and Herzegovina KD BiH 2010."""
    return await _ingest_derived_from_nace(conn, _KD_BA)


async def ingest_kd_me(conn) -> int:
    """Ingest Montenegrin KD 2010 (Klasifikacija Djelatnosti)."""
    return await _ingest_derived_from_nace(conn, _KD_ME)


async def ingest_nve_al(conn) -> int:
    """Ingest Albanian NVE Rev 2 (Nomenklatura e Veprimtarive Ekonomike)."""
    return await _ingest_derived_from_nace(conn, _NVE_AL)


async def ingest_kd_xk(conn) -> int:
    """Ingest Kosovo KD 2010 (Klasifikimi i Aktiviteteve Ekonomike)."""
    return await _ingest_derived_from_nace(conn, _KD_XK)


async def ingest_caem_md(conn) -> int:
    """Ingest Moldova CAEM Rev 2 (Clasificatorul Activitatilor din Economia Moldovei)."""
    return await _ingest_derived_from_nace(conn, _CAEM_MD)


async def ingest_kved_ua(conn) -> int:
    """Ingest Ukrainian KVED 2010 (Klasyfikatsiya Vydiv Ekonomichnoyi Diyalnosti)."""
    return await _ingest_derived_from_nace(conn, _KVED_UA)


async def ingest_nace_ge(conn) -> int:
    """Ingest Georgian NACE Rev 2 classification."""
    return await _ingest_derived_from_nace(conn, _NACE_GE)


async def ingest_nace_am(conn) -> int:
    """Ingest Armenian NACE Rev 2 classification."""
    return await _ingest_derived_from_nace(conn, _NACE_AM)
