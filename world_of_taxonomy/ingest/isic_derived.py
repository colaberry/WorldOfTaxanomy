"""ISIC-derived classification system ingesters.

These systems (CIIU Rev 4 for Colombia, Argentina, and Chile) are national
adaptations of ISIC Rev 4. At the ISIC-level granularity the codes are
identical, so we derive them by copying every node from the existing
isic_rev4 data in the database and creating exact-match equivalence edges
back to isic_rev4.

National extensions can be added later by parsing country-specific data files.

Must be called AFTER isic_rev4 has been ingested.
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


_CIIU_CO = _SystemMeta(
    id="ciiu_co",
    name="CIIU Rev 4 AC (Colombia)",
    full_name="Clasificacion Industrial Internacional Uniforme de todas las Actividades Economicas, Revision 4 adaptada para Colombia",
    region="Colombia",
    version="Rev 4 AC",
    authority="Departamento Administrativo Nacional de Estadistica (DANE)",
    tint_color="#FBBF24",
)

_CIIU_AR = _SystemMeta(
    id="ciiu_ar",
    name="CLANAE Rev 4 (Argentina)",
    full_name="Clasificador Nacional de Actividades Economicas Revision 4 - Argentina",
    region="Argentina",
    version="Rev 4",
    authority="Instituto Nacional de Estadistica y Censos (INDEC)",
    tint_color="#60A5FA",
)

_CIIU_CL = _SystemMeta(
    id="ciiu_cl",
    name="CIIU Rev 4 (Chile)",
    full_name="Clasificacion Industrial Internacional Uniforme de todas las Actividades Economicas, Revision 4 - Chile",
    region="Chile",
    version="Rev 4",
    authority="Instituto Nacional de Estadisticas (INE Chile)",
    tint_color="#F472B6",
)

# ── LATAM additional systems ────────────────────────────────────

_CIIU_PE = _SystemMeta(
    id="ciiu_pe",
    name="CIIU Rev 4 (Peru)",
    full_name="Clasificacion Industrial Internacional Uniforme, Revision 4 - Peru",
    region="Peru",
    version="Rev 4",
    authority="Instituto Nacional de Estadistica e Informatica (INEI)",
    tint_color="#A16207",
)

_CIIU_EC = _SystemMeta(
    id="ciiu_ec",
    name="CIIU Rev 4 (Ecuador)",
    full_name="Clasificacion Industrial Internacional Uniforme, Revision 4 - Ecuador",
    region="Ecuador",
    version="Rev 4",
    authority="Instituto Nacional de Estadistica y Censos (INEC Ecuador)",
    tint_color="#0369A1",
)

_CAEB = _SystemMeta(
    id="caeb",
    name="CAEB",
    full_name="Clasificacion de Actividades Economicas de Bolivia",
    region="Bolivia",
    version="Rev 4",
    authority="Instituto Nacional de Estadistica (INE Bolivia)",
    tint_color="#15803D",
)

_CIIU_VE = _SystemMeta(
    id="ciiu_ve",
    name="CIIU Rev 4 (Venezuela)",
    full_name="Clasificacion Industrial Internacional Uniforme, Revision 4 - Venezuela",
    region="Venezuela",
    version="Rev 4",
    authority="Instituto Nacional de Estadistica (INE Venezuela)",
    tint_color="#B91C1C",
)

_CIIU_CR = _SystemMeta(
    id="ciiu_cr",
    name="CIIU Rev 4 (Costa Rica)",
    full_name="Clasificacion Industrial Internacional Uniforme, Revision 4 - Costa Rica",
    region="Costa Rica",
    version="Rev 4",
    authority="Instituto Nacional de Estadistica y Censos (INEC Costa Rica)",
    tint_color="#0891B2",
)

_CIIU_GT = _SystemMeta(
    id="ciiu_gt",
    name="CIIU Rev 4 (Guatemala)",
    full_name="Clasificacion Industrial Internacional Uniforme, Revision 4 - Guatemala",
    region="Guatemala",
    version="Rev 4",
    authority="Instituto Nacional de Estadistica (INE Guatemala)",
    tint_color="#7C3AED",
)

_CIIU_PA = _SystemMeta(
    id="ciiu_pa",
    name="CIIU Rev 4 (Panama)",
    full_name="Clasificacion Industrial Internacional Uniforme, Revision 4 - Panama",
    region="Panama",
    version="Rev 4",
    authority="Instituto Nacional de Estadistica y Censo (INEC Panama)",
    tint_color="#C2410C",
)

# ── Asia systems ────────────────────────────────────────────────

_VSIC_2018 = _SystemMeta(
    id="vsic_2018",
    name="VSIC 2018",
    full_name="Vietnam Standard Industrial Classification 2018",
    region="Vietnam",
    version="2018",
    authority="General Statistics Office of Vietnam (GSO)",
    tint_color="#DC2626",
)

_BSIC = _SystemMeta(
    id="bsic",
    name="BSIC",
    full_name="Bangladesh Standard Industrial Classification (ISIC Rev 4 adaptation)",
    region="Bangladesh",
    version="2010",
    authority="Bangladesh Bureau of Statistics (BBS)",
    tint_color="#16A34A",
)

_PSIC_PK = _SystemMeta(
    id="psic_pk",
    name="PSIC (Pakistan)",
    full_name="Pakistan Standard Industrial Classification (ISIC Rev 4 based)",
    region="Pakistan",
    version="2010",
    authority="Pakistan Bureau of Statistics (PBS)",
    tint_color="#0EA5E9",
)

# ── Africa/Middle East systems ──────────────────────────────────

_ISIC_NG = _SystemMeta(
    id="isic_ng",
    name="ISIC Rev 4 (Nigeria)",
    full_name="Nigeria Standard Industrial Classification (ISIC Rev 4 based)",
    region="Nigeria",
    version="Rev 4",
    authority="National Bureau of Statistics Nigeria (NBS)",
    tint_color="#16A34A",
)

_ISIC_KE = _SystemMeta(
    id="isic_ke",
    name="ISIC Rev 4 (Kenya)",
    full_name="Kenya Standard Industrial Classification (ISIC Rev 4 based)",
    region="Kenya",
    version="Rev 4",
    authority="Kenya National Bureau of Statistics (KNBS)",
    tint_color="#B45309",
)

_ISIC_EG = _SystemMeta(
    id="isic_eg",
    name="ISIC Rev 4 (Egypt)",
    full_name="Egypt Standard Industrial Classification (ISIC Rev 4 based)",
    region="Egypt",
    version="Rev 4",
    authority="Central Agency for Public Mobilization and Statistics (CAPMAS)",
    tint_color="#D97706",
)

_ISIC_SA = _SystemMeta(
    id="isic_sa",
    name="ISIC Rev 4 (Saudi Arabia)",
    full_name="Saudi Arabia Standard Industrial Classification (ISIC Rev 4 based)",
    region="Saudi Arabia",
    version="Rev 4",
    authority="General Authority for Statistics (GASTAT)",
    tint_color="#15803D",
)

_ISIC_AE = _SystemMeta(
    id="isic_ae",
    name="ISIC Rev 4 (UAE)",
    full_name="UAE Standard Industrial Classification (ISIC Rev 4 based)",
    region="United Arab Emirates",
    version="Rev 4",
    authority="Federal Competitiveness and Statistics Centre (FCSC)",
    tint_color="#7C3AED",
)

# ── Additional Asia systems ───────────────────────────────────

_KBLI_ID = _SystemMeta(
    id="kbli_id",
    name="KBLI 2020 (Indonesia)",
    full_name="Klasifikasi Baku Lapangan Usaha Indonesia 2020 (ISIC Rev 4 based)",
    region="Indonesia",
    version="2020",
    authority="Badan Pusat Statistik (BPS Indonesia)",
    tint_color="#DC2626",
)

_SLSIC = _SystemMeta(
    id="slsic",
    name="SLSIC (Sri Lanka)",
    full_name="Sri Lanka Standard Industrial Classification (ISIC Rev 4 based)",
    region="Sri Lanka",
    version="Rev 4",
    authority="Department of Census and Statistics Sri Lanka",
    tint_color="#059669",
)

_ISIC_MM = _SystemMeta(
    id="isic_mm",
    name="ISIC Rev 4 (Myanmar)",
    full_name="Myanmar Standard Industrial Classification (ISIC Rev 4 based)",
    region="Myanmar",
    version="Rev 4",
    authority="Central Statistical Organization Myanmar (CSO)",
    tint_color="#D97706",
)

_ISIC_KH = _SystemMeta(
    id="isic_kh",
    name="ISIC Rev 4 (Cambodia)",
    full_name="Cambodia Standard Industrial Classification (ISIC Rev 4 based)",
    region="Cambodia",
    version="Rev 4",
    authority="National Institute of Statistics Cambodia (NIS)",
    tint_color="#7C3AED",
)

_ISIC_LA = _SystemMeta(
    id="isic_la",
    name="ISIC Rev 4 (Laos)",
    full_name="Lao Standard Industrial Classification (ISIC Rev 4 based)",
    region="Laos",
    version="Rev 4",
    authority="Lao Statistics Bureau (LSB)",
    tint_color="#0891B2",
)

_ISIC_NP = _SystemMeta(
    id="isic_np",
    name="ISIC Rev 4 (Nepal)",
    full_name="Nepal Standard Industrial Classification (ISIC Rev 4 based)",
    region="Nepal",
    version="Rev 4",
    authority="Central Bureau of Statistics Nepal (CBS)",
    tint_color="#B45309",
)

# ── Additional Africa systems ─────────────────────────────────

_ISIC_ET = _SystemMeta(
    id="isic_et",
    name="ISIC Rev 4 (Ethiopia)",
    full_name="Ethiopia Standard Industrial Classification (ISIC Rev 4 based)",
    region="Ethiopia",
    version="Rev 4",
    authority="Central Statistical Agency Ethiopia (CSA)",
    tint_color="#16A34A",
)

_ISIC_TZ = _SystemMeta(
    id="isic_tz",
    name="ISIC Rev 4 (Tanzania)",
    full_name="Tanzania Standard Industrial Classification (ISIC Rev 4 based)",
    region="Tanzania",
    version="Rev 4",
    authority="National Bureau of Statistics Tanzania (NBS)",
    tint_color="#0369A1",
)

_ISIC_GH = _SystemMeta(
    id="isic_gh",
    name="ISIC Rev 4 (Ghana)",
    full_name="Ghana Standard Industrial Classification (ISIC Rev 4 based)",
    region="Ghana",
    version="Rev 4",
    authority="Ghana Statistical Service (GSS)",
    tint_color="#CA8A04",
)

_ISIC_MA = _SystemMeta(
    id="isic_ma",
    name="ISIC Rev 4 (Morocco)",
    full_name="Nomenclature Marocaine des Activites Economiques (ISIC Rev 4 based)",
    region="Morocco",
    version="Rev 4",
    authority="Haut-Commissariat au Plan (HCP Morocco)",
    tint_color="#DC2626",
)

_ISIC_TN = _SystemMeta(
    id="isic_tn",
    name="ISIC Rev 4 (Tunisia)",
    full_name="Tunisia Standard Industrial Classification (ISIC Rev 4 based)",
    region="Tunisia",
    version="Rev 4",
    authority="Institut National de la Statistique Tunisie (INS)",
    tint_color="#EA580C",
)

_ISIC_DZ = _SystemMeta(
    id="isic_dz",
    name="ISIC Rev 4 (Algeria)",
    full_name="Nomenclature Algerienne des Activites (ISIC Rev 4 based)",
    region="Algeria",
    version="Rev 4",
    authority="Office National des Statistiques (ONS Algeria)",
    tint_color="#15803D",
)

_ISIC_SN = _SystemMeta(
    id="isic_sn",
    name="ISIC Rev 4 (Senegal)",
    full_name="Senegal Standard Industrial Classification (ISIC Rev 4 based)",
    region="Senegal",
    version="Rev 4",
    authority="Agence Nationale de la Statistique et de la Demographie (ANSD)",
    tint_color="#0284C7",
)

_ISIC_CM = _SystemMeta(
    id="isic_cm",
    name="ISIC Rev 4 (Cameroon)",
    full_name="Cameroon Standard Industrial Classification (ISIC Rev 4 based)",
    region="Cameroon",
    version="Rev 4",
    authority="Institut National de la Statistique Cameroun (INS)",
    tint_color="#9333EA",
)

_ISIC_UG = _SystemMeta(
    id="isic_ug",
    name="ISIC Rev 4 (Uganda)",
    full_name="Uganda Standard Industrial Classification (ISIC Rev 4 based)",
    region="Uganda",
    version="Rev 4",
    authority="Uganda Bureau of Statistics (UBOS)",
    tint_color="#E11D48",
)

_ISIC_MZ = _SystemMeta(
    id="isic_mz",
    name="ISIC Rev 4 (Mozambique)",
    full_name="Mozambique Standard Industrial Classification (ISIC Rev 4 based)",
    region="Mozambique",
    version="Rev 4",
    authority="Instituto Nacional de Estatistica Mozambique (INE)",
    tint_color="#4F46E5",
)

# ── Additional Middle East systems ─────────────────────────────

_ISIC_IQ = _SystemMeta(
    id="isic_iq",
    name="ISIC Rev 4 (Iraq)",
    full_name="Iraq Standard Industrial Classification (ISIC Rev 4 based)",
    region="Iraq",
    version="Rev 4",
    authority="Central Statistical Organization Iraq (CSO)",
    tint_color="#B91C1C",
)

_ISIC_JO = _SystemMeta(
    id="isic_jo",
    name="ISIC Rev 4 (Jordan)",
    full_name="Jordan Standard Industrial Classification (ISIC Rev 4 based)",
    region="Jordan",
    version="Rev 4",
    authority="Department of Statistics Jordan (DOS)",
    tint_color="#059669",
)

# ── Additional LATAM systems ──────────────────────────────────

_CIIU_PY = _SystemMeta(
    id="ciiu_py",
    name="CIIU Rev 4 (Paraguay)",
    full_name="Clasificacion Industrial Internacional Uniforme, Revision 4 - Paraguay",
    region="Paraguay",
    version="Rev 4",
    authority="Direccion General de Estadistica, Encuestas y Censos (DGEEC Paraguay)",
    tint_color="#DC2626",
)

_CIIU_UY = _SystemMeta(
    id="ciiu_uy",
    name="CIIU Rev 4 (Uruguay)",
    full_name="Clasificacion Industrial Internacional Uniforme, Revision 4 - Uruguay",
    region="Uruguay",
    version="Rev 4",
    authority="Instituto Nacional de Estadistica (INE Uruguay)",
    tint_color="#2563EB",
)

_CIIU_DO = _SystemMeta(
    id="ciiu_do",
    name="CIIU Rev 4 (Dominican Republic)",
    full_name="Clasificacion Industrial Internacional Uniforme, Revision 4 - Dominican Republic",
    region="Dominican Republic",
    version="Rev 4",
    authority="Oficina Nacional de Estadistica (ONE Dominican Republic)",
    tint_color="#16A34A",
)

_ISIC_HN = _SystemMeta(
    id="isic_hn",
    name="ISIC Rev 4 (Honduras)",
    full_name="Honduras Standard Industrial Classification (ISIC Rev 4 based)",
    region="Honduras",
    version="Rev 4",
    authority="Instituto Nacional de Estadistica Honduras (INE)",
    tint_color="#CA8A04",
)

_ISIC_SV = _SystemMeta(
    id="isic_sv",
    name="ISIC Rev 4 (El Salvador)",
    full_name="El Salvador Standard Industrial Classification (ISIC Rev 4 based)",
    region="El Salvador",
    version="Rev 4",
    authority="Direccion General de Estadistica y Censos (DIGESTYC)",
    tint_color="#7C3AED",
)

_ISIC_NI = _SystemMeta(
    id="isic_ni",
    name="ISIC Rev 4 (Nicaragua)",
    full_name="Nicaragua Standard Industrial Classification (ISIC Rev 4 based)",
    region="Nicaragua",
    version="Rev 4",
    authority="Instituto Nacional de Informacion de Desarrollo (INIDE)",
    tint_color="#0891B2",
)

_ISIC_ZW = _SystemMeta(
    id="isic_zw",
    name="ISIC Rev 4 (Zimbabwe)",
    full_name="Zimbabwe Standard Industrial Classification (ISIC Rev 4 based)",
    region="Zimbabwe",
    version="Rev 4",
    authority="Zimbabwe National Statistics Agency (ZIMSTAT)",
    tint_color="#B45309",
)

# ── Caribbean / Pacific / Central Asia / Additional Africa ─────

_ISIC_TT = _SystemMeta(
    id="isic_tt", name="ISIC Rev 4 (Trinidad and Tobago)",
    full_name="Trinidad and Tobago Industrial Classification (ISIC Rev 4 based)",
    region="Trinidad and Tobago", version="Rev 4",
    authority="Central Statistical Office Trinidad and Tobago (CSO)", tint_color="#DC2626",
)

_ISIC_JM = _SystemMeta(
    id="isic_jm", name="ISIC Rev 4 (Jamaica)",
    full_name="Jamaica Standard Industrial Classification (ISIC Rev 4 based)",
    region="Jamaica", version="Rev 4",
    authority="Statistical Institute of Jamaica (STATIN)", tint_color="#16A34A",
)

_ISIC_HT = _SystemMeta(
    id="isic_ht", name="ISIC Rev 4 (Haiti)",
    full_name="Haiti Classification Industrielle (ISIC Rev 4 based)",
    region="Haiti", version="Rev 4",
    authority="Institut Haitien de Statistique et d'Informatique (IHSI)", tint_color="#7C3AED",
)

_ISIC_FJ = _SystemMeta(
    id="isic_fj", name="ISIC Rev 4 (Fiji)",
    full_name="Fiji Standard Industrial Classification (ISIC Rev 4 based)",
    region="Fiji", version="Rev 4",
    authority="Fiji Bureau of Statistics (FBoS)", tint_color="#0891B2",
)

_ISIC_PG = _SystemMeta(
    id="isic_pg", name="ISIC Rev 4 (Papua New Guinea)",
    full_name="Papua New Guinea Industrial Classification (ISIC Rev 4 based)",
    region="Papua New Guinea", version="Rev 4",
    authority="National Statistical Office Papua New Guinea (NSO)", tint_color="#D97706",
)

_ISIC_MN = _SystemMeta(
    id="isic_mn", name="ISIC Rev 4 (Mongolia)",
    full_name="Mongolia Standard Industrial Classification (ISIC Rev 4 based)",
    region="Mongolia", version="Rev 4",
    authority="National Statistics Office of Mongolia (NSO)", tint_color="#059669",
)

_ISIC_KZ = _SystemMeta(
    id="isic_kz", name="ISIC Rev 4 (Kazakhstan)",
    full_name="Kazakhstan Standard Industrial Classification (ISIC Rev 4 based)",
    region="Kazakhstan", version="Rev 4",
    authority="Bureau of National Statistics of Kazakhstan", tint_color="#E11D48",
)

_ISIC_UZ = _SystemMeta(
    id="isic_uz", name="ISIC Rev 4 (Uzbekistan)",
    full_name="Uzbekistan Standard Industrial Classification (ISIC Rev 4 based)",
    region="Uzbekistan", version="Rev 4",
    authority="State Committee on Statistics of Uzbekistan", tint_color="#F59E0B",
)

_ISIC_AZ = _SystemMeta(
    id="isic_az", name="ISIC Rev 4 (Azerbaijan)",
    full_name="Azerbaijan Standard Industrial Classification (ISIC Rev 4 based)",
    region="Azerbaijan", version="Rev 4",
    authority="State Statistical Committee of Azerbaijan", tint_color="#9333EA",
)

_ISIC_CI = _SystemMeta(
    id="isic_ci", name="ISIC Rev 4 (Ivory Coast)",
    full_name="Classification Ivoirienne des Activites (ISIC Rev 4 based)",
    region="Ivory Coast", version="Rev 4",
    authority="Institut National de la Statistique de Cote d'Ivoire (INS)", tint_color="#15803D",
)

_ISIC_RW = _SystemMeta(
    id="isic_rw", name="ISIC Rev 4 (Rwanda)",
    full_name="Rwanda Standard Industrial Classification (ISIC Rev 4 based)",
    region="Rwanda", version="Rev 4",
    authority="National Institute of Statistics of Rwanda (NISR)", tint_color="#0284C7",
)

_ISIC_ZM = _SystemMeta(
    id="isic_zm", name="ISIC Rev 4 (Zambia)",
    full_name="Zambia Standard Industrial Classification (ISIC Rev 4 based)",
    region="Zambia", version="Rev 4",
    authority="Zambia Statistics Agency (ZamStats)", tint_color="#B91C1C",
)

_ISIC_BW = _SystemMeta(
    id="isic_bw", name="ISIC Rev 4 (Botswana)",
    full_name="Botswana Standard Industrial Classification (ISIC Rev 4 based)",
    region="Botswana", version="Rev 4",
    authority="Statistics Botswana (SB)", tint_color="#CA8A04",
)

_ISIC_NA = _SystemMeta(
    id="isic_na", name="ISIC Rev 4 (Namibia)",
    full_name="Namibia Standard Industrial Classification (ISIC Rev 4 based)",
    region="Namibia", version="Rev 4",
    authority="Namibia Statistics Agency (NSA)", tint_color="#4F46E5",
)

_ISIC_MG = _SystemMeta(
    id="isic_mg", name="ISIC Rev 4 (Madagascar)",
    full_name="Classification des Activites Economiques de Madagascar (ISIC Rev 4 based)",
    region="Madagascar", version="Rev 4",
    authority="Institut National de la Statistique de Madagascar (INSTAT)", tint_color="#EA580C",
)

_ISIC_MU = _SystemMeta(
    id="isic_mu", name="ISIC Rev 4 (Mauritius)",
    full_name="Mauritius Standard Industrial Classification (ISIC Rev 4 based)",
    region="Mauritius", version="Rev 4",
    authority="Statistics Mauritius", tint_color="#0369A1",
)

_ISIC_BF = _SystemMeta(
    id="isic_bf", name="ISIC Rev 4 (Burkina Faso)",
    full_name="Classification des Activites du Burkina Faso (ISIC Rev 4 based)",
    region="Burkina Faso", version="Rev 4",
    authority="Institut National de la Statistique et de la Demographie (INSD)", tint_color="#DC2626",
)

_ISIC_ML = _SystemMeta(
    id="isic_ml", name="ISIC Rev 4 (Mali)",
    full_name="Classification des Activites Economiques du Mali (ISIC Rev 4 based)",
    region="Mali", version="Rev 4",
    authority="Institut National de la Statistique du Mali (INSTAT)", tint_color="#16A34A",
)

_ISIC_MW = _SystemMeta(
    id="isic_mw", name="ISIC Rev 4 (Malawi)",
    full_name="Malawi Standard Industrial Classification (ISIC Rev 4 based)",
    region="Malawi", version="Rev 4",
    authority="National Statistical Office of Malawi (NSO)", tint_color="#7C3AED",
)

_ISIC_AF = _SystemMeta(
    id="isic_af", name="ISIC Rev 4 (Afghanistan)",
    full_name="Afghanistan Standard Industrial Classification (ISIC Rev 4 based)",
    region="Afghanistan", version="Rev 4",
    authority="National Statistics and Information Authority (NSIA)", tint_color="#D97706",
)


# ── Core derivation logic ───────────────────────────────────────


async def _ingest_derived_from_isic(conn, meta: _SystemMeta) -> int:
    """Generic ingester that copies isic_rev4 nodes into a derived system.

    Steps:
      1. Register the classification_system.
      2. Copy every classification_node from isic_rev4 with the new system_id.
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

    # 2. Copy nodes from isic_rev4
    isic_rows = await conn.fetch(
        """
        SELECT code, title, description, level, parent_code,
               sector_code, is_leaf, seq_order
        FROM classification_node
        WHERE system_id = 'isic_rev4'
        ORDER BY seq_order
        """
    )

    if not isic_rows:
        print(f"  WARNING: No isic_rev4 nodes found - {meta.id} will be empty")
        return 0

    count = 0
    for row in isic_rows:
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
    for row in isic_rows:
        code = row["code"]
        # Forward: derived -> isic_rev4
        await conn.execute(
            """
            INSERT INTO equivalence
                (source_system, source_code, target_system, target_code, match_type)
            VALUES ($1, $2, 'isic_rev4', $3, 'exact')
            ON CONFLICT (source_system, source_code, target_system, target_code)
            DO NOTHING
            """,
            meta.id,
            code,
            code,
        )
        # Reverse: isic_rev4 -> derived
        await conn.execute(
            """
            INSERT INTO equivalence
                (source_system, source_code, target_system, target_code, match_type)
            VALUES ('isic_rev4', $1, $2, $3, 'exact')
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

    print(f"  Ingested {count} {meta.name} codes (derived from ISIC Rev 4)")
    return count


# ── Public API ──────────────────────────────────────────────────


async def ingest_ciiu_co(conn) -> int:
    """Ingest Colombian CIIU Rev 4 AC (Clasificacion Industrial Internacional Uniforme).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from DANE data files.
    """
    return await _ingest_derived_from_isic(conn, _CIIU_CO)


async def ingest_ciiu_ar(conn) -> int:
    """Ingest Argentine CLANAE Rev 4 (Clasificador Nacional de Actividades Economicas).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from INDEC data files.
    """
    return await _ingest_derived_from_isic(conn, _CIIU_AR)


async def ingest_ciiu_cl(conn) -> int:
    """Ingest Chilean CIIU Rev 4 (Clasificacion Industrial Internacional Uniforme).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from INE Chile data files.
    """
    return await _ingest_derived_from_isic(conn, _CIIU_CL)


async def ingest_ciiu_pe(conn) -> int:
    """Ingest Peruvian CIIU Rev 4 (Clasificacion Industrial Internacional Uniforme).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from INEI data files.
    """
    return await _ingest_derived_from_isic(conn, _CIIU_PE)


async def ingest_ciiu_ec(conn) -> int:
    """Ingest Ecuadorian CIIU Rev 4 (Clasificacion Industrial Internacional Uniforme).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from INEC Ecuador data files.
    """
    return await _ingest_derived_from_isic(conn, _CIIU_EC)


async def ingest_caeb(conn) -> int:
    """Ingest Bolivian CAEB (Clasificacion de Actividades Economicas de Bolivia).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from INE Bolivia data files.
    """
    return await _ingest_derived_from_isic(conn, _CAEB)


async def ingest_ciiu_ve(conn) -> int:
    """Ingest Venezuelan CIIU Rev 4 (Clasificacion Industrial Internacional Uniforme).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from INE Venezuela data files.
    """
    return await _ingest_derived_from_isic(conn, _CIIU_VE)


async def ingest_ciiu_cr(conn) -> int:
    """Ingest Costa Rican CIIU Rev 4 (Clasificacion Industrial Internacional Uniforme).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from INEC Costa Rica data files.
    """
    return await _ingest_derived_from_isic(conn, _CIIU_CR)


async def ingest_ciiu_gt(conn) -> int:
    """Ingest Guatemalan CIIU Rev 4 (Clasificacion Industrial Internacional Uniforme).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from INE Guatemala data files.
    """
    return await _ingest_derived_from_isic(conn, _CIIU_GT)


async def ingest_ciiu_pa(conn) -> int:
    """Ingest Panamanian CIIU Rev 4 (Clasificacion Industrial Internacional Uniforme).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from INEC Panama data files.
    """
    return await _ingest_derived_from_isic(conn, _CIIU_PA)


async def ingest_vsic_2018(conn) -> int:
    """Ingest Vietnamese VSIC 2018 (Vietnam Standard Industrial Classification).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from GSO Vietnam data files.
    """
    return await _ingest_derived_from_isic(conn, _VSIC_2018)


async def ingest_bsic(conn) -> int:
    """Ingest Bangladesh Standard Industrial Classification (BSIC).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from BBS data files.
    """
    return await _ingest_derived_from_isic(conn, _BSIC)


async def ingest_psic_pk(conn) -> int:
    """Ingest Pakistan Standard Industrial Classification (PSIC).

    Derives all codes from ISIC Rev 4 already present in the database.
    Note: distinct from Philippines psic_2009 (same acronym, different country).
    """
    return await _ingest_derived_from_isic(conn, _PSIC_PK)


async def ingest_isic_ng(conn) -> int:
    """Ingest Nigeria Standard Industrial Classification (ISIC Rev 4 based).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from NBS Nigeria data files.
    """
    return await _ingest_derived_from_isic(conn, _ISIC_NG)


async def ingest_isic_ke(conn) -> int:
    """Ingest Kenya Standard Industrial Classification (ISIC Rev 4 based).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from KNBS data files.
    """
    return await _ingest_derived_from_isic(conn, _ISIC_KE)


async def ingest_isic_eg(conn) -> int:
    """Ingest Egypt Standard Industrial Classification (ISIC Rev 4 based).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from CAPMAS data files.
    """
    return await _ingest_derived_from_isic(conn, _ISIC_EG)


async def ingest_isic_sa(conn) -> int:
    """Ingest Saudi Arabia Standard Industrial Classification (ISIC Rev 4 based).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from GASTAT data files.
    """
    return await _ingest_derived_from_isic(conn, _ISIC_SA)


async def ingest_isic_ae(conn) -> int:
    """Ingest UAE Standard Industrial Classification (ISIC Rev 4 based).

    Derives all codes from ISIC Rev 4 already present in the database.
    National extensions can be added later from FCSC data files.
    """
    return await _ingest_derived_from_isic(conn, _ISIC_AE)


async def ingest_kbli_id(conn) -> int:
    """Ingest Indonesian KBLI 2020 (Klasifikasi Baku Lapangan Usaha Indonesia)."""
    return await _ingest_derived_from_isic(conn, _KBLI_ID)


async def ingest_slsic(conn) -> int:
    """Ingest Sri Lanka Standard Industrial Classification (SLSIC)."""
    return await _ingest_derived_from_isic(conn, _SLSIC)


async def ingest_isic_mm(conn) -> int:
    """Ingest Myanmar Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_MM)


async def ingest_isic_kh(conn) -> int:
    """Ingest Cambodia Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_KH)


async def ingest_isic_la(conn) -> int:
    """Ingest Lao Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_LA)


async def ingest_isic_np(conn) -> int:
    """Ingest Nepal Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_NP)


async def ingest_isic_et(conn) -> int:
    """Ingest Ethiopia Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_ET)


async def ingest_isic_tz(conn) -> int:
    """Ingest Tanzania Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_TZ)


async def ingest_isic_gh(conn) -> int:
    """Ingest Ghana Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_GH)


async def ingest_isic_ma(conn) -> int:
    """Ingest Morocco Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_MA)


async def ingest_isic_tn(conn) -> int:
    """Ingest Tunisia Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_TN)


async def ingest_isic_dz(conn) -> int:
    """Ingest Algeria Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_DZ)


async def ingest_isic_sn(conn) -> int:
    """Ingest Senegal Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_SN)


async def ingest_isic_cm(conn) -> int:
    """Ingest Cameroon Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_CM)


async def ingest_isic_ug(conn) -> int:
    """Ingest Uganda Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_UG)


async def ingest_isic_mz(conn) -> int:
    """Ingest Mozambique Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_MZ)


async def ingest_isic_iq(conn) -> int:
    """Ingest Iraq Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_IQ)


async def ingest_isic_jo(conn) -> int:
    """Ingest Jordan Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_JO)


async def ingest_ciiu_py(conn) -> int:
    """Ingest Paraguayan CIIU Rev 4 (Clasificacion Industrial Internacional Uniforme)."""
    return await _ingest_derived_from_isic(conn, _CIIU_PY)


async def ingest_ciiu_uy(conn) -> int:
    """Ingest Uruguayan CIIU Rev 4 (Clasificacion Industrial Internacional Uniforme)."""
    return await _ingest_derived_from_isic(conn, _CIIU_UY)


async def ingest_ciiu_do(conn) -> int:
    """Ingest Dominican Republic CIIU Rev 4."""
    return await _ingest_derived_from_isic(conn, _CIIU_DO)


async def ingest_isic_hn(conn) -> int:
    """Ingest Honduras Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_HN)


async def ingest_isic_sv(conn) -> int:
    """Ingest El Salvador Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_SV)


async def ingest_isic_ni(conn) -> int:
    """Ingest Nicaragua Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_NI)


async def ingest_isic_zw(conn) -> int:
    """Ingest Zimbabwe Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_ZW)


async def ingest_isic_tt(conn) -> int:
    """Ingest Trinidad and Tobago Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_TT)


async def ingest_isic_jm(conn) -> int:
    """Ingest Jamaica Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_JM)


async def ingest_isic_ht(conn) -> int:
    """Ingest Haiti Classification Industrielle (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_HT)


async def ingest_isic_fj(conn) -> int:
    """Ingest Fiji Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_FJ)


async def ingest_isic_pg(conn) -> int:
    """Ingest Papua New Guinea Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_PG)


async def ingest_isic_mn(conn) -> int:
    """Ingest Mongolia Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_MN)


async def ingest_isic_kz(conn) -> int:
    """Ingest Kazakhstan Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_KZ)


async def ingest_isic_uz(conn) -> int:
    """Ingest Uzbekistan Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_UZ)


async def ingest_isic_az(conn) -> int:
    """Ingest Azerbaijan Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_AZ)


async def ingest_isic_ci(conn) -> int:
    """Ingest Ivory Coast Classification des Activites (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_CI)


async def ingest_isic_rw(conn) -> int:
    """Ingest Rwanda Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_RW)


async def ingest_isic_zm(conn) -> int:
    """Ingest Zambia Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_ZM)


async def ingest_isic_bw(conn) -> int:
    """Ingest Botswana Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_BW)


async def ingest_isic_na(conn) -> int:
    """Ingest Namibia Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_NA)


async def ingest_isic_mg(conn) -> int:
    """Ingest Madagascar Classification des Activites (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_MG)


async def ingest_isic_mu(conn) -> int:
    """Ingest Mauritius Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_MU)


async def ingest_isic_bf(conn) -> int:
    """Ingest Burkina Faso Classification des Activites (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_BF)


async def ingest_isic_ml(conn) -> int:
    """Ingest Mali Classification des Activites (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_ML)


async def ingest_isic_mw(conn) -> int:
    """Ingest Malawi Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_MW)


async def ingest_isic_af(conn) -> int:
    """Ingest Afghanistan Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_AF)

# ── Phase 1-3: 59 additional ISIC-derived systems ──────────────

_ISIC_LB = _SystemMeta(
    id="isic_lb", name="ISIC Rev 4 (Lebanon)",
    full_name="Lebanon Standard Industrial Classification (ISIC Rev 4 based)",
    region="Lebanon", version="Rev 4",
    authority="Lebanese Central Administration of Statistics (CAS)", tint_color="#059669",
)

_ISIC_OM = _SystemMeta(
    id="isic_om", name="ISIC Rev 4 (Oman)",
    full_name="Oman Standard Industrial Classification (ISIC Rev 4 based)",
    region="Oman", version="Rev 4",
    authority="National Centre for Statistics and Information (NCSI Oman)", tint_color="#7C3AED",
)

_ISIC_QA = _SystemMeta(
    id="isic_qa", name="ISIC Rev 4 (Qatar)",
    full_name="Qatar Standard Industrial Classification (ISIC Rev 4 based)",
    region="Qatar", version="Rev 4",
    authority="Planning and Statistics Authority (PSA Qatar)", tint_color="#0891B2",
)

_ISIC_BH = _SystemMeta(
    id="isic_bh", name="ISIC Rev 4 (Bahrain)",
    full_name="Bahrain Standard Industrial Classification (ISIC Rev 4 based)",
    region="Bahrain", version="Rev 4",
    authority="Information and eGovernment Authority (iGA Bahrain)", tint_color="#DC2626",
)

_ISIC_KW = _SystemMeta(
    id="isic_kw", name="ISIC Rev 4 (Kuwait)",
    full_name="Kuwait Standard Industrial Classification (ISIC Rev 4 based)",
    region="Kuwait", version="Rev 4",
    authority="Central Statistical Bureau (CSB Kuwait)", tint_color="#CA8A04",
)

_ISIC_YE = _SystemMeta(
    id="isic_ye", name="ISIC Rev 4 (Yemen)",
    full_name="Yemen Standard Industrial Classification (ISIC Rev 4 based)",
    region="Yemen", version="Rev 4",
    authority="Central Statistical Organization (CSO Yemen)", tint_color="#B45309",
)

_ISIC_IR = _SystemMeta(
    id="isic_ir", name="ISIC Rev 4 (Iran)",
    full_name="Iran Standard Industrial Classification (ISIC Rev 4 based)",
    region="Iran", version="Rev 4",
    authority="Statistical Centre of Iran (SCI)", tint_color="#4338CA",
)

_ISIC_LY = _SystemMeta(
    id="isic_ly", name="ISIC Rev 4 (Libya)",
    full_name="Libya Standard Industrial Classification (ISIC Rev 4 based)",
    region="Libya", version="Rev 4",
    authority="Bureau of Statistics and Census Libya (BSC)", tint_color="#0D9488",
)

_ISIC_IL = _SystemMeta(
    id="isic_il", name="ISIC Rev 4 (Israel)",
    full_name="Israel Standard Industrial Classification (ISIC Rev 4 based)",
    region="Israel", version="Rev 4",
    authority="Israel Central Bureau of Statistics (CBS)", tint_color="#2563EB",
)

_ISIC_PS = _SystemMeta(
    id="isic_ps", name="ISIC Rev 4 (Palestine)",
    full_name="Palestine Standard Industrial Classification (ISIC Rev 4 based)",
    region="Palestine", version="Rev 4",
    authority="Palestinian Central Bureau of Statistics (PCBS)", tint_color="#16A34A",
)

_ISIC_SY = _SystemMeta(
    id="isic_sy", name="ISIC Rev 4 (Syria)",
    full_name="Syria Standard Industrial Classification (ISIC Rev 4 based)",
    region="Syria", version="Rev 4",
    authority="Central Bureau of Statistics Syria (CBS)", tint_color="#9333EA",
)

_ISIC_KG = _SystemMeta(
    id="isic_kg", name="ISIC Rev 4 (Kyrgyzstan)",
    full_name="Kyrgyzstan Standard Industrial Classification (ISIC Rev 4 based)",
    region="Kyrgyzstan", version="Rev 4",
    authority="National Statistical Committee of the Kyrgyz Republic", tint_color="#0369A1",
)

_ISIC_TJ = _SystemMeta(
    id="isic_tj", name="ISIC Rev 4 (Tajikistan)",
    full_name="Tajikistan Standard Industrial Classification (ISIC Rev 4 based)",
    region="Tajikistan", version="Rev 4",
    authority="Agency on Statistics under the President of Tajikistan", tint_color="#B91C1C",
)

_ISIC_TM = _SystemMeta(
    id="isic_tm", name="ISIC Rev 4 (Turkmenistan)",
    full_name="Turkmenistan Standard Industrial Classification (ISIC Rev 4 based)",
    region="Turkmenistan", version="Rev 4",
    authority="State Statistics Committee of Turkmenistan", tint_color="#A16207",
)

_ISIC_CU = _SystemMeta(
    id="isic_cu", name="ISIC Rev 4 (Cuba)",
    full_name="Cuba Standard Industrial Classification (ISIC Rev 4 based)",
    region="Cuba", version="Rev 4",
    authority="Oficina Nacional de Estadisticas e Informacion (ONEI Cuba)", tint_color="#E11D48",
)

_ISIC_BB = _SystemMeta(
    id="isic_bb", name="ISIC Rev 4 (Barbados)",
    full_name="Barbados Standard Industrial Classification (ISIC Rev 4 based)",
    region="Barbados", version="Rev 4",
    authority="Barbados Statistical Service (BSS)", tint_color="#7C3AED",
)

_ISIC_BS = _SystemMeta(
    id="isic_bs", name="ISIC Rev 4 (Bahamas)",
    full_name="Bahamas Standard Industrial Classification (ISIC Rev 4 based)",
    region="Bahamas", version="Rev 4",
    authority="Department of Statistics Bahamas", tint_color="#0891B2",
)

_ISIC_GY = _SystemMeta(
    id="isic_gy", name="ISIC Rev 4 (Guyana)",
    full_name="Guyana Standard Industrial Classification (ISIC Rev 4 based)",
    region="Guyana", version="Rev 4",
    authority="Bureau of Statistics Guyana", tint_color="#059669",
)

_ISIC_SR = _SystemMeta(
    id="isic_sr", name="ISIC Rev 4 (Suriname)",
    full_name="Suriname Standard Industrial Classification (ISIC Rev 4 based)",
    region="Suriname", version="Rev 4",
    authority="Algemeen Bureau voor de Statistiek (ABS Suriname)", tint_color="#DC2626",
)

_ISIC_BZ = _SystemMeta(
    id="isic_bz", name="ISIC Rev 4 (Belize)",
    full_name="Belize Standard Industrial Classification (ISIC Rev 4 based)",
    region="Belize", version="Rev 4",
    authority="Statistical Institute of Belize (SIB)", tint_color="#CA8A04",
)

_ISIC_AG = _SystemMeta(
    id="isic_ag", name="ISIC Rev 4 (Antigua and Barbuda)",
    full_name="Antigua and Barbuda Industrial Classification (ISIC Rev 4 based)",
    region="Antigua and Barbuda", version="Rev 4",
    authority="Statistics Division Antigua and Barbuda", tint_color="#F97316",
)

_ISIC_LC = _SystemMeta(
    id="isic_lc", name="ISIC Rev 4 (Saint Lucia)",
    full_name="Saint Lucia Industrial Classification (ISIC Rev 4 based)",
    region="Saint Lucia", version="Rev 4",
    authority="Central Statistical Office Saint Lucia", tint_color="#4338CA",
)

_ISIC_GD = _SystemMeta(
    id="isic_gd", name="ISIC Rev 4 (Grenada)",
    full_name="Grenada Industrial Classification (ISIC Rev 4 based)",
    region="Grenada", version="Rev 4",
    authority="Central Statistical Office Grenada", tint_color="#0D9488",
)

_ISIC_VC = _SystemMeta(
    id="isic_vc", name="ISIC Rev 4 (Saint Vincent)",
    full_name="Saint Vincent Industrial Classification (ISIC Rev 4 based)",
    region="Saint Vincent and the Grenadines", version="Rev 4",
    authority="Statistical Office Saint Vincent", tint_color="#B45309",
)

_ISIC_DM = _SystemMeta(
    id="isic_dm", name="ISIC Rev 4 (Dominica)",
    full_name="Dominica Industrial Classification (ISIC Rev 4 based)",
    region="Dominica", version="Rev 4",
    authority="Central Statistical Office Dominica", tint_color="#9333EA",
)

_ISIC_KN = _SystemMeta(
    id="isic_kn", name="ISIC Rev 4 (Saint Kitts)",
    full_name="Saint Kitts and Nevis Industrial Classification (ISIC Rev 4 based)",
    region="Saint Kitts and Nevis", version="Rev 4",
    authority="Statistics Department Saint Kitts and Nevis", tint_color="#2563EB",
)

_ISIC_SS = _SystemMeta(
    id="isic_ss", name="ISIC Rev 4 (South Sudan)",
    full_name="South Sudan Standard Industrial Classification (ISIC Rev 4 based)",
    region="South Sudan", version="Rev 4",
    authority="National Bureau of Statistics (NBS South Sudan)", tint_color="#16A34A",
)

_ISIC_SO = _SystemMeta(
    id="isic_so", name="ISIC Rev 4 (Somalia)",
    full_name="Somalia Standard Industrial Classification (ISIC Rev 4 based)",
    region="Somalia", version="Rev 4",
    authority="Directorate of National Statistics Somalia", tint_color="#E11D48",
)

_ISIC_GN = _SystemMeta(
    id="isic_gn", name="ISIC Rev 4 (Guinea)",
    full_name="Guinea Standard Industrial Classification (ISIC Rev 4 based)",
    region="Guinea", version="Rev 4",
    authority="Institut National de la Statistique (INS Guinea)", tint_color="#7C3AED",
)

_ISIC_SL = _SystemMeta(
    id="isic_sl", name="ISIC Rev 4 (Sierra Leone)",
    full_name="Sierra Leone Standard Industrial Classification (ISIC Rev 4 based)",
    region="Sierra Leone", version="Rev 4",
    authority="Statistics Sierra Leone (SSL)", tint_color="#0891B2",
)

_ISIC_LR = _SystemMeta(
    id="isic_lr", name="ISIC Rev 4 (Liberia)",
    full_name="Liberia Standard Industrial Classification (ISIC Rev 4 based)",
    region="Liberia", version="Rev 4",
    authority="Liberia Institute of Statistics and Geo-Information Services (LISGIS)", tint_color="#059669",
)

_ISIC_TG = _SystemMeta(
    id="isic_tg", name="ISIC Rev 4 (Togo)",
    full_name="Togo Standard Industrial Classification (ISIC Rev 4 based)",
    region="Togo", version="Rev 4",
    authority="Institut National de la Statistique et des Etudes Economiques (INSEED Togo)", tint_color="#DC2626",
)

_ISIC_BJ = _SystemMeta(
    id="isic_bj", name="ISIC Rev 4 (Benin)",
    full_name="Benin Standard Industrial Classification (ISIC Rev 4 based)",
    region="Benin", version="Rev 4",
    authority="Institut National de la Statistique et de la Demographie (INStaD Benin)", tint_color="#CA8A04",
)

_ISIC_NE = _SystemMeta(
    id="isic_ne", name="ISIC Rev 4 (Niger)",
    full_name="Niger Standard Industrial Classification (ISIC Rev 4 based)",
    region="Niger", version="Rev 4",
    authority="Institut National de la Statistique (INS Niger)", tint_color="#A16207",
)

_ISIC_TD = _SystemMeta(
    id="isic_td", name="ISIC Rev 4 (Chad)",
    full_name="Chad Standard Industrial Classification (ISIC Rev 4 based)",
    region="Chad", version="Rev 4",
    authority="Institut National de la Statistique (INSEED Chad)", tint_color="#B91C1C",
)

_ISIC_CD = _SystemMeta(
    id="isic_cd", name="ISIC Rev 4 (DRC)",
    full_name="Democratic Republic of Congo Industrial Classification (ISIC Rev 4 based)",
    region="Democratic Republic of the Congo", version="Rev 4",
    authority="Institut National de la Statistique (INS DRC)", tint_color="#0369A1",
)

_ISIC_AO = _SystemMeta(
    id="isic_ao", name="ISIC Rev 4 (Angola)",
    full_name="Angola Standard Industrial Classification (ISIC Rev 4 based)",
    region="Angola", version="Rev 4",
    authority="Instituto Nacional de Estatistica Angola (INE)", tint_color="#4338CA",
)

_ISIC_GA = _SystemMeta(
    id="isic_ga", name="ISIC Rev 4 (Gabon)",
    full_name="Gabon Standard Industrial Classification (ISIC Rev 4 based)",
    region="Gabon", version="Rev 4",
    authority="Direction Generale de la Statistique (DGS Gabon)", tint_color="#0D9488",
)

_ISIC_GQ = _SystemMeta(
    id="isic_gq", name="ISIC Rev 4 (Equatorial Guinea)",
    full_name="Equatorial Guinea Industrial Classification (ISIC Rev 4 based)",
    region="Equatorial Guinea", version="Rev 4",
    authority="Instituto Nacional de Estadistica (INEGE)", tint_color="#F97316",
)

_ISIC_CG = _SystemMeta(
    id="isic_cg", name="ISIC Rev 4 (Congo Republic)",
    full_name="Republic of Congo Industrial Classification (ISIC Rev 4 based)",
    region="Republic of the Congo", version="Rev 4",
    authority="Institut National de la Statistique (INS Congo)", tint_color="#B45309",
)

_ISIC_KM = _SystemMeta(
    id="isic_km", name="ISIC Rev 4 (Comoros)",
    full_name="Comoros Industrial Classification (ISIC Rev 4 based)",
    region="Comoros", version="Rev 4",
    authority="Direction Nationale de la Statistique (DNS Comoros)", tint_color="#9333EA",
)

_ISIC_DJ = _SystemMeta(
    id="isic_dj", name="ISIC Rev 4 (Djibouti)",
    full_name="Djibouti Industrial Classification (ISIC Rev 4 based)",
    region="Djibouti", version="Rev 4",
    authority="Direction de la Statistique et des Etudes Demographiques (DISED Djibouti)", tint_color="#2563EB",
)

_ISIC_CV = _SystemMeta(
    id="isic_cv", name="ISIC Rev 4 (Cabo Verde)",
    full_name="Cabo Verde Industrial Classification (ISIC Rev 4 based)",
    region="Cabo Verde", version="Rev 4",
    authority="Instituto Nacional de Estatistica Cabo Verde (INE)", tint_color="#16A34A",
)

_ISIC_GM = _SystemMeta(
    id="isic_gm", name="ISIC Rev 4 (Gambia)",
    full_name="Gambia Industrial Classification (ISIC Rev 4 based)",
    region="Gambia", version="Rev 4",
    authority="Gambia Bureau of Statistics (GBoS)", tint_color="#E11D48",
)

_ISIC_GW = _SystemMeta(
    id="isic_gw", name="ISIC Rev 4 (Guinea-Bissau)",
    full_name="Guinea-Bissau Industrial Classification (ISIC Rev 4 based)",
    region="Guinea-Bissau", version="Rev 4",
    authority="Instituto Nacional de Estatistica (INE Guinea-Bissau)", tint_color="#7C3AED",
)

_ISIC_MR = _SystemMeta(
    id="isic_mr", name="ISIC Rev 4 (Mauritania)",
    full_name="Mauritania Industrial Classification (ISIC Rev 4 based)",
    region="Mauritania", version="Rev 4",
    authority="Office National de la Statistique (ONS Mauritania)", tint_color="#0891B2",
)

_ISIC_SZ = _SystemMeta(
    id="isic_sz", name="ISIC Rev 4 (Eswatini)",
    full_name="Eswatini Industrial Classification (ISIC Rev 4 based)",
    region="Eswatini", version="Rev 4",
    authority="Central Statistical Office Eswatini (CSO)", tint_color="#059669",
)

_ISIC_LS = _SystemMeta(
    id="isic_ls", name="ISIC Rev 4 (Lesotho)",
    full_name="Lesotho Industrial Classification (ISIC Rev 4 based)",
    region="Lesotho", version="Rev 4",
    authority="Bureau of Statistics Lesotho (BOS)", tint_color="#DC2626",
)

_ISIC_BI = _SystemMeta(
    id="isic_bi", name="ISIC Rev 4 (Burundi)",
    full_name="Burundi Industrial Classification (ISIC Rev 4 based)",
    region="Burundi", version="Rev 4",
    authority="Institut de Statistiques et d'Etudes Economiques du Burundi (ISTEEBU)", tint_color="#CA8A04",
)

_ISIC_ER = _SystemMeta(
    id="isic_er", name="ISIC Rev 4 (Eritrea)",
    full_name="Eritrea Industrial Classification (ISIC Rev 4 based)",
    region="Eritrea", version="Rev 4",
    authority="National Statistics Office Eritrea (NSO)", tint_color="#A16207",
)

_ISIC_SC = _SystemMeta(
    id="isic_sc", name="ISIC Rev 4 (Seychelles)",
    full_name="Seychelles Industrial Classification (ISIC Rev 4 based)",
    region="Seychelles", version="Rev 4",
    authority="National Bureau of Statistics Seychelles (NBS)", tint_color="#B91C1C",
)

_ISIC_WS = _SystemMeta(
    id="isic_ws", name="ISIC Rev 4 (Samoa)",
    full_name="Samoa Industrial Classification (ISIC Rev 4 based)",
    region="Samoa", version="Rev 4",
    authority="Samoa Bureau of Statistics (SBS)", tint_color="#0369A1",
)

_ISIC_TO = _SystemMeta(
    id="isic_to", name="ISIC Rev 4 (Tonga)",
    full_name="Tonga Industrial Classification (ISIC Rev 4 based)",
    region="Tonga", version="Rev 4",
    authority="Tonga Statistics Department (TSD)", tint_color="#4338CA",
)

_ISIC_VU = _SystemMeta(
    id="isic_vu", name="ISIC Rev 4 (Vanuatu)",
    full_name="Vanuatu Industrial Classification (ISIC Rev 4 based)",
    region="Vanuatu", version="Rev 4",
    authority="Vanuatu National Statistics Office (VNSO)", tint_color="#0D9488",
)

_ISIC_SB = _SystemMeta(
    id="isic_sb", name="ISIC Rev 4 (Solomon Islands)",
    full_name="Solomon Islands Industrial Classification (ISIC Rev 4 based)",
    region="Solomon Islands", version="Rev 4",
    authority="Solomon Islands National Statistics Office (SINSO)", tint_color="#F97316",
)

_ISIC_BN = _SystemMeta(
    id="isic_bn", name="ISIC Rev 4 (Brunei)",
    full_name="Brunei Industrial Classification (ISIC Rev 4 based)",
    region="Brunei", version="Rev 4",
    authority="Department of Economic Planning and Statistics (DEPS Brunei)", tint_color="#B45309",
)

_ISIC_TL = _SystemMeta(
    id="isic_tl", name="ISIC Rev 4 (East Timor)",
    full_name="Timor-Leste Industrial Classification (ISIC Rev 4 based)",
    region="East Timor", version="Rev 4",
    authority="General Directorate of Statistics Timor-Leste (GDS)", tint_color="#9333EA",
)

_ISIC_BT = _SystemMeta(
    id="isic_bt", name="ISIC Rev 4 (Bhutan)",
    full_name="Bhutan Industrial Classification (ISIC Rev 4 based)",
    region="Bhutan", version="Rev 4",
    authority="National Statistics Bureau Bhutan (NSB)", tint_color="#2563EB",
)

_ISIC_MV = _SystemMeta(
    id="isic_mv", name="ISIC Rev 4 (Maldives)",
    full_name="Maldives Industrial Classification (ISIC Rev 4 based)",
    region="Maldives", version="Rev 4",
    authority="Maldives Bureau of Statistics (MBS)", tint_color="#16A34A",
)

async def ingest_isic_lb(conn) -> int:
    """Ingest Lebanon Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_LB)

async def ingest_isic_om(conn) -> int:
    """Ingest Oman Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_OM)

async def ingest_isic_qa(conn) -> int:
    """Ingest Qatar Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_QA)

async def ingest_isic_bh(conn) -> int:
    """Ingest Bahrain Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_BH)

async def ingest_isic_kw(conn) -> int:
    """Ingest Kuwait Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_KW)

async def ingest_isic_ye(conn) -> int:
    """Ingest Yemen Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_YE)

async def ingest_isic_ir(conn) -> int:
    """Ingest Iran Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_IR)

async def ingest_isic_ly(conn) -> int:
    """Ingest Libya Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_LY)

async def ingest_isic_il(conn) -> int:
    """Ingest Israel Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_IL)

async def ingest_isic_ps(conn) -> int:
    """Ingest Palestine Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_PS)

async def ingest_isic_sy(conn) -> int:
    """Ingest Syria Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_SY)

async def ingest_isic_kg(conn) -> int:
    """Ingest Kyrgyzstan Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_KG)

async def ingest_isic_tj(conn) -> int:
    """Ingest Tajikistan Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_TJ)

async def ingest_isic_tm(conn) -> int:
    """Ingest Turkmenistan Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_TM)

async def ingest_isic_cu(conn) -> int:
    """Ingest Cuba Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_CU)

async def ingest_isic_bb(conn) -> int:
    """Ingest Barbados Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_BB)

async def ingest_isic_bs(conn) -> int:
    """Ingest Bahamas Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_BS)

async def ingest_isic_gy(conn) -> int:
    """Ingest Guyana Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_GY)

async def ingest_isic_sr(conn) -> int:
    """Ingest Suriname Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_SR)

async def ingest_isic_bz(conn) -> int:
    """Ingest Belize Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_BZ)

async def ingest_isic_ag(conn) -> int:
    """Ingest Antigua and Barbuda Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_AG)

async def ingest_isic_lc(conn) -> int:
    """Ingest Saint Lucia Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_LC)

async def ingest_isic_gd(conn) -> int:
    """Ingest Grenada Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_GD)

async def ingest_isic_vc(conn) -> int:
    """Ingest Saint Vincent and the Grenadines Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_VC)

async def ingest_isic_dm(conn) -> int:
    """Ingest Dominica Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_DM)

async def ingest_isic_kn(conn) -> int:
    """Ingest Saint Kitts and Nevis Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_KN)

async def ingest_isic_ss(conn) -> int:
    """Ingest South Sudan Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_SS)

async def ingest_isic_so(conn) -> int:
    """Ingest Somalia Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_SO)

async def ingest_isic_gn(conn) -> int:
    """Ingest Guinea Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_GN)

async def ingest_isic_sl(conn) -> int:
    """Ingest Sierra Leone Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_SL)

async def ingest_isic_lr(conn) -> int:
    """Ingest Liberia Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_LR)

async def ingest_isic_tg(conn) -> int:
    """Ingest Togo Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_TG)

async def ingest_isic_bj(conn) -> int:
    """Ingest Benin Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_BJ)

async def ingest_isic_ne(conn) -> int:
    """Ingest Niger Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_NE)

async def ingest_isic_td(conn) -> int:
    """Ingest Chad Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_TD)

async def ingest_isic_cd(conn) -> int:
    """Ingest Democratic Republic of the Congo Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_CD)

async def ingest_isic_ao(conn) -> int:
    """Ingest Angola Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_AO)

async def ingest_isic_ga(conn) -> int:
    """Ingest Gabon Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_GA)

async def ingest_isic_gq(conn) -> int:
    """Ingest Equatorial Guinea Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_GQ)

async def ingest_isic_cg(conn) -> int:
    """Ingest Republic of the Congo Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_CG)

async def ingest_isic_km(conn) -> int:
    """Ingest Comoros Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_KM)

async def ingest_isic_dj(conn) -> int:
    """Ingest Djibouti Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_DJ)

async def ingest_isic_cv(conn) -> int:
    """Ingest Cabo Verde Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_CV)

async def ingest_isic_gm(conn) -> int:
    """Ingest Gambia Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_GM)

async def ingest_isic_gw(conn) -> int:
    """Ingest Guinea-Bissau Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_GW)

async def ingest_isic_mr(conn) -> int:
    """Ingest Mauritania Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_MR)

async def ingest_isic_sz(conn) -> int:
    """Ingest Eswatini Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_SZ)

async def ingest_isic_ls(conn) -> int:
    """Ingest Lesotho Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_LS)

async def ingest_isic_bi(conn) -> int:
    """Ingest Burundi Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_BI)

async def ingest_isic_er(conn) -> int:
    """Ingest Eritrea Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_ER)

async def ingest_isic_sc(conn) -> int:
    """Ingest Seychelles Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_SC)

async def ingest_isic_ws(conn) -> int:
    """Ingest Samoa Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_WS)

async def ingest_isic_to(conn) -> int:
    """Ingest Tonga Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_TO)

async def ingest_isic_vu(conn) -> int:
    """Ingest Vanuatu Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_VU)

async def ingest_isic_sb(conn) -> int:
    """Ingest Solomon Islands Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_SB)

async def ingest_isic_bn(conn) -> int:
    """Ingest Brunei Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_BN)

async def ingest_isic_tl(conn) -> int:
    """Ingest East Timor Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_TL)

async def ingest_isic_bt(conn) -> int:
    """Ingest Bhutan Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_BT)

async def ingest_isic_mv(conn) -> int:
    """Ingest Maldives Standard Industrial Classification (ISIC Rev 4 based)."""
    return await _ingest_derived_from_isic(conn, _ISIC_MV)