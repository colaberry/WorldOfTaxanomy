import type { ClassificationSystem } from './types'

export interface SystemCategory {
  id: string
  label: string
  description: string
  accent: string       // border/icon color (CSS color)
  bg: string           // card background tint (CSS color, low opacity)
  systemIds: string[]  // exact system IDs; domain_* matched by prefix
}

export interface DomainSector {
  id: string           // e.g., 'truck'
  label: string        // e.g., 'Trucking'
  prefix: string       // e.g., 'domain_truck_' - used for prefix matching
  accent: string       // distinct hex color per sector
  group: 'traditional' | 'emerging'
  extraIds?: string[]  // domain systems that don't match the prefix
}

export const SYSTEM_CATEGORIES: SystemCategory[] = [
  {
    id: 'education',
    label: 'Education',
    description: 'Academic programs, credentials, and educational standards',
    accent: '#059669',
    bg: 'rgba(5,150,105,0.08)',
    systemIds: [
      'isced_2011', 'iscedf_2013', 'cip_2020',
      'ngss', 'ccss', 'bloom_taxonomy',
    ],
  },
  {
    id: 'environmental',
    label: 'Environmental & Earth Sciences',
    description: 'Conservation, biodiversity, climate data, and earth science classifications',
    accent: '#16A34A',
    bg: 'rgba(22,163,74,0.08)',
    systemIds: [
      'iucn_red_list', 'cites', 'ramsar',
      'stockholm_pops', 'rotterdam_pic', 'minamata',
      'unep_chemicals', 'epa_rcra_waste', 'eu_waste_cat',
      'fao_aquastat', 'fao_stat_domain',
      'iea_energy_bal', 'irena_re',
      'usda_soil', 'koppen_climate', 'geological_time',
      'cbd_aichi', 'cbd_targets', 'un_sdg_indicators',
    ],
  },
  {
    id: 'financial',
    label: 'Financial / Investment',
    description: 'Capital markets, financial instruments, ESG, and government finance',
    accent: '#10B981',
    bg: 'rgba(16,185,129,0.08)',
    systemIds: [
      'gics_bridge', 'icb', 'cfi_iso10962',
      'cofog', 'coicop', 'ghg_protocol', 'patent_cpc',
      'bloomberg_bics', 'refinitiv_trbc', 'ftse_icb_detail', 'ifrs',
      'naic_lines', 'basel_exposure',
      'swift_mt', 'iso20022_msg', 'card_schemes',
      'corporate_action', 'opec_basket', 'lme_metals',
      'vat_rates', 'irs_forms',
      'board_committee', 'contract_types',
      'breeam', 'leed_v4_1', 'rics_valuation',
    ],
  },
  {
    id: 'geographic',
    label: 'Geographic',
    description: 'Country, subdivision, and regional classification systems',
    accent: '#64748B',
    bg: 'rgba(100,116,139,0.08)',
    systemIds: [
      'iso_3166_1', 'iso_3166_2', 'un_m49',
      'eu_nuts_2021', 'us_fips', 'wb_income', 'adb_sector',
      'nuts_candidate',
    ],
  },
  {
    id: 'health',
    label: 'Health / Clinical',
    description: 'Medical diagnoses, drugs, lab tests, devices, and clinical codes',
    accent: '#E11D48',
    bg: 'rgba(225,29,72,0.08)',
    systemIds: [
      'atc_who', 'icd_11', 'loinc',
      'icd10cm', 'icd10_pcs', 'icd10_am', 'icd10_gm', 'icd10_ca',
      'icdo3', 'icf', 'ms_drg', 'nucc_hcpt', 'hcpcs_l2', 'mesh',
      'snomed_ct', 'rxnorm', 'cpt_ama', 'dsm5', 'orphanet', 'omim',
      'g_drg', 'icpc2', 'ichi_who', 'hcpcs_l3', 'cdc_vaccine',
      'gbd_cause', 'gmdn', 'who_essential_med', 'ctcae',
      'fhir_resources', 'dicom_modality',
      'icn_nursing', 'nic_nursing_intv', 'nanda_nursing_dx', 'edqm_dosage',
      'hedis', 'cms_star',
    ],
  },
  {
    id: 'industry',
    label: 'Industry',
    description: 'National and international industry classification standards',
    accent: '#F59E0B',
    bg: 'rgba(245,158,11,0.08)',
    systemIds: [
      'naics_2022', 'isic_rev4', 'nace_rev2', 'sic_1987',
      'nic_2008', 'wz_2008', 'onace_2008', 'noga_2008',
      'anzsic_2006', 'jsic_2013',
      'naics_2012', 'naics_2017', 'isic_rev3',
      'ateco_2007', 'naf_rev2', 'pkd_2007', 'sbi_2008', 'sni_2007',
      'db07', 'tol_2008', 'cae_rev3', 'cz_nace', 'teaor_2008',
      'caen_rev2', 'nkd_2007', 'sk_nace', 'nkid', 'emtak',
      'nace_lt', 'nk_lv', 'nace_tr',
      'cnae_2009', 'nace_bel', 'nace_lu', 'nace_ie', 'stakod_08',
      'nace_cy', 'nace_mt', 'skd_2008', 'sn_2007', 'isat_2008',
      'kd_rs', 'nkd_mk', 'kd_ba', 'kd_me', 'nve_al',
      'kd_xk', 'caem_md', 'kved_ua', 'nace_ge', 'nace_am',
      'cnae_2012', 'ciiu_co', 'ciiu_ar', 'ciiu_cl', 'ciiu_pe', 'ciiu_ec',
      'caeb', 'ciiu_ve', 'ciiu_cr', 'ciiu_gt', 'ciiu_pa', 'scian_2018',
      'ciiu_do', 'ciiu_py', 'ciiu_uy',
      'csic_2017', 'gbt_4754', 'okved_2', 'kbli_2020', 'kbli_id', 'sic_sa',
      'ksic_2017', 'ssic_2020', 'msic_2008', 'tsic_2009', 'psic_2009',
      'vsic_2018', 'bsic', 'psic_pk', 'slsic',
      'isic_ng', 'isic_ke', 'isic_eg', 'isic_sa', 'isic_ae',
      'isic_af', 'isic_ag', 'isic_ao', 'isic_az', 'isic_bb', 'isic_bf',
      'isic_bh', 'isic_bi', 'isic_bj', 'isic_bn', 'isic_bs', 'isic_bt',
      'isic_bw', 'isic_bz', 'isic_cd', 'isic_cg', 'isic_ci', 'isic_cm',
      'isic_cu', 'isic_cv', 'isic_dj', 'isic_dm', 'isic_dz', 'isic_er',
      'isic_et', 'isic_fj', 'isic_ga', 'isic_gd', 'isic_gh', 'isic_gm',
      'isic_gn', 'isic_gq', 'isic_gw', 'isic_gy', 'isic_hn', 'isic_ht',
      'isic_il', 'isic_iq', 'isic_ir', 'isic_jm', 'isic_jo', 'isic_kg',
      'isic_kh', 'isic_km', 'isic_kn', 'isic_kw', 'isic_kz', 'isic_la',
      'isic_lb', 'isic_lc', 'isic_lr', 'isic_ls', 'isic_ly', 'isic_ma',
      'isic_mg', 'isic_ml', 'isic_mm', 'isic_mn', 'isic_mr', 'isic_mu',
      'isic_mv', 'isic_mw', 'isic_mz', 'isic_na', 'isic_ne', 'isic_ni',
      'isic_np', 'isic_om', 'isic_pg', 'isic_ps', 'isic_qa', 'isic_rw',
      'isic_sb', 'isic_sc', 'isic_sl', 'isic_sn', 'isic_so', 'isic_sr',
      'isic_ss', 'isic_sv', 'isic_sy', 'isic_sz', 'isic_td', 'isic_tg',
      'isic_tj', 'isic_tl', 'isic_tm', 'isic_tn', 'isic_to', 'isic_tt',
      'isic_tz', 'isic_ug', 'isic_uz', 'isic_vc', 'isic_vu', 'isic_ws',
      'isic_ye', 'isic_zm', 'isic_zw',
    ],
  },
  {
    id: 'international',
    label: 'International Treaties',
    description: 'Global conventions, treaties, and multilateral frameworks',
    accent: '#4338CA',
    bg: 'rgba(67,56,202,0.08)',
    systemIds: [
      'reg_basel3', 'reg_fatf', 'reg_ilo_core', 'reg_ungp', 'reg_oecd_mne',
      'reg_wto_sps', 'reg_wto_tbt', 'reg_codex', 'reg_who_fctc',
      'reg_uncitral', 'reg_unclos', 'reg_montreal', 'reg_paris',
      'reg_kimberley', 'reg_equator', 'reg_ifc_ps',
      'reg_icao_annex', 'reg_marpol', 'reg_solas', 'reg_berne',
    ],
  },
  {
    id: 'iso',
    label: 'ISO Standards',
    description: 'ISO management system standards for quality, safety, and sustainability',
    accent: '#0891B2',
    bg: 'rgba(8,145,178,0.08)',
    systemIds: [
      'reg_iso_9001', 'reg_iso_14001', 'reg_iso_27001', 'reg_iso_22000', 'reg_iso_45001',
      'reg_iso_50001', 'reg_iso_13485', 'reg_iso_22301', 'reg_iso_20000', 'reg_iso_26000',
      'reg_iso_37001', 'reg_iso_42001', 'reg_iso_28000', 'reg_iso_55001', 'reg_iso_41001',
      'reg_iso_30401', 'reg_iso_21001', 'reg_iso_39001', 'reg_iso_37101', 'reg_iso_14064',
      'reg_iso_14040', 'reg_iso_19011', 'reg_iso_31010', 'reg_iso_22313', 'reg_iso_27701',
    ],
  },
  {
    id: 'occupational',
    label: 'Occupational',
    description: 'Skills, roles, occupational frameworks, and professional standards',
    accent: '#7C3AED',
    bg: 'rgba(124,58,237,0.08)',
    systemIds: [
      'isco_08', 'soc_2018', 'anzsco_2022',
      'esco_occupations', 'esco_skills', 'onet_soc',
      'noc_2021', 'uksoc_2020', 'kldb_2010', 'rome_v4',
      'onet_abilities', 'onet_knowledge', 'onet_work_activities',
      'onet_work_context', 'onet_interests', 'onet_work_values', 'onet_work_styles',
      'sfia_v8', 'digcomp_22', 'ecf_v4', 'linkedin_skills',
      'worldskills', 'esco_qualifications', 'shrm_competency',
      'nqf_uk', 'eqf', 'aqf', 'job_family',
      'pmbok7', 'prince2', 'itil4', 'togaf_adm', 'archimate',
      'six_sigma', 'lean_tools',
    ],
  },
  {
    id: 'trade',
    label: 'Product / Trade',
    description: 'Harmonized trade codes and product classification hierarchies',
    accent: '#D97706',
    bg: 'rgba(217,119,6,0.08)',
    systemIds: [
      'hs_2022', 'cpc_v21', 'unspsc_v24',
      'sitc_rev4', 'bec_rev5', 'hts_us', 'schedule_b', 'eccn',
      'prodcom', 'cpv_2008', 'cn_2024',
      'eu_taric', 'uk_trade_tariff', 'asean_tariff', 'mercosur_tariff',
      'gcc_tariff', 'ecowas_cet', 'afcfta_tariff',
      'gs1_standards', 'edi_standards', 'scor_model', 'wco_safe',
    ],
  },
  {
    id: 'regulatory',
    label: 'Regulatory & Governance',
    description: 'Regulatory frameworks, compliance standards, and governance codes',
    accent: '#0369A1',
    bg: 'rgba(3,105,161,0.08)',
    systemIds: [
      'cfr_title_49', 'fmcsa_regs', 'gdpr_articles', 'iso_31000',
      'eu_taxonomy', 'sfdr', 'tnfd', 'gri_standards', 'sasb_sics',
      'sdg', 'seea', 'oecd_dac',
      'reg_hipaa', 'reg_sox', 'reg_glba', 'reg_ferpa', 'reg_coppa',
      'reg_fcra', 'reg_ada', 'reg_osha_1910', 'reg_osha_1926',
      'reg_nerc_cip', 'reg_fisma', 'reg_fedramp', 'reg_ccpa',
      'reg_cfpb', 'reg_sec', 'reg_finra', 'reg_far', 'reg_dfars',
      'reg_itar', 'reg_ear', 'reg_clean_air', 'reg_clean_water',
      'reg_cercla', 'reg_rcra', 'reg_tsca',
      'reg_pci_dss', 'reg_soc2', 'reg_hitrust', 'reg_cmmc',
      'reg_nist_csf', 'reg_nist_800_53', 'reg_nist_800_171',
      'reg_cis_controls', 'reg_cobit', 'reg_coso',
      'reg_ffiec', 'reg_ftc_safeguards', 'reg_naic',
      'reg_us_gaap', 'reg_fasb', 'reg_pcaob', 'reg_aicpa',
      'reg_joint_commission', 'reg_cap', 'reg_clia',
      'reg_fda_21cfr', 'reg_dea', 'reg_usp',
      'reg_ashrae', 'reg_asme',
      'reg_dora', 'reg_nis2', 'reg_eu_ai_act', 'reg_eprivacy',
      'reg_mifid2', 'reg_solvency2', 'reg_psd2',
      'reg_reach', 'reg_rohs', 'reg_mdr', 'reg_ivdr',
      'reg_eu_whistleblower', 'reg_csrd', 'reg_cbam', 'reg_weee',
      'reg_eu_packaging', 'reg_eu_batteries', 'reg_sfdr_detail',
      'reg_eu_deforestation', 'reg_dsa', 'reg_dma',
      'reg_eu_cra', 'reg_eu_data_act', 'reg_eu_machinery', 'reg_emas',
      'gdpr_basis', 'gdpr_rights', 'data_retention',
      'cfr_titles', 'usc_titles',
      'sbti', 'tcfd', 'issb_s1_s2',
      'haccp', 'allergen_list', 'codex_committees', 'wcag',
      'nfpa_codes', 'ibc_2021',
      'nato_codification', 'stanag', 'un_ammo', 'dod_mil_std',
      'owasp_top10', 'mitre_attack', 'cve_types',
      'isa_standards',
    ],
  },
  {
    id: 'research',
    label: 'Research & Knowledge',
    description: 'Academic disciplines, library classification, and knowledge organization',
    accent: '#0284C7',
    bg: 'rgba(2,132,199,0.08)',
    systemIds: [
      'ford_frascati', 'jel', 'lcc', 'pacs', 'msc_2020',
      'acm_ccs', 'arxiv_taxonomy', 'anzsrc_for_2020',
      'scopus_asjc', 'wos_categories', 'era_for', 'anzsrc_seo',
      'dewey_decimal', 'udc', 'lcsh', 'unesco_thesaurus', 'getty_aat',
      'aacsb', 'abet', 'skos', 'xbrl_taxonomy', 'iab_content',
    ],
  },
  {
    id: 'scales',
    label: 'Scales & Reference',
    description: 'Measurement scales, physical constants, and reference classification systems',
    accent: '#A1A1AA',
    bg: 'rgba(161,161,170,0.08)',
    systemIds: [
      'si_units', 'periodic_table', 'beaufort_scale',
      'richter_scale', 'mohs_hardness', 'saffir_simpson',
      'fujita_tornado', 'uv_index',
      'apgar_score', 'bristol_stool', 'pain_scale',
      'bmi_categories', 'asa_physical', 'blood_types',
      'pantone_families', 'ral_colors', 'emoji_categories',
      'isbn_groups', 'isrc_format',
      'olympic_sports', 'fifa_confederations',
    ],
  },
  {
    id: 'technology',
    label: 'Technology Standards',
    description: 'Networking, hardware, software, and digital infrastructure standards',
    accent: '#6366F1',
    bg: 'rgba(99,102,241,0.08)',
    systemIds: [
      'ietf_rfc', 'w3c_standards', 'ieee_standards',
      'usb_classes', 'bluetooth_profiles', 'pci_sig',
      'jedec', 'semi_standards', 'vesa_standards',
      '3gpp_specs', 'itu_t', 'itu_r_bands',
      'http_status', 'mime_types', 'spdx_licenses',
      'cloud_native', 'ai_model_type',
      'token_standard', 'defi_protocol',
    ],
  },
  {
    id: 'transport',
    label: 'Transport & Logistics',
    description: 'Aviation, maritime, rail, and freight classification codes',
    accent: '#8B5CF6',
    bg: 'rgba(139,92,246,0.08)',
    systemIds: [
      'iata_aircraft', 'imo_vessel', 'imo_ship_type',
      'uic_railway', 'icao_airport', 'icao_doc4444', 'faa_aircraft_cat',
      'nmfc', 'stcc', 'container_iso',
    ],
  },
  {
    id: 'domain',
    label: 'Domain Deep-Dives',
    description: 'Sector vocabularies for traditional industries and emerging technologies',
    accent: '#475569',
    bg: 'rgba(71,85,105,0.08)',
    systemIds: [],  // matched by domain_ prefix at runtime
  },
]

// ---------------------------------------------------------------------------
// Domain sectors - sub-categories within "Domain Deep-Dives"
// Used to drive sector sub-navigation in the dashboard and explore pages.
// ---------------------------------------------------------------------------

export const DOMAIN_SECTORS: DomainSector[] = [
  {
    id: 'advert', label: 'Advertising & Media', prefix: 'domain_advertising_',
    accent: '#EA580C', group: 'traditional',
  },
  {
    id: 'materials', label: 'Advanced Materials', prefix: 'domain_materials_',
    accent: '#F59E0B', group: 'emerging',
    extraIds: ['domain_adv_materials'],
  },
  {
    id: 'ag', label: 'Agriculture', prefix: 'domain_ag_',
    accent: '#22C55E', group: 'traditional',
    extraIds: [
      'domain_aquaponics', 'domain_greenhouse', 'domain_irrigation',
      'domain_seed_variety', 'domain_vertical_farm', 'domain_agritech',
      'domain_crop_protection', 'domain_organic_cert', 'domain_precision_ag',
    ],
  },
  {
    id: 'ai', label: 'AI & Data', prefix: 'domain_ai_',
    accent: '#3B82F6', group: 'emerging',
  },
  {
    id: 'arts', label: 'Arts & Entertainment', prefix: 'domain_arts_',
    accent: '#D946EF', group: 'traditional',
  },
  {
    id: 'automotive', label: 'Automotive & EV', prefix: 'domain_auto_',
    accent: '#DC2626', group: 'emerging',
    extraIds: ['domain_ev_charging', 'domain_fleet_mgmt'],
  },
  {
    id: 'biotech', label: 'Biotechnology', prefix: 'domain_biotech_',
    accent: '#10B981', group: 'emerging',
    extraIds: ['domain_biotech'],
  },
  {
    id: 'chemical', label: 'Chemical Industry', prefix: 'domain_chemical_',
    accent: '#F97316', group: 'emerging',
  },
  {
    id: 'climate', label: 'Climate Technology', prefix: 'domain_climate_',
    accent: '#22C55E', group: 'emerging',
  },
  {
    id: 'const', label: 'Construction', prefix: 'domain_const_',
    accent: '#F97316', group: 'traditional',
    extraIds: [
      'domain_accessibility', 'domain_brownfield', 'domain_building_auto',
      'domain_building_code', 'domain_commissioning', 'domain_construction_permit',
      'domain_electrical_code', 'domain_elevator', 'domain_facade',
      'domain_facility_bench', 'domain_fire_protection', 'domain_foundation_type',
      'domain_green_material', 'domain_hvac_system', 'domain_landscape',
      'domain_modular_const', 'domain_parking', 'domain_plumbing_code',
      'domain_prefab', 'domain_retro_cx', 'domain_roofing_type',
      'domain_signage', 'domain_smart_building', 'domain_structural',
    ],
  },
  {
    id: 'cyber', label: 'Cybersecurity', prefix: 'domain_cyber_',
    accent: '#EF4444', group: 'emerging',
    extraIds: [
      'domain_blue_team', 'domain_cert_authority', 'domain_encryption',
      'domain_hsm', 'domain_identity_gov', 'domain_incident_resp',
      'domain_key_mgmt', 'domain_pentest', 'domain_pki',
      'domain_purple_team', 'domain_red_team', 'domain_siem',
      'domain_soar', 'domain_threat_intel', 'domain_vuln_mgmt',
      'domain_zero_trust',
    ],
  },
  {
    id: 'defence', label: 'Defence & Security', prefix: 'domain_defence_',
    accent: '#334155', group: 'emerging',
  },
  {
    id: 'digital', label: 'Digital Assets', prefix: 'domain_digital_',
    accent: '#EAB308', group: 'emerging',
  },
  {
    id: 'education', label: 'Education', prefix: 'domain_education_',
    accent: '#059669', group: 'traditional',
    extraIds: [
      'domain_accreditation', 'domain_competency', 'domain_curriculum',
      'domain_digital_badge', 'domain_learning_outcome', 'domain_micro_cred',
      'domain_student_assess', 'domain_univ_ranking',
      'domain_edtech_platform', 'domain_childcare_early',
    ],
  },
  {
    id: 'energy', label: 'Energy', prefix: 'domain_energy_',
    accent: '#0D9488', group: 'emerging',
    extraIds: [
      'domain_ancillary', 'domain_biofuel', 'domain_capacity_mkt',
      'domain_carbon_offset', 'domain_cogeneration', 'domain_demand_resp',
      'domain_district_heat', 'domain_emission_factor', 'domain_geothermal',
      'domain_lng_terminal', 'domain_microgrid_type', 'domain_nat_gas',
      'domain_oil_grade', 'domain_petrochem', 'domain_pipeline',
      'domain_rec', 'domain_refinery', 'domain_tidal', 'domain_vpp',
      'domain_wave_energy', 'domain_battery_tech', 'domain_smart_grid',
      'domain_solar_energy', 'domain_wind_energy',
    ],
  },
  {
    id: 'finance', label: 'Finance', prefix: 'domain_finance_',
    accent: '#10B981', group: 'traditional',
    extraIds: [
      'domain_bond_rating', 'domain_commercial_lending',
      'domain_commodity_trading', 'domain_credit_rating', 'domain_derivatives',
      'domain_fintech_service', 'domain_forex', 'domain_hedge_fund',
      'domain_microfinance', 'domain_mortgage_type', 'domain_muni_bond',
      'domain_payment_proc', 'domain_pe_stage', 'domain_reit_type',
      'domain_securitization', 'domain_trade_finance', 'domain_wealth_mgmt',
    ],
  },
  {
    id: 'food', label: 'Food Service', prefix: 'domain_food_',
    accent: '#EF4444', group: 'traditional',
  },
  {
    id: 'forestry', label: 'Forestry & Fishing', prefix: 'domain_forestry_',
    accent: '#15803D', group: 'traditional',
    extraIds: ['domain_fishing_aqua'],
  },
  {
    id: 'gaming', label: 'Gaming & Sports', prefix: 'domain_gaming_',
    accent: '#E11D48', group: 'emerging',
    extraIds: ['domain_sports_recreation', 'domain_pet_animal'],
  },
  {
    id: 'health', label: 'Healthcare', prefix: 'domain_health_',
    accent: '#F43F5E', group: 'traditional',
    extraIds: [
      'domain_adverse_event', 'domain_allied_health', 'domain_anesthesia',
      'domain_biobank', 'domain_biomarker', 'domain_biosimilar',
      'domain_bundled_pay', 'domain_capitation', 'domain_cds',
      'domain_cell_therapy', 'domain_cleanroom', 'domain_clinical_endpoint',
      'domain_clinical_reg', 'domain_companion_dx', 'domain_drug_interaction',
      'domain_formulary', 'domain_gene_therapy', 'domain_global_budget',
      'domain_hospital_dept', 'domain_imaging', 'domain_implant',
      'domain_infection_ctrl', 'domain_lab_test', 'domain_med_gas',
      'domain_nursing_spec', 'domain_orphan_drug', 'domain_orthotic',
      'domain_pathology_sub', 'domain_pharma_practice', 'domain_pop_health',
      'domain_prosthetic', 'domain_radiopharm', 'domain_remote_monitor',
      'domain_sdoh', 'domain_sterilization', 'domain_surgical_inst',
      'domain_surgical_spec', 'domain_telemedicine', 'domain_vbc_model',
      'domain_wound_care', 'domain_clinical_trial', 'domain_dental',
      'domain_mental_health', 'domain_medical_device', 'domain_pharma_drug_class',
      'domain_veterinary', 'domain_healthtech',
    ],
  },
  {
    id: 'hr', label: 'HR & Labor', prefix: 'domain_hr_',
    accent: '#0EA5E9', group: 'traditional',
    extraIds: [
      'domain_diversity_metric', 'domain_eeo_category', 'domain_freelance_plat',
      'domain_internship', 'domain_labor_union',
      'domain_hr_tech', 'domain_talent_market',
    ],
  },
  {
    id: 'info', label: 'Information & Media', prefix: 'domain_info_',
    accent: '#3B82F6', group: 'traditional',
  },
  {
    id: 'insurance', label: 'Insurance', prefix: 'domain_insurance_',
    accent: '#059669', group: 'traditional',
    extraIds: [
      'domain_actuarial_method', 'domain_reinsurance', 'domain_insurtech',
    ],
  },
  {
    id: 'it', label: 'IT & Software', prefix: 'domain_sw_',
    accent: '#6366F1', group: 'emerging',
    extraIds: [
      'domain_api_architecture', 'domain_backup', 'domain_cicd_pipeline',
      'domain_container_orch', 'domain_data_catalog', 'domain_data_governance',
      'domain_data_lakehouse', 'domain_data_lineage', 'domain_data_mesh',
      'domain_data_quality', 'domain_database_type', 'domain_dr',
      'domain_event_arch', 'domain_feature_store', 'domain_master_data',
      'domain_microservices', 'domain_mlops', 'domain_model_registry',
      'domain_oss_governance', 'domain_prog_paradigm', 'domain_ref_data',
      'domain_serverless', 'domain_sw_license', 'domain_synthetic_data',
      'domain_version_control', 'domain_cloud_service', 'domain_datacenter_cloud',
      'domain_devops', 'domain_edge_computing',
      'domain_saas_category', 'domain_regtech',
    ],
  },
  {
    id: 'legal', label: 'Legal', prefix: 'domain_legal_',
    accent: '#7C3AED', group: 'traditional',
    extraIds: [
      'domain_adr', 'domain_antitrust', 'domain_arb_type',
      'domain_class_action', 'domain_consumer_prot', 'domain_copyright',
      'domain_corrections', 'domain_court_type', 'domain_export_ctrl',
      'domain_law_enforce', 'domain_notary', 'domain_patent_type',
      'domain_product_liab', 'domain_sanctions', 'domain_trade_secret',
      'domain_trademark', 'domain_workplace_med',
      'domain_legal_practice', 'domain_legaltech',
    ],
  },
  {
    id: 'mfg', label: 'Manufacturing', prefix: 'domain_mfg_',
    accent: '#F59E0B', group: 'traditional',
    extraIds: ['domain_textile_fashion'],
  },
  {
    id: 'mining', label: 'Mining', prefix: 'domain_mining_',
    accent: '#A1A1AA', group: 'traditional',
  },
  {
    id: 'nuclear', label: 'Nuclear & Hydrogen', prefix: 'domain_nuclear_',
    accent: '#7C3AED', group: 'emerging',
    extraIds: ['domain_hydrogen_economy'],
  },
  {
    id: 'other', label: 'Other Services', prefix: 'domain_other_',
    accent: '#64748B', group: 'traditional',
    extraIds: [
      'domain_pet', 'domain_senior_care', 'domain_sharing_econ',
    ],
  },
  {
    id: 'prof', label: 'Professional Services', prefix: 'domain_prof_',
    accent: '#14B8A6', group: 'traditional',
    extraIds: [
      'domain_pro', 'domain_coworking', 'domain_franchise', 'domain_subscription',
    ],
  },
  {
    id: 'public', label: 'Public Administration', prefix: 'domain_public_',
    accent: '#475569', group: 'traditional',
    extraIds: [
      'domain_emergency_svc', 'domain_gov_contract',
      'domain_grant_type', 'domain_municipal_svc', 'domain_nonprofit_social',
    ],
  },
  {
    id: 'quantum', label: 'Quantum Computing', prefix: 'domain_quantum_',
    accent: '#7C3AED', group: 'emerging',
    extraIds: ['domain_quantum'],
  },
  {
    id: 'realestate', label: 'Real Estate', prefix: 'domain_realestate_',
    accent: '#0EA5E9', group: 'traditional',
    extraIds: [
      'domain_property_val', 'domain_zoning',
      'domain_lease_abstract', 'domain_proptech',
    ],
  },
  {
    id: 'retail', label: 'Retail', prefix: 'domain_retail_',
    accent: '#EC4899', group: 'traditional',
    extraIds: ['domain_ecommerce_platform'],
  },
  {
    id: 'robotics', label: 'Robotics & Autonomy', prefix: 'domain_robotics_',
    accent: '#94A3B8', group: 'emerging',
    extraIds: ['domain_robotics'],
  },
  {
    id: 'semiconductor', label: 'Semiconductors', prefix: 'domain_semiconductor_',
    accent: '#EF4444', group: 'emerging',
    extraIds: ['domain_semiconductor'],
  },
  {
    id: 'space', label: 'Space & Satellite', prefix: 'domain_space_',
    accent: '#4F46E5', group: 'emerging',
    extraIds: ['domain_space'],
  },
  {
    id: 'supply', label: 'Supply Chain', prefix: 'domain_supply_',
    accent: '#7C3AED', group: 'traditional',
    extraIds: [
      'domain_cold_chain', 'domain_cross_dock', 'domain_customs_class',
      'domain_customs_proc', 'domain_freight_class', 'domain_ftz',
      'domain_incoterm_detail', 'domain_warehouse',
      'domain_last_mile', 'domain_maritime_shipping',
    ],
  },
  {
    id: 'sustainability', label: 'Sustainability & ESG', prefix: 'domain_sustain_',
    accent: '#16A34A', group: 'emerging',
    extraIds: [
      'domain_carbon_credit', 'domain_circular_econ', 'domain_cleantech',
    ],
  },
  {
    id: 'synbio', label: 'Synthetic Biology', prefix: 'domain_synbio_',
    accent: '#06B6D4', group: 'emerging',
    extraIds: ['domain_synbio'],
  },
  {
    id: 'telecom', label: 'Telecom', prefix: 'domain_telecom_',
    accent: '#0284C7', group: 'emerging',
    extraIds: ['domain_iot_device'],
  },
  {
    id: 'tourism', label: 'Tourism & Hospitality', prefix: 'domain_tourism_',
    accent: '#F97316', group: 'traditional',
    extraIds: ['domain_event_mgmt', 'domain_wine_spirits'],
  },
  {
    id: 'transport', label: 'Transportation', prefix: 'domain_transport_',
    accent: '#8B5CF6', group: 'traditional',
    extraIds: [
      'domain_aviation_service', 'domain_rail_service',
    ],
  },
  {
    id: 'truck', label: 'Trucking', prefix: 'domain_truck_',
    accent: '#6366F1', group: 'traditional',
  },
  {
    id: 'util', label: 'Utilities', prefix: 'domain_util_',
    accent: '#EAB308', group: 'traditional',
  },
  {
    id: 'water', label: 'Water & Environment', prefix: 'domain_water_',
    accent: '#0EA5E9', group: 'emerging',
    extraIds: [
      'domain_air_quality', 'domain_biodiv_offset', 'domain_coral_reef',
      'domain_invasive_sp', 'domain_light_pollution', 'domain_mangrove',
      'domain_noise_pollution', 'domain_soil_contam', 'domain_water_quality',
      'domain_wetland', 'domain_env_remediation',
      'domain_soil_mgmt', 'domain_waste_mgmt',
    ],
  },
  {
    id: 'wholesale', label: 'Wholesale Trade', prefix: 'domain_wholesale_',
    accent: '#D97706', group: 'traditional',
  },
  {
    id: 'workforce', label: 'Workforce Safety', prefix: 'domain_workforce_',
    accent: '#DC2626', group: 'traditional',
    extraIds: [
      'domain_apprentice', 'domain_coll_bargain', 'domain_comp_structure',
      'domain_employee_benefit', 'domain_gig_worker',
    ],
  },
  {
    id: 'xr', label: 'XR & Metaverse', prefix: 'domain_xr_',
    accent: '#EC4899', group: 'emerging',
  },
]

/**
 * Returns the domain sector for a domain_* system ID, or null if not found.
 * Checks explicit extraIds first, then falls back to prefix matching.
 */
export function getDomainSector(systemId: string): DomainSector | null {
  const explicit = DOMAIN_SECTORS.find((s) => s.extraIds?.includes(systemId))
  if (explicit) return explicit
  return DOMAIN_SECTORS.find((s) => systemId.startsWith(s.prefix)) ?? null
}

export function getCategoryForSystem(systemId: string): SystemCategory {
  if (systemId.startsWith('domain_')) {
    return SYSTEM_CATEGORIES.find((c) => c.id === 'domain')!
  }
  // Explicit lookup - if not found, fall back to the domain catch-all
  // (keeps behaviour safe for any future systems not yet categorized)
  return (
    SYSTEM_CATEGORIES.find((c) => c.systemIds.includes(systemId)) ??
    SYSTEM_CATEGORIES.find((c) => c.id === 'domain')!
  )
}

export function groupSystemsByCategory(
  systems: ClassificationSystem[]
): Array<{ category: SystemCategory; systems: ClassificationSystem[] }> {
  return SYSTEM_CATEGORIES.map((cat) => ({
    category: cat,
    systems: systems.filter((s) =>
      cat.id === 'domain'
        ? s.id.startsWith('domain_')
        : cat.systemIds.includes(s.id)
    ),
  })).filter((g) => g.systems.length > 0)
}
