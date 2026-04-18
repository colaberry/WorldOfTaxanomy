/**
 * Systems we pre-render sector-level code pages for.
 *
 * These are the major industry, occupational, and product classification
 * systems where a 2-digit / top-level node page is commercially meaningful.
 * Keep this list small and intentional: each system here produces ~10-25
 * static pages under /codes/[system]/[code].
 */
export const MAJOR_SYSTEMS: readonly string[] = [
  // Industry (by region)
  'naics_2022',   // North America
  'isic_rev4',    // Global (UN)
  'sic_1987',     // US / UK legacy
  'nace_rev2',    // European Union
  'anzsic_2006',  // Australia / New Zealand
  'nic_2008',     // India

  // Occupational
  'soc_2018',     // United States
  'isco_08',      // Global (ILO)

  // Product / Trade
  'hs_2022',      // Global (WCO)
  'cpc_v21',      // Global (UN)
] as const

export const MAJOR_SYSTEM_SET = new Set<string>(MAJOR_SYSTEMS)

export function isMajorSystem(id: string): boolean {
  return MAJOR_SYSTEM_SET.has(id)
}
