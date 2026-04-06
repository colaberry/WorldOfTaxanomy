# CLAUDE.md — WorldOfTaxanomy

## Project
WorldOfTaxanomy is a unified global industry classification graph. It federates ISIC, NAICS, NACE, ANZSIC, JSIC, and other national classification systems as equal peers, connecting each industry node to its domain-specific taxonomies (ICD codes, drug classifications, crop taxonomies, financial instrument codes, etc.).

Surfaces: Web app + REST API + MCP server.

## Design System
Always read DESIGN.md before making any visual or UI decisions.
All font choices, colors, spacing, and aesthetic direction are defined there.
Do not deviate without explicit user approval.
In QA mode, flag any code that doesn't match DESIGN.md.

Key design principles:
- Observatory aesthetic: deep dark canvas, glowing sector-colored nodes
- Instrument Serif for display, Instrument Sans for body, Geist Mono for data
- Federation model: every classification system is a first-class peer
- MCP-first: show machine interfaces inline, not hidden behind docs
- Typography encodes hierarchy depth (serif → sans → mono as you go deeper)
