# Portfolio Auth Architecture

This doc lives in the WorldOfTaxonomy repo because WoT is the first
product to need the integration, but the design is portfolio-wide. It
applies to every product under `aiaccelerator.ai`:

- WorldOfTaxonomy (`worldoftaxonomy.com`)
- WorldOfOntology (planned)
- WorldOfUseCases (planned)
- WorldOfAgents (planned)
- anything else the portfolio adds later

## Goal

One identity across the portfolio. A user signs in once at
`auth.aiaccelerator.ai`, lands back on any World-Of site already logged
in, sees one account page, has one billing relationship, and uses one
set of API keys that work across every product with per-product
entitlements.

## Decision: Zitadel Cloud

The central Identity Provider is **Zitadel Cloud**, hosted at
`auth.aiaccelerator.ai`. The decision criteria and the alternatives
considered are captured in [memory](../../../.claude/...). Short version:

| Requirement | Why Zitadel Cloud |
|-------------|-------------------|
| One billing across products | Native org/user hierarchy; Stripe customer = Zitadel org |
| Enterprise SSO (SAML + SCIM) within 12 months | SAML and SCIM on every tier, no per-connection upsell |
| Outsource security | Fully managed; never touch the infrastructure |
| Long-lived API keys for paid tiers | Native Personal Access Tokens, not M2M grants |
| OIDC for all products | Standard OIDC + JWKS; any product in any language integrates |

Rejected alternatives: Auth0 (API-key fit + MAU pricing), Keycloak /
self-hosted Zitadel (ops burden), WorkOS (per-SAML-connection pricing,
weak API-key story), Clerk (SAML is a paid add-on), rolling our own
(permanent maintenance burden).

## Architecture

```
                          ┌─────────────────────────────────┐
                          │  auth.aiaccelerator.ai          │
                          │  (Zitadel Cloud)                │
                          │                                 │
                          │  - Login, MFA, social, SSO      │
                          │  - Orgs (= Stripe customers)    │
                          │  - Users, roles, PATs           │
                          │  - SAML + SCIM for enterprise   │
                          └──────────┬──────────────────────┘
                                     │  OIDC
                                     │  JWKS, introspection
        ┌────────────────────────────┼────────────────────────────┐
        │                            │                            │
        ▼                            ▼                            ▼
┌───────────────────┐    ┌───────────────────┐        ┌───────────────────┐
│ worldoftaxonomy   │    │ worldofontology   │        │ worldofagents     │
│                   │    │                   │        │                   │
│ - Web (Next.js)   │    │ - Web             │        │ - Web             │
│ - REST API        │    │ - REST API        │        │ - REST API        │
│ - MCP server      │    │ - MCP server      │        │ - MCP server      │
│                   │    │                   │        │                   │
│ Verifies tokens   │    │ Verifies tokens   │        │ Verifies tokens   │
│ via JWKS          │    │ via JWKS          │        │ via JWKS          │
└─────────┬─────────┘    └─────────┬─────────┘        └─────────┬─────────┘
          │                        │                            │
          │                        ▼                            │
          │              ┌───────────────────┐                  │
          └─────────────▶│ Stripe            │◀─────────────────┘
                         │ customer = Zitadel│
                         │ org; products =   │
                         │ SKUs on subscr.   │
                         └───────────────────┘
```

Every product:

- Redirects unauthenticated web users to `auth.aiaccelerator.ai/login`.
- On callback, verifies the Zitadel-issued ID token, then mints (or
  accepts) its own session.
- Verifies REST/MCP bearer tokens against Zitadel's JWKS.
- Accepts Zitadel Personal Access Tokens (long-lived, user-scoped) as
  "API keys" for paid tiers. Product-specific rate-limit tiers come from
  a claim on the token, looked up from Stripe entitlements.

## What this changes for WorldOfTaxonomy

WoT currently implements auth itself: bcrypt passwords, HS256 JWT,
three OAuth providers, a `wot_` API-key table. The central-IdP
migration replaces the first three and re-homes the fourth:

| Today (WoT-local) | After migration (Zitadel-backed) |
|-------------------|-----------------------------------|
| `POST /auth/register` + bcrypt | Deleted. Users register at `auth.aiaccelerator.ai`. |
| `POST /auth/login` + HS256 JWT | Deleted. Login happens at Zitadel; backend verifies RS256 JWT via JWKS. |
| `/auth/oauth/{github,google,linkedin}` | Deleted. Social providers are configured once inside Zitadel. |
| `JWT_SECRET` env var | Deleted. Replaced by `ZITADEL_ISSUER` + JWKS URL. |
| `app_user` table | Keep, but add `zitadel_sub` (TEXT, unique). New rows created on first login keyed by `sub`. Existing rows get linked by email on next login. |
| `api_key` table with `wot_...` prefix | Two options: (A) keep the table, continue minting `wot_...` keys, but scope them to a `zitadel_sub` instead of a local user ID. (B) move entirely to Zitadel Personal Access Tokens. **Recommendation: (A)** - it preserves the `wot_...` ergonomics and doesn't force every skills bundle to re-plumb. |
| Rate-limit tier lookup | Unchanged mechanism; `tier` now comes from a claim on the Zitadel token or from the local `app_user.tier` column that Stripe webhooks update. |

## Migration path for WoT

1. **Provision.** Create a Zitadel Cloud instance, point
   `auth.aiaccelerator.ai` at it, register WoT as an OIDC application
   with callback `https://worldoftaxonomy.com/auth/callback`. Import
   GitHub / Google / LinkedIn social providers inside Zitadel (removes
   the per-product provider admin-console work forever).
2. **Backend token verification.** Add a `world_of_taxonomy/auth/zitadel.py`
   module. Rewrite `get_current_user` in `world_of_taxonomy/api/deps.py`
   to verify via the Zitadel JWKS URL (cached, with a JWKSClient).
   Drop `JWT_SECRET`. Keep the existing `DISABLE_AUTH=true` dev bypass.
3. **Schema.** `ALTER TABLE app_user ADD COLUMN zitadel_sub TEXT UNIQUE`.
   Migration script backfills NULL; next-login flow links by email.
   Drop `oauth_provider`, `oauth_provider_id`, and the password columns
   after all users have signed in once (a deprecation window, not an
   immediate drop).
4. **Frontend.** Replace `/login` and `/register` pages with a redirect
   to Zitadel's hosted login. Account page (`/account`) becomes a thin
   shell that iframes or deep-links Zitadel's user profile.
5. **API keys.** Keep minting `wot_...` keys from the dashboard, but
   store the Zitadel `sub` alongside. Nothing changes for the customer.
6. **Stripe wiring.** Stripe customer metadata carries the Zitadel org
   ID. Subscription webhooks update a `tier` column on the org; the
   rate-limit middleware reads from there instead of from the user
   record directly.
7. **Cut over.** Behind a feature flag (`AUTH_MODE=zitadel`), route
   new logins to Zitadel. Once green in staging, flip the flag in prod
   and delete the legacy routes in a follow-up release.

Expect three to five focused days for WoT. Every subsequent World-Of
product is one day because the pattern is already worn in.

## What this does *not* cover

- Which Zitadel Cloud plan to pick. Start on the free tier; the paid
  plan becomes relevant when MAU or SAML requirements cross the
  threshold.
- Billing implementation. Stripe wiring is a separate doc; this one
  stops at "org = Stripe customer."
- Secrets management for the Zitadel machine-user credentials (the
  client secret each product uses to introspect tokens). Default to
  GCP Secret Manager referenced from Cloud Run env vars, consistent
  with the rest of the deploy stack.
- Audit logging at the portfolio level. Zitadel writes its own; each
  product still writes its own access log. A unified log view can
  come later if needed.

## Open questions Ram still has to decide

1. **Domain migration.** `wot.aixcelerator.app` is the current API host.
   If the portfolio standardizes on `aiaccelerator.ai`, WoT's API should
   probably move to `api.worldoftaxonomy.com` or `wot.aiaccelerator.ai`
   for consistency. That is a CORS + OAuth-callback + docs update, not
   a rebuild.
2. **When to migrate.** Before the first paying customer (cheap) or
   after launch (harder, but unblocks launch). The current WoT auth
   works; this is not launch-blocking.
