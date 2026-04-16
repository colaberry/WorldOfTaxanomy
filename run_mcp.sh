#!/bin/bash
# MCP server launcher for Claude Desktop
# Sets PYTHONPATH so world_of_taxonomy is importable regardless of cwd

export PYTHONPATH="/Users/ramkotamaraja/Documents/ai/projects/WorldOfTaxonomy"
export DATABASE_URL="postgresql://neondb_owner:npg_cfUw4eq0lErs@ep-red-frog-ajwa4v49.c-3.us-east-2.aws.neon.tech/neondb?sslmode=require"

exec /usr/bin/python3 -m world_of_taxonomy mcp
