# Career Platform Backend – Neo4j Graph Integration

This backend integrates an optional Neo4j graph to power role adjacency queries. The feature is guarded by a flag so the service runs without Neo4j by default.

## Configuration

Copy `.env.example` to `.env` and adjust values:

```
FEATURE_GRAPH_ENABLED=false

NEO4J_URI=bolt://neo4j:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=please-change-me

CORS_ALLOW_ORIGINS=*
```

- When `FEATURE_GRAPH_ENABLED=false`, the API starts with no graph dependency.
- When `FEATURE_GRAPH_ENABLED=true`, the service tries to connect to Neo4j using `NEO4J_*` variables.

## Endpoints

- `GET /` – Health check with graph status (disabled/healthy/unhealthy).
- `GET /role-adjacency` – Returns alternative role suggestions.
  - Uses Neo4j when `FEATURE_GRAPH_ENABLED=true`.
  - Returns an empty list fallback when disabled.

Query parameters:
- `userId` (optional)
- `currentRoleId` (optional)
- `targetRoleId` (optional)
- `limit` (default 20)

Response shape remains unchanged (array of Role objects).

## ETL Utilities

A CLI is provided to ingest Roles, Competencies, REQUIRES, and ADJACENT_TO data into Neo4j.

Run commands from the backend root:

```
# Roles (CSV/Excel)
python -m src.etl.ingest_graph roles data/roles.csv

# Competencies (CSV/Excel)
python -m src.etl.ingest_graph competencies data/competencies.xlsx

# REQUIRES (CSV/Excel)
python -m src.etl.ingest_graph requires data/requires.csv

# ADJACENT_TO (CSV/Excel)
python -m src.etl.ingest_graph adjacency data/adjacency.xlsx --bidirectional

# Seed from JSON files (roles.json, competencies.json, requires.json, adjacency.json)
python -m src.etl.ingest_graph seed seed/

# Attempt to ingest everything from a directory
python -m src.etl.ingest_graph all data/
```

### Expected columns

- Roles: `id`, `name`, (`description` optional), (`source` optional), (`version` optional)
- Competencies: `id`, `name`, (`definition` optional), (`source` optional), (`version` optional)
- REQUIRES: `roleId`, `competencyId`, (`requiredLevel` optional), (`version` optional), (`source` optional), (`validFrom` optional), (`validTo` optional)
- ADJACENT_TO: `roleA`, `roleB`, (`score` optional), (`rationale` optional), (`version` optional), (`source` optional)

Column names are matched case-insensitively; minor variations like underscores/dashes are tolerated.

## Notes

- The DAL ensures idempotent upserts for nodes and relationships.
- ADJACENT_TO is created in both directions by default.
- Health check returns `"graph": "disabled"|"healthy"|"unhealthy"` for quick diagnostics.

## Troubleshooting

- If the service starts with `"graph": "unhealthy"`, verify that Neo4j is running and credentials are correct.
- If ETL fails with connection errors, ensure `FEATURE_GRAPH_ENABLED=true` and `NEO4J_*` variables are set in your environment when running the CLI.
