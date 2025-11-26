# Career Platform Backend – Neo4j Graph Integration

This backend integrates an optional Neo4j graph to power role adjacency queries. The feature is guarded by a flag so the service runs without Neo4j by default.

## Configuration

Copy `.env.example` to `.env` and adjust values:

```
# Either flag enables graph integration (USE_GRAPH is an alias)
FEATURE_GRAPH_ENABLED=false
# or
USE_GRAPH=false

NEO4J_URI=bolt://neo4j:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=please-change-me

CORS_ALLOW_ORIGINS=*
```

- When `FEATURE_GRAPH_ENABLED=false` (or `USE_GRAPH=false`), the API starts with no graph dependency and will use in-memory fallbacks for certain endpoints.
- When `FEATURE_GRAPH_ENABLED=true` (or `USE_GRAPH=true`), the service tries to connect to Neo4j using `NEO4J_*` variables.

## Endpoints

- `GET /` – Health check with graph status (disabled/healthy/misconfigured/unhealthy).
- `GET /role-adjacency` – Returns alternative role suggestions.
  - Uses Neo4j when `FEATURE_GRAPH_ENABLED=true` (or `USE_GRAPH=true`).
  - Returns an empty list fallback when disabled.

### NEW: Roles Management

- `POST /roles` – Create or upsert a Role (201).
  - When graph is enabled/healthy, persists to Neo4j.
  - Otherwise, uses an in-memory fallback DAO and logs a warning.
- `GET /roles` – List roles (200).
  - From Neo4j when enabled; from in-memory fallback otherwise.
  - Optional query param: `limit` (default 500, max 5000).
- `GET /roles/{id}` – Get a single role by id (200 or 404).

Role model:
```
{
  "id": "string",
  "name": "string | null",
  "description": "string | null",
  "metadata": { "key": "value", ... } | null,
  "source": "string | null",
  "version": "string | null"
}
```

Example curl:
```
# Create/Upsert a role
curl -s -X POST http://localhost:3001/roles \
  -H "Content-Type: application/json" \
  -d '{"id":"CA","name":"Chief Architect","description":"Architecture leader"}'

# List roles
curl -s "http://localhost:3001/roles?limit=100"

# Get role by id
curl -s http://localhost:3001/roles/CA
```

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

### Excel support and optional JSON converter

- The ETL already accepts both CSV and Excel files directly (read via pandas/openpyxl).
- For MCP and traceability workflows, you can optionally convert Excel sources into JSON seed files first:
  - Converter CLI: `python -m src.etl.xlsx_to_json ...`
  - Output directory (default): `data/processed/` (roles.json, competencies.json, requires.json, adjacency.json)

Examples using provided spreadsheets:

```
# 1) Roles from abbreviations (Competency_mapping.xlsx / 'Role abbreviations' sheet)
python -m src.etl.xlsx_to_json roles-from-abbrev attachments/20251126_165919_Competency_mapping.xlsx --output-dir data/processed

# 2) Competencies from definitions (Competency_mapping.xlsx / 'Competency Definitions' sheet)
python -m src.etl.xlsx_to_json competencies-from-definitions attachments/20251126_165919_Competency_mapping.xlsx --output-dir data/processed

# 3) Role->Competency requirements from matrix (Competency_mapping.xlsx / 'Competencies and roles')
python -m src.etl.xlsx_to_json requires-from-matrix attachments/20251126_165919_Competency_mapping.xlsx --output-dir data/processed

# 4) Role adjacency from square matrix (CA_Role_Adjacency.xlsx)
#    Optionally map long role names to abbreviations using the same Competency_mapping workbook.
python -m src.etl.xlsx_to_json adjacency-from-matrix attachments/20251126_165917_CA_Role_Adjacency.xlsx --mapping-xlsx attachments/20251126_165919_Competency_mapping.xlsx --output-dir data/processed

# 5) Ingest JSON seeds into Neo4j
python -m src.etl.ingest_graph seed data/processed
```

Notes:
- Column names are matched case-insensitively and tolerate minor variations/underscores/dashes.
- The adjacency matrix converter deduplicates bidirectional pairs and ignores self-edges.

### Expected columns

- Roles: `id`, `name`, (`description` optional), (`source` optional), (`version` optional)
- Competencies: `id`, `name`, (`definition` optional), (`source` optional), (`version` optional)
- REQUIRES: `roleId`, `competencyId`, (`requiredLevel` optional), (`version` optional), (`source` optional), (`validFrom` optional), (`validTo` optional)
- ADJACENT_TO: `roleA`, `roleB`, (`score` optional), (`rationale` optional), (`version` optional), (`source` optional)

Column names are matched case-insensitively; minor variations like underscores/dashes are tolerated.

## Notes

- The DAL ensures idempotent upserts for nodes and relationships.
- ADJACENT_TO is created in both directions by default.
- Health check returns `"graph": "disabled"|"healthy"|"misconfigured"|"unhealthy"` for quick diagnostics.

## Troubleshooting

- Graph status meanings:
  - `disabled`: Graph feature flag is OFF (`FEATURE_GRAPH_ENABLED=false` or `USE_GRAPH=false`).
  - `misconfigured`: Graph feature flag is ON but one or more `NEO4J_*` variables are missing. The service runs but will not use Neo4j until fixed.
  - `unhealthy`: Graph is configured but the API cannot connect (Neo4j down/unreachable) or authentication failed.
  - `healthy`: Graph is configured and responds to a basic query.

- Checklist when the graph is not healthy:
  1) Ensure the feature flag is set: `FEATURE_GRAPH_ENABLED=true` or `USE_GRAPH=true`.
  2) Provide all required env vars: `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`.
  3) Verify the database is reachable from the backend container (e.g., bolt port 7687).
  4) Validate credentials independently if possible (e.g., using neo4j Browser or cypher-shell).
  5) Check backend logs; when misconfigured, logs include the missing variable names.

- ETL:
  - If ETL fails with connection errors, ensure the same environment is set when running the CLI:
    `FEATURE_GRAPH_ENABLED=true` (or `USE_GRAPH=true`) and `NEO4J_*` variables are present.
