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
- Health check returns `"graph": "disabled"|"healthy"|"unhealthy"` for quick diagnostics.

## Troubleshooting

- If the service starts with `"graph": "unhealthy"`, verify that Neo4j is running and credentials are correct.
- If ETL fails with connection errors, ensure `FEATURE_GRAPH_ENABLED=true` and `NEO4J_*` variables are set in your environment when running the CLI.
