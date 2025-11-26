# Ingestion Mapping – Graph Path

This MVP supports an optional graph ingestion path into Neo4j for role adjacency and competency requirements.

## Entities

- Role (label: `Role`)
- Competency (label: `Competency`)

## Relationships

- `(:Role)-[:REQUIRES {requiredLevel, version, source, validFrom, validTo}]->(:Competency)`
- `(:Role)-[:ADJACENT_TO {score, rationale, version, source}]->(:Role)` (stored bidirectionally)

## ETL Sources

Preferred file formats:
- CSV: `data/roles.csv`, `data/competencies.csv`, `data/requires.csv`, `data/adjacency.csv`
- Excel: `data/roles.xlsx`, `data/competencies.xlsx`, `data/requires.xlsx`, `data/adjacency.xlsx`
- JSON seeds: `seed/roles.json`, `seed/competencies.json`, `seed/requires.json`, `seed/adjacency.json`

The backend ETL accepts CSV and Excel directly. For MCP flows you may optionally convert Excel inputs to JSON seeds first using:

```
python -m src.etl.xlsx_to_json roles-from-abbrev <Competency_mapping.xlsx> --output-dir data/processed
python -m src.etl.xlsx_to_json competencies-from-definitions <Competency_mapping.xlsx> --output-dir data/processed
python -m src.etl.xlsx_to_json requires-from-matrix <Competency_mapping.xlsx> --output-dir data/processed
python -m src.etl.xlsx_to_json adjacency-from-matrix <CA_Role_Adjacency.xlsx> --mapping-xlsx <Competency_mapping.xlsx> --output-dir data/processed
```

Then ingest via:

```
python -m src.etl.ingest_graph seed data/processed
```

See `README_backend.md` for complete CLI usage and expected columns.
