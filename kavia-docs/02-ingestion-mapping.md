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

See `README_backend.md` for CLI usage and expected columns.
