import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import typer

from src.core.config import get_settings
from src.graph.graph_client import GraphClient
from src.graph.graph_dal import GraphDAL

app = typer.Typer(add_completion=False, help="Graph ETL utilities for Neo4j (roles, competencies, requires, adjacency).")
logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


def _ensure_graph() -> GraphDAL:
    settings = get_settings()
    client = GraphClient(settings)
    client.start()
    if not client.enabled or client.driver is None:
        raise typer.Exit(code=2)
    return GraphDAL(client.driver)


def _read_table(path: Path) -> pd.DataFrame:
    p = str(path)
    if p.lower().endswith(".csv"):
        return pd.read_csv(p)
    if p.lower().endswith(".xlsx") or p.lower().endswith(".xls"):
        # openpyxl is the default engine for .xlsx
        return pd.read_excel(p)
    raise ValueError(f"Unsupported file type for {path}")


def _norm_col(df: pd.DataFrame, col: str) -> Optional[str]:
    cols = {c.strip().lower(): c for c in df.columns}
    candidates = [
        col.lower(),
        col.replace("_", "").lower(),
        col.replace("-", "").lower(),
    ]
    for cand in candidates:
        if cand in cols:
            return cols[cand]
    return None


def _require_columns(df: pd.DataFrame, required: List[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for col in required:
        actual = _norm_col(df, col)
        if not actual:
            raise ValueError(f"Missing required column '{col}'. Found columns: {list(df.columns)}")
        mapping[col] = actual
    return mapping


@app.command("roles")
def ingest_roles(file: str = typer.Argument(..., help="CSV/Excel file containing roles: id, name, description, source, version")) -> None:
    """Ingest Role nodes."""
    dal = _ensure_graph()
    df = _read_table(Path(file))
    mapping = _require_columns(df, ["id", "name"])
    description_col = _norm_col(df, "description")
    source_col = _norm_col(df, "source")
    version_col = _norm_col(df, "version")
    for _, row in df.iterrows():
        dal.upsert_role(
            str(row[mapping["id"]]),
            name=str(row[mapping["name"]]) if not pd.isna(row[mapping["name"]]) else None,
            description=str(row[description_col]) if description_col and not pd.isna(row[description_col]) else None,
            source=str(row[source_col]) if source_col and not pd.isna(row[source_col]) else None,
            version=str(row[version_col]) if version_col and not pd.isna(row[version_col]) else None,
        )
    typer.echo(f"Ingested {len(df)} roles from {file}.")


@app.command("competencies")
def ingest_competencies(file: str = typer.Argument(..., help="CSV/Excel file containing competencies: id, name, definition, source, version")) -> None:
    """Ingest Competency nodes."""
    dal = _ensure_graph()
    df = _read_table(Path(file))
    mapping = _require_columns(df, ["id", "name"])
    definition_col = _norm_col(df, "definition")
    source_col = _norm_col(df, "source")
    version_col = _norm_col(df, "version")
    for _, row in df.iterrows():
        dal.upsert_competency(
            str(row[mapping["id"]]),
            name=str(row[mapping["name"]]) if not pd.isna(row[mapping["name"]]) else None,
            definition=str(row[definition_col]) if definition_col and not pd.isna(row[definition_col]) else None,
            source=str(row[source_col]) if source_col and not pd.isna(row[source_col]) else None,
            version=str(row[version_col]) if version_col and not pd.isna(row[version_col]) else None,
        )
    typer.echo(f"Ingested {len(df)} competencies from {file}.")


@app.command("requires")
def ingest_requires(
    file: str = typer.Argument(..., help="CSV/Excel file with columns: roleId, competencyId, requiredLevel, version, source, validFrom, validTo")
) -> None:
    """Ingest REQUIRES edges Role->Competency."""
    dal = _ensure_graph()
    df = _read_table(Path(file))
    mapping = _require_columns(df, ["roleId", "competencyId"])
    required_level_col = _norm_col(df, "requiredLevel")
    version_col = _norm_col(df, "version")
    source_col = _norm_col(df, "source")
    valid_from_col = _norm_col(df, "validFrom")
    valid_to_col = _norm_col(df, "validTo")

    for _, row in df.iterrows():
        dal.upsert_requires_edge(
            role_id=str(row[mapping["roleId"]]),
            competency_id=str(row[mapping["competencyId"]]),
            required_level=str(row[required_level_col]) if required_level_col and not pd.isna(row[required_level_col]) else None,
            version=str(row[version_col]) if version_col and not pd.isna(row[version_col]) else None,
            source=str(row[source_col]) if source_col and not pd.isna(row[source_col]) else None,
            valid_from=str(row[valid_from_col]) if valid_from_col and not pd.isna(row[valid_from_col]) else None,
            valid_to=str(row[valid_to_col]) if valid_to_col and not pd.isna(row[valid_to_col]) else None,
        )
    typer.echo(f"Ingested {len(df)} REQUIRES relationships from {file}.")


@app.command("adjacency")
def ingest_adjacency(
    file: str = typer.Argument(..., help="CSV/Excel file with columns: roleA, roleB, score, rationale, version, source"),
    bidirectional: bool = typer.Option(True, help="Create both A->B and B->A"),
) -> None:
    """Ingest ADJACENT_TO edges Role<->Role."""
    dal = _ensure_graph()
    df = _read_table(Path(file))
    mapping = _require_columns(df, ["roleA", "roleB"])
    score_col = _norm_col(df, "score")
    rationale_col = _norm_col(df, "rationale")
    version_col = _norm_col(df, "version")
    source_col = _norm_col(df, "source")

    for _, row in df.iterrows():
        score = float(row[score_col]) if score_col and not pd.isna(row[score_col]) else None
        dal.upsert_adjacency(
            role_a=str(row[mapping["roleA"]]),
            role_b=str(row[mapping["roleB"]]),
            score=score,
            rationale=str(row[rationale_col]) if rationale_col and not pd.isna(row[rationale_col]) else None,
            version=str(row[version_col]) if version_col and not pd.isna(row[version_col]) else None,
            source=str(row[source_col]) if source_col and not pd.isna(row[source_col]) else None,
            bidirectional=bidirectional,
        )
    typer.echo(f"Ingested {len(df)} ADJACENT_TO relationships from {file}.")


def _load_json_if_exists(path: Path) -> Optional[List[dict]]:
    if path.exists():
        with path.open("r") as f:
            return json.load(f)
    return None


@app.command("seed")
def seed_from_json(
    directory: str = typer.Argument("seed", help="Directory containing JSON seeds: roles.json, competencies.json, requires.json, adjacency.json"),
) -> None:
    """Ingest from seed JSON files if present."""
    dal = _ensure_graph()
    base = Path(directory)
    roles = _load_json_if_exists(base / "roles.json") or []
    competencies = _load_json_if_exists(base / "competencies.json") or []
    requires = _load_json_if_exists(base / "requires.json") or []
    adjacency = _load_json_if_exists(base / "adjacency.json") or []

    for r in roles:
        dal.upsert_role(
            r.get("id"),
            name=r.get("name"),
            description=r.get("description"),
            metadata=r.get("metadata") or {},
            source=r.get("source"),
            version=r.get("version"),
        )
    typer.echo(f"Seeded {len(roles)} roles from {base}/roles.json")

    for c in competencies:
        dal.upsert_competency(
            c.get("id"),
            name=c.get("name"),
            definition=c.get("definition"),
            metadata=c.get("metadata") or {},
            source=c.get("source"),
            version=c.get("version"),
        )
    typer.echo(f"Seeded {len(competencies)} competencies from {base}/competencies.json")

    for req in requires:
        dal.upsert_requires_edge(
            role_id=req.get("roleId"),
            competency_id=req.get("competencyId"),
            required_level=req.get("requiredLevel"),
            version=req.get("version"),
            source=req.get("source"),
            valid_from=req.get("validFrom"),
            valid_to=req.get("validTo"),
        )
    typer.echo(f"Seeded {len(requires)} requires from {base}/requires.json")

    for adj in adjacency:
        dal.upsert_adjacency(
            role_a=adj.get("roleA"),
            role_b=adj.get("roleB"),
            score=adj.get("score"),
            rationale=adj.get("rationale"),
            version=adj.get("version"),
            source=adj.get("source"),
            bidirectional=True,
        )
    typer.echo(f"Seeded {len(adjacency)} adjacency relationships from {base}/adjacency.json")


@app.command("all")
def ingest_all(
    data_dir: str = typer.Argument("data", help="Directory containing CSV/Excel files for roles, competencies, requires, adjacency"),
    bidirectional: bool = typer.Option(True, help="Create both A->B and B->A for adjacency"),
) -> None:
    """Attempt to ingest all entities/relationships from CSV/Excel files if present."""
    base = Path(data_dir)
    candidates = {
        "roles": [base / "roles.csv", base / "roles.xlsx"],
        "competencies": [base / "competencies.csv", base / "competencies.xlsx"],
        "requires": [base / "requires.csv", base / "requires.xlsx"],
        "adjacency": [base / "adjacency.csv", base / "adjacency.xlsx"],
    }
    dal = _ensure_graph()

    def pick_file(options: List[Path]) -> Optional[Path]:
        for p in options:
            if p.exists():
                return p
        return None

    picked = {k: pick_file(v) for k, v in candidates.items()}
    if all(v is None for v in picked.values()):
        typer.echo(f"No CSV/Excel files found in {base.resolve()}")
        raise typer.Exit(code=0)

    if picked["roles"]:
        df = _read_table(picked["roles"])
        mapping = _require_columns(df, ["id", "name"])
        description_col = _norm_col(df, "description")
        source_col = _norm_col(df, "source")
        version_col = _norm_col(df, "version")
        for _, row in df.iterrows():
            dal.upsert_role(
                str(row[mapping["id"]]),
                name=str(row[mapping["name"]]) if not pd.isna(row[mapping["name"]]) else None,
                description=str(row[description_col]) if description_col and not pd.isna(row[description_col]) else None,
                source=str(row[source_col]) if source_col and not pd.isna(row[source_col]) else None,
                version=str(row[version_col]) if version_col and not pd.isna(row[version_col]) else None,
            )
        typer.echo(f"Ingested roles from {picked['roles']}.")

    if picked["competencies"]:
        df = _read_table(picked["competencies"])
        mapping = _require_columns(df, ["id", "name"])
        definition_col = _norm_col(df, "definition")
        source_col = _norm_col(df, "source")
        version_col = _norm_col(df, "version")
        for _, row in df.iterrows():
            dal.upsert_competency(
                str(row[mapping["id"]]),
                name=str(row[mapping["name"]]) if not pd.isna(row[mapping["name"]]) else None,
                definition=str(row[definition_col]) if definition_col and not pd.isna(row[definition_col]) else None,
                source=str(row[source_col]) if source_col and not pd.isna(row[source_col]) else None,
                version=str(row[version_col]) if version_col and not pd.isna(row[version_col]) else None,
            )
        typer.echo(f"Ingested competencies from {picked['competencies']}.")

    if picked["requires"]:
        df = _read_table(picked["requires"])
        mapping = _require_columns(df, ["roleId", "competencyId"])
        required_level_col = _norm_col(df, "requiredLevel")
        version_col = _norm_col(df, "version")
        source_col = _norm_col(df, "source")
        valid_from_col = _norm_col(df, "validFrom")
        valid_to_col = _norm_col(df, "validTo")
        for _, row in df.iterrows():
            dal.upsert_requires_edge(
                role_id=str(row[mapping["roleId"]]),
                competency_id=str(row[mapping["competencyId"]]),
                required_level=str(row[required_level_col]) if required_level_col and not pd.isna(row[required_level_col]) else None,
                version=str(row[version_col]) if version_col and not pd.isna(row[version_col]) else None,
                source=str(row[source_col]) if source_col and not pd.isna(row[source_col]) else None,
                valid_from=str(row[valid_from_col]) if valid_from_col and not pd.isna(row[valid_from_col]) else None,
                valid_to=str(row[valid_to_col]) if valid_to_col and not pd.isna(row[valid_to_col]) else None,
            )
        typer.echo(f"Ingested REQUIRES from {picked['requires']}.")

    if picked["adjacency"]:
        df = _read_table(picked["adjacency"])
        mapping = _require_columns(df, ["roleA", "roleB"])
        score_col = _norm_col(df, "score")
        rationale_col = _norm_col(df, "rationale")
        version_col = _norm_col(df, "version")
        source_col = _norm_col(df, "source")
        for _, row in df.iterrows():
            score = float(row[score_col]) if score_col and not pd.isna(row[score_col]) else None
            dal.upsert_adjacency(
                role_a=str(row[mapping["roleA"]]),
                role_b=str(row[mapping["roleB"]]),
                score=score,
                rationale=str(row[rationale_col]) if rationale_col and not pd.isna(row[rationale_col]) else None,
                version=str(row[version_col]) if version_col and not pd.isna(row[version_col]) else None,
                source=str(row[source_col]) if source_col and not pd.isna(row[source_col]) else None,
                bidirectional=bidirectional,
            )
        typer.echo(f"Ingested ADJACENT_TO from {picked['adjacency']}.")


if __name__ == "__main__":
    app()
