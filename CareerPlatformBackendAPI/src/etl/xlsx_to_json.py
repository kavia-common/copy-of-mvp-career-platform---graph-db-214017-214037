"""Excel (.xlsx/.xls) to normalized JSON converter for graph ETL seeds.

This utility provides commands to convert the project’s provided spreadsheets
into JSON seed files compatible with src.etl.ingest_graph.seed().
It is optional because ingest_graph already supports CSV/Excel directly,
but JSON mirrors help with MCP flows and traceability.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import typer

app = typer.Typer(add_completion=False, help="Excel (.xlsx/.xls) to normalized JSON converter for graph ETL seeds.")


def _canon(name: str) -> str:
    """Return a canonical, comparable version of a string (lowercase, alphanumeric only)."""
    return "".join(ch.lower() for ch in str(name).strip() if ch.isalnum())


def _build_lookup(df: pd.DataFrame) -> Dict[str, str]:
    """Build a lookup from canonical column name to actual column name found in the DataFrame."""
    return {_canon(col): col for col in df.columns}


def _find_col(lookup: Dict[str, str], candidates: List[str]) -> Optional[str]:
    """Find the first matching column in the given lookup using candidate names."""
    for cand in candidates:
        canon = _canon(cand)
        if canon in lookup:
            return lookup[canon]
    return None


def _require_mapping(df: pd.DataFrame, spec: Dict[str, List[str]], required_keys: List[str]) -> Dict[str, Optional[str]]:
    """Resolve a mapping from logical keys to actual DataFrame column names using a synonym spec."""
    lookup = _build_lookup(df)
    mapping: Dict[str, Optional[str]] = {}
    for key, candidates in spec.items():
        mapping[key] = _find_col(lookup, candidates)

    missing = [k for k in required_keys if not mapping.get(k)]
    if missing:
        raise ValueError(f"Missing required columns {missing}. Available columns: {list(df.columns)}")
    return mapping


def _read_excel(path: Path, sheet: Optional[str] = None) -> pd.DataFrame:
    """Read an Excel file (first sheet by default)."""
    return pd.read_excel(path, sheet_name=sheet)


def _opt_str(val) -> Optional[str]:
    """Return None for NaN/blank values, otherwise a string representation."""
    if val is None:
        return None
    # Use pandas to detect NaN safely
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    s = str(val).strip()
    return s if s != "" else None


def _opt_float(val) -> Optional[float]:
    v = _opt_str(val)
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _ensure_out_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, payload: List[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def _summarize(path_in: Path, path_out: Path, count: int) -> None:
    typer.echo(f"Wrote {count} records to {path_out} (from {path_in.name})")


def _resolve_output(output_dir: Path, default_name: str) -> Path:
    return output_dir / default_name


def _norm_role_name(name: str) -> str:
    """Normalize role names for fuzzy matching (lowercase, alphanumeric only)."""
    return _canon(name)


def _abbr_fallback(name: str) -> str:
    """Create a simple fallback id from a role name, using PascalCase words or initials."""
    # Prefer initials of words
    parts = [p for p in str(name).replace("/", " ").replace("&", " ").split() if p.strip()]
    if not parts:
        return _canon(name) or "UNKNOWN"
    initials = "".join(p[0] for p in parts if p)
    if 1 < len(initials) <= 6:
        return initials
    # Otherwise a compact slug
    return "".join(p.capitalize() for p in parts[:4])


def _load_role_abbreviation_map(mapping_xlsx: Path) -> Dict[str, str]:
    """
    Load 'Role abbreviations' sheet from the competency mapping workbook and
    return a dictionary mapping normalized full role name -> abbreviation id.
    """
    try:
        df = pd.read_excel(mapping_xlsx, sheet_name="Role abbreviations")
    except Exception:
        return {}
    role_col = None
    abbr_col = None
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl in {"role", "rolename", "name"}:
            role_col = c
        if cl in {"abbreviation", "abbr", "code"}:
            abbr_col = c
    if not role_col or not abbr_col:
        return {}
    mapping: Dict[str, str] = {}
    for _, row in df.iterrows():
        role_name = _opt_str(row[role_col])
        abbr = _opt_str(row[abbr_col])
        if not role_name or not abbr:
            continue
        mapping[_norm_role_name(role_name)] = abbr
    return mapping


def _match_role_to_abbr(role_name: str, abbr_map: Dict[str, str]) -> str:
    """
    Try to map a role display name to a standard abbreviation id using:
    - exact normalized match
    - contains heuristic (either way)
    Fallback: generate a simple id from the name.
    """
    norm = _norm_role_name(role_name)
    if norm in abbr_map:
        return abbr_map[norm]

    # Heuristic: find any abbr whose role key is contained within norm or vice versa
    for role_key, abbr in abbr_map.items():
        if role_key in norm or norm in role_key:
            return abbr

    # Fallback to generated id
    return _abbr_fallback(role_name)


# PUBLIC_INTERFACE
@app.command("roles-from-abbrev")
def roles_from_abbrev(
    input_file: str = typer.Argument(..., help="Excel file containing a 'Role abbreviations' sheet."),
    output_dir: str = typer.Option("data/processed", help="Directory to write roles.json"),
) -> None:
    """Convert Role abbreviations sheet to roles.json (id=abbreviation, name=role)."""
    path_in = Path(input_file)
    df = pd.read_excel(path_in, sheet_name="Role abbreviations")
    # Resolve columns
    role_col = None
    abbr_col = None
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl in {"role", "rolename", "name"}:
            role_col = c
        if cl in {"abbreviation", "abbr", "code"}:
            abbr_col = c
    if not role_col or not abbr_col:
        raise ValueError("Could not find 'Role' and 'Abbreviation' columns in 'Role abbreviations' sheet")

    records: List[dict] = []
    for _, row in df.iterrows():
        role = _opt_str(row[role_col])
        abbr = _opt_str(row[abbr_col])
        if not role or not abbr:
            continue
        records.append(
            {
                "id": abbr,
                "name": role,
                "description": None,
                "metadata": {},
                "source": Path(input_file).name,
                "version": None,
            }
        )

    out_dir = Path(output_dir)
    _ensure_out_dir(out_dir)
    out_path = _resolve_output(out_dir, "roles.json")
    _write_json(out_path, records)
    _summarize(path_in, out_path, len(records))


# PUBLIC_INTERFACE
@app.command("competencies-from-definitions")
def competencies_from_definitions(
    input_file: str = typer.Argument(..., help="Excel file containing a 'Competency Definitions' sheet."),
    sheet: Optional[str] = typer.Option("Competency Definitions", help="Sheet name with competency definitions"),
    output_dir: str = typer.Option("data/processed", help="Directory to write competencies.json"),
) -> None:
    """Convert a 'Competency Definitions' sheet to competencies.json."""
    path_in = Path(input_file)
    df = pd.read_excel(path_in, sheet_name=sheet or "Competency Definitions")
    # Resolve columns
    comp_col = None
    def_col = None
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl in {"competency", "name", "competencyname"}:
            comp_col = c
        if cl in {"definition", "desc", "description"}:
            def_col = c
    if not comp_col:
        raise ValueError("Could not find 'Competency' column in definitions sheet")

    records: List[dict] = []
    for _, row in df.iterrows():
        comp = _opt_str(row[comp_col])
        if not comp:
            continue
        definition = _opt_str(row[def_col]) if def_col else None
        records.append(
            {
                "id": comp,
                "name": comp,
                "definition": definition,
                "metadata": {},
                "source": Path(input_file).name,
                "version": None,
            }
        )

    out_dir = Path(output_dir)
    _ensure_out_dir(out_dir)
    out_path = _resolve_output(out_dir, "competencies.json")
    _write_json(out_path, records)
    _summarize(path_in, out_path, len(records))


# PUBLIC_INTERFACE
@app.command("requires-from-matrix")
def requires_from_matrix(
    input_file: str = typer.Argument(..., help="Excel file containing a matrix sheet (default: 'Competencies and roles')."),
    sheet: Optional[str] = typer.Option("Competencies and roles", help="Sheet name with competency vs role abbreviation columns"),
    output_dir: str = typer.Option("data/processed", help="Directory to write requires.json"),
) -> None:
    """
    Convert a competency matrix to requires.json.
    Expected format: first column = 'Competency', subsequent columns = role abbreviations (e.g., CA, CTO, ...).
    Any non-empty cell indicates a REQUIRES relationship; the cell text is used as 'requiredLevel'.
    """
    path_in = Path(input_file)
    df = pd.read_excel(path_in, sheet_name=sheet or 0)

    # Identify the competency column
    comp_col = None
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl in {"competency", "name"}:
            comp_col = c
            break
    if not comp_col:
        # fallback: assume first column is competency
        comp_col = df.columns[0]

    role_columns = [c for c in df.columns if c != comp_col]

    records: List[dict] = []
    for _, row in df.iterrows():
        competency_id = _opt_str(row[comp_col])
        if not competency_id:
            continue
        for rc in role_columns:
            val = row[rc]
            if pd.isna(val):
                continue
            s = _opt_str(val)
            if s:
                records.append(
                    {
                        "roleId": str(rc).strip(),
                        "competencyId": competency_id,
                        "requiredLevel": s,
                        "version": None,
                        "source": Path(input_file).name,
                        "validFrom": None,
                        "validTo": None,
                    }
                )

    out_dir = Path(output_dir)
    _ensure_out_dir(out_dir)
    out_path = _resolve_output(out_dir, "requires.json")
    _write_json(out_path, records)
    _summarize(path_in, out_path, len(records))


# PUBLIC_INTERFACE
@app.command("adjacency-from-matrix")
def adjacency_from_matrix(
    input_file: str = typer.Argument(..., help="Excel file containing a square adjacency matrix (rows/columns = roles)."),
    sheet: Optional[str] = typer.Option(None, help="Sheet name (defaults to first sheet)"),
    mapping_xlsx: Optional[str] = typer.Option(
        None, help="Optional workbook with 'Role abbreviations' to map full names to ids (e.g., Competency_mapping.xlsx)"
    ),
    output_dir: str = typer.Option("data/processed", help="Directory to write adjacency.json"),
    use_upper_triangle_only: bool = typer.Option(True, help="Avoid duplicates by emitting only one direction per pair"),
) -> None:
    """
    Convert a role adjacency matrix (square table) to adjacency.json with records:
      { roleA, roleB, score, rationale, version, source }
    - Self-edges are ignored
    - If mapping_xlsx is provided, map row/column role names to abbreviations
    """
    path_in = Path(input_file)
    df = pd.read_excel(path_in, sheet_name=sheet or 0)

    # Heuristic: first column is row labels if it is non-numeric and doesn't align with header columns
    # If the first column header looks like "Unnamed: 0", treat it as row labels.
    row_label_col = df.columns[0]
    # Get column role names (excluding row label col)
    col_roles: List[str] = [str(c).strip() for c in df.columns[1:]]
    row_roles: List[str] = []

    for _, row in df.iterrows():
        row_roles.append(str(row[row_label_col]).strip())

    # Optional mapping to abbreviations
    abbr_map: Dict[str, str] = {}
    if mapping_xlsx:
        abbr_map = _load_role_abbreviation_map(Path(mapping_xlsx))

    def to_id(display_name: str) -> str:
        if abbr_map:
            return _match_role_to_abbr(display_name, abbr_map)
        return _abbr_fallback(display_name)

    # Convert matrix to edges
    seen: set[frozenset[str]] = set()
    records: List[dict] = []

    n_rows = len(row_roles)
    n_cols = len(col_roles)

    for i in range(n_rows):
        role_a_name = row_roles[i]
        # Defensive: columns align with rows (i vs j); if not, we still use any j.
        for j in range(n_cols):
            role_b_name = col_roles[j]
            # Skip if missing names
            if not role_a_name or not role_b_name:
                continue
            # Self edge: row i corresponds to column j with same name?
            if role_a_name == role_b_name:
                continue
            score_val = df.iloc[i, j + 1]  # +1 to skip label column
            if pd.isna(score_val):
                continue

            a_id = to_id(role_a_name)
            b_id = to_id(role_b_name)
            edge_key = frozenset({a_id, b_id})

            # Deduplicate if requested
            if use_upper_triangle_only and edge_key in seen:
                continue

            records.append(
                {
                    "roleA": a_id,
                    "roleB": b_id,
                    "score": _opt_float(score_val),
                    "rationale": None,
                    "version": None,
                    "source": Path(input_file).name,
                }
            )
            seen.add(edge_key)

    out_dir = Path(output_dir)
    _ensure_out_dir(out_dir)
    out_path = _resolve_output(out_dir, "adjacency.json")
    _write_json(out_path, records)
    _summarize(path_in, out_path, len(records))


if __name__ == "__main__":
    app()
