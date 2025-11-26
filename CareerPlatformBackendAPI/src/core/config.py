import logging
import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def _to_bool(val: Optional[str], default: bool = False) -> bool:
    """
    Convert environment variable strings to boolean.
    Accepts: "1", "true", "t", "yes", "y" (case insensitive) as True.
    """
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "t", "yes", "y"}


def _to_float(val: Optional[str], default: Optional[float] = None) -> Optional[float]:
    """Convert a string to float, returning default when None/empty or on parse failure."""
    if val is None:
        return default
    s = str(val).strip()
    if s == "":
        return default
    try:
        return float(s)
    except Exception:
        return default


@dataclass(frozen=True)
class Settings:
    """
    Application settings loaded from environment variables.

    This is intentionally simple to avoid additional dependencies.
    """

    # App metadata
    app_title: str = "MVP Career Platform Internal REST API"
    app_description: str = (
        "Internal API for authentication, role/competency management, gap analysis, "
        "plan generation, and admin functions. All endpoints are versioned and secured."
    )
    app_version: str = "1.0.0"

    # Feature flags
    feature_graph_enabled: bool = False

    # Neo4j configuration
    neo4j_uri: Optional[str] = None
    neo4j_username: Optional[str] = None
    neo4j_password: Optional[str] = None

    # Neo4j health tuning
    neo4j_health_query: str = "RETURN 1 AS ok"
    neo4j_connect_timeout: Optional[float] = None  # seconds

    # CORS
    cors_allow_origins: str = "*"  # comma-separated allowed origins


_settings: Optional[Settings] = None


# PUBLIC_INTERFACE
def get_settings() -> Settings:
    """Return a singleton Settings instance loaded from environment variables."""
    global _settings
    if _settings is not None:
        return _settings

    # Load .env if present (no-op if missing)
    load_dotenv(override=False)

    # Read environment variables
    # Support both FEATURE_GRAPH_ENABLED and USE_GRAPH as aliases for toggling graph usage
    feature_graph_enabled = _to_bool(os.getenv("FEATURE_GRAPH_ENABLED") or os.getenv("USE_GRAPH"), default=False)
    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_username = os.getenv("NEO4J_USERNAME")
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    neo4j_health_query = os.getenv("NEO4J_HEALTH_QUERY", "RETURN 1 AS ok")
    neo4j_connect_timeout = _to_float(os.getenv("NEO4J_CONNECT_TIMEOUT"), default=3.0)
    cors_allow_origins = os.getenv("CORS_ALLOW_ORIGINS", "*")

    _settings = Settings(
        feature_graph_enabled=feature_graph_enabled,
        neo4j_uri=neo4j_uri,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_health_query=neo4j_health_query,
        neo4j_connect_timeout=neo4j_connect_timeout,
        cors_allow_origins=cors_allow_origins,
    )

    # Debug logging about configuration (mask sensitive values)
    try:
        logger.debug(
            "Settings resolved: feature_graph_enabled=%s, NEO4J_URI set=%s, NEO4J_USERNAME set=%s, "
            "NEO4J_PASSWORD set=%s, NEO4J_CONNECT_TIMEOUT=%s, NEO4J_HEALTH_QUERY='%s', CORS_ALLOW_ORIGINS='%s'",
            feature_graph_enabled,
            bool(neo4j_uri),
            bool(neo4j_username),
            bool(neo4j_password),
            neo4j_connect_timeout,
            neo4j_health_query,
            cors_allow_origins,
        )
    except Exception:
        # Avoid any possible logging errors impacting startup
        pass

    return _settings
