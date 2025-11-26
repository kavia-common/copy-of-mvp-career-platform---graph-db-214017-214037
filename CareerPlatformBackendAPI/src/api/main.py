import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from src.core.config import get_settings
from src.graph.graph_client import GraphClient
from src.graph.graph_dal import GraphDAL

logger = logging.getLogger(__name__)

openapi_tags = [
    {"name": "Health", "description": "Service health and diagnostics"},
    {"name": "Graph", "description": "Feature-flagged role adjacency via Neo4j"},
]


class RoleModel(BaseModel):
    """Role model used in API responses."""

    id: str = Field(..., description="Unique role identifier")
    name: Optional[str] = Field(None, description="Role name")
    description: Optional[str] = Field(None, description="Role description")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    source: Optional[str] = Field(None, description="Source of the data")
    version: Optional[str] = Field(None, description="Version of the data")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()
    app = FastAPI(
        title=settings.app_title,
        description=settings.app_description,
        version=settings.app_version,
        openapi_tags=openapi_tags,
    )

    # CORS
    allow_origins = [o.strip() for o in settings.cors_allow_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Initialize graph client/DAL on startup based on feature flag
    @app.on_event("startup")
    def _on_startup() -> None:
        """
        Startup event: initialize graph client if feature flag is enabled and config provided.
        """
        app.state.settings = settings
        app.state.graph_client = GraphClient(settings)
        if settings.feature_graph_enabled:
            app.state.graph_client.start()
            if app.state.graph_client.driver:
                app.state.graph_dal = GraphDAL(app.state.graph_client.driver)
            else:
                app.state.graph_dal = None
                logger.warning("Graph feature enabled but driver not initialized.")
        else:
            app.state.graph_dal = None
            logger.info("Graph feature disabled. Using fallback behavior.")

    @app.on_event("shutdown")
    def _on_shutdown() -> None:
        """
        Shutdown event: close graph client if initialized.
        """
        client = getattr(app.state, "graph_client", None)
        if client:
            client.close()

    # PUBLIC_INTERFACE
    @app.get("/", tags=["Health"], summary="Health Check")
    def health_check() -> Dict[str, str]:
        """
        Health check endpoint.
        Returns a simple JSON payload indicating service health.
        """
        client = getattr(app.state, "graph_client", None)
        graph_status = "disabled"
        if client and client.enabled:
            graph_status = "healthy" if client.is_healthy() else "unhealthy"
        return {"message": "Healthy", "graph": graph_status}

    # PUBLIC_INTERFACE
    @app.get(
        "/role-adjacency",
        response_model=List[RoleModel],
        tags=["Graph"],
        summary="Get alternative role suggestions",
        description=(
            "Returns a list of adjacent/suggested roles. "
            "When FEATURE_GRAPH_ENABLED=true and Neo4j is configured, results are fetched from the graph; "
            "otherwise the endpoint returns an empty list as a placeholder."
        ),
        responses={
            200: {"description": "Role suggestions"},
            500: {"description": "Internal Server Error"},
        },
    )
    def get_role_adjacency(
        userId: Optional[str] = Query(
            default=None,
            description="Optional user identifier to contextualize suggestions",
        ),
        currentRoleId: Optional[str] = Query(
            default=None,
            description="Current role id to base adjacency suggestions on",
        ),
        targetRoleId: Optional[str] = Query(
            default=None,
            description="Optional target role id to bias adjacency results",
        ),
        limit: int = Query(default=20, ge=1, le=100, description="Max number of suggestions"),
    ) -> List[RoleModel]:
        """
        Get adjacent roles from the graph if enabled, otherwise fallback to an empty list.
        The API response shape remains unchanged.
        """
        dal: Optional[GraphDAL] = getattr(app.state, "graph_dal", None)
        if dal is None:
            return []

        try:
            rows = dal.get_role_adjacency(
                user_id=userId, current_role_id=currentRoleId, target_role_id=targetRoleId, limit=limit
            )
            # Map DAL output to RoleModel (ignore rationale in public response to keep contract)
            resp: List[RoleModel] = []
            for r in rows:
                resp.append(
                    RoleModel(
                        id=str(r.get("id")),
                        name=r.get("name"),
                        description=r.get("description"),
                    )
                )
            return resp
        except Exception as e:
            logger.exception("Error retrieving role adjacency: %s", e)
            # Fallback to empty to keep API resilient
            return []

    return app


# FastAPI application instance
app = create_app()
