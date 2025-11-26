import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from src.core.config import get_settings
from src.graph.graph_client import GraphClient
from src.graph.graph_dal import GraphDAL

logger = logging.getLogger(__name__)

openapi_tags = [
    {"name": "Health", "description": "Service health and diagnostics"},
    {"name": "Graph", "description": "Feature-flagged role adjacency via Neo4j"},
    {"name": "Roles", "description": "Manage and query Role entities"},
]


class RoleModel(BaseModel):
    """Role model used in API responses."""

    id: str = Field(..., description="Unique role identifier")
    name: Optional[str] = Field(None, description="Role name")
    description: Optional[str] = Field(None, description="Role description")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    source: Optional[str] = Field(None, description="Source of the data")
    version: Optional[str] = Field(None, description="Version of the data")


class RoleUpsertModel(BaseModel):
    """Payload model for creating or updating a role."""

    id: str = Field(..., min_length=1, description="Unique role identifier")
    name: Optional[str] = Field(None, description="Role name")
    description: Optional[str] = Field(None, description="Role description")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    source: Optional[str] = Field(None, description="Source of the data")
    version: Optional[str] = Field(None, description="Version of the data")


class InMemoryRoleDAO:
    """
    A very simple in-memory Role DAO used as a fallback when the graph feature
    is disabled or unavailable. This keeps the API functional for local/dev environments.
    """

    def __init__(self) -> None:
        self._store: Dict[str, Dict[str, Any]] = {}

    # PUBLIC_INTERFACE
    def upsert(self, payload: RoleUpsertModel) -> Dict[str, Any]:
        """
        Upsert a role using COALESCE semantics (only overwrite fields when provided).
        Returns the stored role as a dict.
        """
        existing = self._store.get(payload.id, {})
        obj: Dict[str, Any] = {
            "id": payload.id,
            "name": payload.name if payload.name is not None else existing.get("name"),
            "description": payload.description if payload.description is not None else existing.get("description"),
            "metadata": payload.metadata if payload.metadata is not None else existing.get("metadata"),
            "source": payload.source if payload.source is not None else existing.get("source"),
            "version": payload.version if payload.version is not None else existing.get("version"),
        }
        self._store[payload.id] = obj
        return obj

    # PUBLIC_INTERFACE
    def get(self, role_id: str) -> Optional[Dict[str, Any]]:
        """Return a role by id or None if not found."""
        return self._store.get(role_id)

    # PUBLIC_INTERFACE
    def all(self) -> List[Dict[str, Any]]:
        """Return all roles as a list."""
        return list(self._store.values())


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
        app.state.role_dao = None  # default

        if settings.feature_graph_enabled:
            app.state.graph_client.start()
            if app.state.graph_client.driver:
                app.state.graph_dal = GraphDAL(app.state.graph_client.driver)
                app.state.role_dao = None
                logger.info("Graph feature enabled and driver initialized.")
            else:
                app.state.graph_dal = None
                app.state.role_dao = InMemoryRoleDAO()
                logger.warning("Graph feature enabled but driver not initialized. Falling back to in-memory Role DAO.")
        else:
            app.state.graph_dal = None
            app.state.role_dao = InMemoryRoleDAO()
            logger.info("Graph feature disabled. Using in-memory Role DAO.")

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

        Returns a simple JSON payload indicating service health. Graph status will be one of:
        - "disabled" when FEATURE_GRAPH_ENABLED/USE_GRAPH is false
        - "misconfigured" when the flag is on but NEO4J_* env vars are missing
        - "healthy" when the Neo4j driver is connected and responds to a trivial query
        - "unhealthy" when configured but connectivity/auth fails
        """
        client = getattr(app.state, "graph_client", None)
        graph_status = "disabled"
        if client:
            try:
                graph_status = client.status()
            except Exception as e:
                logger.debug("Graph status check failed: %s", e)
                graph_status = "unhealthy" if getattr(client, "enabled", False) else "disabled"
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

    # -------------------- Roles Endpoints --------------------

    # PUBLIC_INTERFACE
    @app.post(
        "/roles",
        response_model=RoleModel,
        status_code=status.HTTP_201_CREATED,
        tags=["Roles"],
        summary="Create or upsert a role",
        description=(
            "Create or update a Role by id. When graph feature is enabled and healthy, the role is persisted "
            "to Neo4j; otherwise an in-memory fallback store is used."
        ),
        responses={
            201: {"description": "Role created or updated"},
            400: {"description": "Bad request"},
            500: {"description": "Internal Server Error"},
        },
    )
    def create_or_upsert_role(payload: RoleUpsertModel) -> RoleModel:
        """
        Create or update a Role entity.

        Parameters:
        - payload: RoleUpsertModel containing id, name, description, metadata, source, version

        Returns:
        - RoleModel: The stored role representation
        """
        dal: Optional[GraphDAL] = getattr(app.state, "graph_dal", None)
        mem: Optional[InMemoryRoleDAO] = getattr(app.state, "role_dao", None)

        try:
            if dal:
                dal.upsert_role(
                    role_id=payload.id,
                    name=payload.name,
                    description=payload.description,
                    metadata=payload.metadata or {},
                    source=payload.source,
                    version=payload.version,
                )
                # Try to fetch the stored value back from graph for accuracy
                rec = dal.get_role_by_id(payload.id) or payload.model_dump()
                return RoleModel(**rec)
            else:
                if mem is None:
                    # Initialize fallback if somehow missing
                    app.state.role_dao = InMemoryRoleDAO()
                    mem = app.state.role_dao
                    logger.warning("Initialized in-memory Role DAO dynamically.")
                rec = mem.upsert(payload)
                # Log to indicate fallback behavior
                logger.warning("Graph disabled/unavailable; POST /roles persisted to in-memory store.")
                return RoleModel(**rec)
        except Exception as e:
            logger.exception("Error upserting role: %s", e)
            raise HTTPException(status_code=500, detail="Failed to create or update role")

    # PUBLIC_INTERFACE
    @app.get(
        "/roles",
        response_model=List[RoleModel],
        tags=["Roles"],
        summary="List roles",
        description=(
            "List roles from Neo4j when the graph feature is enabled; otherwise list from in-memory fallback store."
        ),
        responses={
            200: {"description": "Roles list"},
            500: {"description": "Internal Server Error"},
        },
    )
    def list_roles(
        limit: int = Query(500, ge=1, le=5000, description="Max number of roles to return")
    ) -> List[RoleModel]:
        """
        Retrieve all roles up to the provided limit.

        Parameters:
        - limit: maximum number of roles to return (default 500)

        Returns:
        - List[RoleModel]: A list of roles
        """
        dal: Optional[GraphDAL] = getattr(app.state, "graph_dal", None)
        mem: Optional[InMemoryRoleDAO] = getattr(app.state, "role_dao", None)

        try:
            if dal:
                rows = dal.get_roles(limit=limit)
                return [RoleModel(**row) for row in rows]
            else:
                if mem is None:
                    app.state.role_dao = InMemoryRoleDAO()
                    mem = app.state.role_dao
                    logger.warning("Initialized in-memory Role DAO dynamically.")
                logger.warning("Graph disabled/unavailable; GET /roles reading from in-memory store.")
                return [RoleModel(**row) for row in mem.all()]
        except Exception as e:
            logger.exception("Error listing roles: %s", e)
            raise HTTPException(status_code=500, detail="Failed to list roles")

    # PUBLIC_INTERFACE
    @app.get(
        "/roles/{role_id}",
        response_model=RoleModel,
        tags=["Roles"],
        summary="Get role by id",
        description="Retrieve a single Role by id from Neo4j or the in-memory fallback store.",
        responses={
            200: {"description": "Role found"},
            404: {"description": "Role not found"},
            500: {"description": "Internal Server Error"},
        },
    )
    def get_role_by_id(role_id: str) -> RoleModel:
        """
        Get a Role by its identifier.

        Parameters:
        - role_id: The role's unique id

        Returns:
        - RoleModel: The role instance

        Raises:
        - 404 if not found
        """
        dal: Optional[GraphDAL] = getattr(app.state, "graph_dal", None)
        mem: Optional[InMemoryRoleDAO] = getattr(app.state, "role_dao", None)

        try:
            if dal:
                rec = dal.get_role_by_id(role_id)
                if not rec:
                    raise HTTPException(status_code=404, detail="Role not found")
                return RoleModel(**rec)
            else:
                if mem is None:
                    app.state.role_dao = InMemoryRoleDAO()
                    mem = app.state.role_dao
                    logger.warning("Initialized in-memory Role DAO dynamically.")
                rec = mem.get(role_id)
                if not rec:
                    raise HTTPException(status_code=404, detail="Role not found")
                logger.warning("Graph disabled/unavailable; GET /roles/{id} reading from in-memory store.")
                return RoleModel(**rec)
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Error retrieving role by id %s: %s", role_id, e)
            raise HTTPException(status_code=500, detail="Failed to retrieve role")

    return app


# FastAPI application instance
app = create_app()
