import logging
from typing import Optional

from neo4j import Driver, GraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable

from src.core.config import Settings, get_settings

logger = logging.getLogger(__name__)


class GraphClient:
    """
    A thin wrapper around the Neo4j Python driver with lifecycle management,
    durable initialization, and a robust health check.
    """

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self._settings = settings or get_settings()
        self._driver: Optional[Driver] = None

    # PUBLIC_INTERFACE
    @property
    def enabled(self) -> bool:
        """
        True if the graph feature flag is ON and configuration appears complete.
        This indicates the app intends to use the graph (not that it is healthy).
        """
        s = self._settings
        return bool(s.feature_graph_enabled and s.neo4j_uri and s.neo4j_username and s.neo4j_password)

    def _flag_on(self) -> bool:
        """Internal: whether the feature flag is enabled."""
        return bool(self._settings.feature_graph_enabled)

    def _configured(self) -> bool:
        """Internal: whether all required NEO4J_* values are present."""
        s = self._settings
        return bool(s.neo4j_uri and s.neo4j_username and s.neo4j_password)

    # PUBLIC_INTERFACE
    def start(self) -> None:
        """
        Initialize the Neo4j driver once if the graph feature is enabled and configured.
        This is called at FastAPI startup so the same driver is reused across requests.
        """
        if not self._flag_on():
            logger.info("GraphClient start skipped: feature flag disabled.")
            return

        if not self._configured():
            missing = []
            if not self._settings.neo4j_uri:
                missing.append("NEO4J_URI")
            if not self._settings.neo4j_username:
                missing.append("NEO4J_USERNAME")
            if not self._settings.neo4j_password:
                missing.append("NEO4J_PASSWORD")
            logger.warning("GraphClient misconfigured (flag enabled) - missing env vars: %s", ", ".join(missing))
            return

        if self._driver is not None:
            logger.debug("GraphClient.start() called; driver already initialized. Reusing existing driver.")
            return

        try:
            self._driver = GraphDatabase.driver(
                self._settings.neo4j_uri,
                auth=(self._settings.neo4j_username, self._settings.neo4j_password),
            )
            # Attempt an early connectivity verification (non-fatal if it fails here)
            try:
                self._driver.verify_connectivity()
                logger.info("GraphClient started and connectivity verified.")
            except Exception as e:
                # Keep the driver; health() will provide an authoritative check later.
                logger.warning("GraphClient started but connectivity verification failed: %s", e)
        except (ServiceUnavailable, AuthError) as e:
            logger.exception("Failed to start GraphClient: %s", e)
            # Keep driver as None so the app can continue in degraded mode
            self._driver = None

    # PUBLIC_INTERFACE
    def close(self) -> None:
        """Close the driver if it was initialized."""
        if self._driver is not None:
            try:
                self._driver.close()
                logger.info("GraphClient closed.")
            except Exception as e:
                logger.warning("Error closing GraphClient: %s", e)
        self._driver = None

    # PUBLIC_INTERFACE
    def is_healthy(self) -> bool:
        """
        Return True if the driver is connected and can execute a trivial query.

        Implementation details:
        - Use driver.verify_connectivity() if available to catch network/auth issues quickly.
        - Run a minimal query (RETURN 1 AS ok) with a short transaction timeout to ensure the server responds.
        - All exceptions are caught and logged at debug level to keep the health check lightweight.
        """
        if self._driver is None:
            return False
        try:
            # Try a fast connectivity check (non-fatal if it fails here)
            try:
                self._driver.verify_connectivity()
            except Exception as exc:
                logger.debug("Graph verify_connectivity() failed: %s", exc)

            # Fallback/minimal query. The 'timeout' sets a server-side transaction timeout if supported.
            with self._driver.session() as session:
                result = session.run("RETURN 1 AS ok", timeout=3)
                record = result.single()
                return bool(record and int(record["ok"]) == 1)
        except Exception as e:
            logger.debug("Graph health check failed: %s", e)
            return False

    # PUBLIC_INTERFACE
    def status(self) -> str:
        """
        Return the graph integration status:
        - "disabled": feature flag is OFF
        - "misconfigured": feature flag ON but NEO4J_* env vars missing
        - "healthy": connectivity ok and a trivial query responds
        - "unhealthy": configured but connectivity/auth fails
        """
        if not self._flag_on():
            return "disabled"
        if not self._configured():
            return "misconfigured"
        return "healthy" if self.is_healthy() else "unhealthy"

    # PUBLIC_INTERFACE
    @property
    def driver(self) -> Optional[Driver]:
        """Expose the underlying driver (or None if disabled/unavailable)."""
        return self._driver
