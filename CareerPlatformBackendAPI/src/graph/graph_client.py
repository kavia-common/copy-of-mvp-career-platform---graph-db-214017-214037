import logging
from typing import Optional

from neo4j import GraphDatabase, Driver
from neo4j.exceptions import ServiceUnavailable, AuthError

from src.core.config import get_settings, Settings

logger = logging.getLogger(__name__)


class GraphClient:
    """
    A thin wrapper around the Neo4j Python driver with lifecycle management
    and a basic health check.
    """

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self._settings = settings or get_settings()
        self._driver: Optional[Driver] = None

    @property
    def enabled(self) -> bool:
        """True if the graph feature flag is enabled and configuration is provided."""
        s = self._settings
        return bool(
            s.feature_graph_enabled and s.neo4j_uri and s.neo4j_username and s.neo4j_password
        )

    # PUBLIC_INTERFACE
    def start(self) -> None:
        """Initialize the Neo4j driver if enabled."""
        if not self.enabled:
            logger.info("GraphClient start skipped: feature disabled or missing config.")
            return

        try:
            self._driver = GraphDatabase.driver(
                self._settings.neo4j_uri, auth=(self._settings.neo4j_username, self._settings.neo4j_password)
            )
            # Run a quick sanity check
            if not self.is_healthy():
                logger.warning("GraphClient started but health check failed.")
            else:
                logger.info("GraphClient started and healthy.")
        except (ServiceUnavailable, AuthError) as e:
            logger.exception("Failed to start GraphClient: %s", e)
            # Keep driver as None so app can still run in non-graph mode
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
        """Return True if the driver is connected and can execute a trivial query."""
        if self._driver is None:
            return False
        try:
            with self._driver.session() as session:
                result = session.run("RETURN 1 AS ok")
                record = result.single()
                return bool(record and record["ok"] == 1)
        except Exception as e:
            logger.debug("Graph health check failed: %s", e)
            return False

    @property
    def driver(self) -> Optional[Driver]:
        """Expose the underlying driver (or None if disabled/unavailable)."""
        return self._driver
