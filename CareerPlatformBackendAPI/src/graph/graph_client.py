import logging
from typing import Dict, Optional, Tuple

from neo4j import Driver, GraphDatabase
from neo4j.exceptions import AuthError, Neo4jError, ServiceUnavailable

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
        self._last_status_details: Optional[Dict[str, str]] = None

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

    def _missing_envs(self) -> list[str]:
        missing = []
        if not self._settings.neo4j_uri:
            missing.append("NEO4J_URI")
        if not self._settings.neo4j_username:
            missing.append("NEO4J_USERNAME")
        if not self._settings.neo4j_password:
            missing.append("NEO4J_PASSWORD")
        return missing

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
            missing = self._missing_envs()
            logger.warning("GraphClient misconfigured (flag enabled) - missing env vars: %s", ", ".join(missing))
            return

        if self._driver is not None:
            logger.debug("GraphClient.start() called; driver already initialized. Reusing existing driver.")
            return

        try:
            # Respect optional connection timeout for establishing TCP connections/handshake
            driver_kwargs = {}
            if self._settings.neo4j_connect_timeout is not None:
                driver_kwargs["connection_timeout"] = self._settings.neo4j_connect_timeout

            self._driver = GraphDatabase.driver(
                self._settings.neo4j_uri,
                auth=(self._settings.neo4j_username, self._settings.neo4j_password),
                **driver_kwargs,
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

    def _categorize_error(self, e: Exception) -> Tuple[str, str, str]:
        """
        Categorize an exception into (category, code, hint).

        Categories: 'auth', 'network', 'timeout', 'other'
        """
        msg = str(e) if e else ""
        code = ""
        # Auth errors
        if isinstance(e, AuthError):
            code = getattr(e, "code", "") or "Neo.ClientError.Security.Unauthorized"
            return "auth", code, "Neo4j authentication failed. Verify NEO4J_USERNAME/NEO4J_PASSWORD and database auth."
        if isinstance(e, Neo4jError):
            code = getattr(e, "code", "") or ""
            if code == "Neo.ClientError.Security.Unauthorized":
                return "auth", code, "Neo4j authentication failed. Verify NEO4J_USERNAME/NEO4J_PASSWORD and database auth."
        # Network/service issues
        if isinstance(e, ServiceUnavailable):
            return "network", code or "ServiceUnavailable", "Neo4j service unavailable. Check NEO4J_URI host/port and network connectivity."
        low = msg.lower()
        if "timed out" in low or "timeout" in low:
            return "timeout", code or "Timeout", "Connection timed out. Consider adjusting NEO4J_CONNECT_TIMEOUT and verify reachability."
        if any(s in low for s in ["dns", "nodename nor servname provided", "name or service not known", "connection refused"]):
            return "network", code or "NetworkError", "Host resolution/connectivity issue. Verify NEO4J_URI host and port are correct and reachable."
        return "other", code or "", "Unexpected error during graph connectivity. Check backend logs for details."

    # PUBLIC_INTERFACE
    def check_health(self) -> Dict[str, str]:
        """
        Perform a robust health check and return details:
        {
          "healthy": "true|false",
          "category": "auth|network|timeout|other|ok|disabled|misconfigured",
          "message": "<error-message or ok>",
          "hint": "<actionable hint>",
          "code": "<driver error code if any>"
        }
        """
        details: Dict[str, str] = {
            "healthy": "false",
            "category": "",
            "message": "",
            "hint": "",
            "code": "",
        }

        # Evaluate flag/config before attempting driver operations
        if not self._flag_on():
            details.update({
                "healthy": "false",
                "category": "disabled",
                "message": "Graph feature disabled by flag.",
                "hint": "Set FEATURE_GRAPH_ENABLED=true or USE_GRAPH=true to enable.",
            })
            self._last_status_details = details
            return details

        if not self._configured():
            missing = self._missing_envs()
            details.update({
                "healthy": "false",
                "category": "misconfigured",
                "message": f"Missing required env vars: {', '.join(missing)}",
                "hint": "Provide NEO4J_URI, NEO4J_USERNAME, and NEO4J_PASSWORD.",
            })
            self._last_status_details = details
            return details

        if self._driver is None:
            # Driver hasn't started successfully
            details.update({
                "healthy": "false",
                "category": "network",
                "message": "Neo4j driver not initialized.",
                "hint": "Check earlier startup logs for driver initialization errors.",
            })
            self._last_status_details = details
            return details

        # Attempt connectivity verification and a trivial query with short timeout
        try:
            try:
                self._driver.verify_connectivity()
            except Exception as exc:
                cat, code, hint = self._categorize_error(exc)
                details.update({
                    "healthy": "false",
                    "category": cat,
                    "message": str(exc),
                    "hint": hint,
                    "code": code,
                })
                logger.debug("Graph verify_connectivity() failed: %s", exc)
                self._last_status_details = details
                return details

            # Minimal query; timeout prevents long-hanging sockets server-side if supported
            health_query = self._settings.neo4j_health_query or "RETURN 1 AS ok"
            timeout = self._settings.neo4j_connect_timeout or 3.0
            with self._driver.session() as session:
                result = session.run(health_query, timeout=timeout)
                record = result.single()
                ok_value = None
                if record is not None:
                    # try 'ok' or first column fallback
                    if "ok" in record.keys():
                        ok_value = record.get("ok")
                    else:
                        # fallback to first key
                        keys = list(record.keys())
                        if keys:
                            ok_value = record.get(keys[0])

                if record is None:
                    details.update({
                        "healthy": "false",
                        "category": "other",
                        "message": "Health query returned no record.",
                        "hint": "Ensure the configured NEO4J_HEALTH_QUERY returns a single record.",
                    })
                    self._last_status_details = details
                    return details

                try:
                    is_ok = int(ok_value) == 1
                except Exception:
                    is_ok = bool(ok_value)

                if is_ok:
                    details.update({
                        "healthy": "true",
                        "category": "ok",
                        "message": "Connected",
                        "hint": "",
                    })
                    self._last_status_details = details
                    return details
                else:
                    details.update({
                        "healthy": "false",
                        "category": "other",
                        "message": f"Unexpected health query result: {ok_value!r}",
                        "hint": "Ensure the configured NEO4J_HEALTH_QUERY returns 1 or a truthy value.",
                    })
                    self._last_status_details = details
                    return details

        except Exception as e:
            cat, code, hint = self._categorize_error(e)
            logger.debug("Graph health check failed: %s", e)
            details.update({
                "healthy": "false",
                "category": cat,
                "message": str(e),
                "hint": hint,
                "code": code,
            })
            self._last_status_details = details
            return details

    # PUBLIC_INTERFACE
    def is_healthy(self) -> bool:
        """
        Return True if the driver is connected and can execute a trivial query.

        Implementation details:
        - Use driver.verify_connectivity() to catch network/auth issues quickly.
        - Run a minimal query (RETURN 1 AS ok) with a short transaction timeout to ensure the server responds.
        """
        details = self.check_health()
        return details.get("healthy") == "true"

    # PUBLIC_INTERFACE
    def status(self) -> str:
        """
        Return the graph integration status:
        - "disabled": feature flag is OFF
        - "misconfigured": feature flag ON but NEO4J_* env vars missing
        - "healthy": connectivity ok and a trivial query responds
        - "unhealthy": configured but connectivity/auth fails
        """
        details = self.check_health()
        category = details.get("category")
        if category == "disabled":
            return "disabled"
        if category == "misconfigured":
            return "misconfigured"
        return "healthy" if details.get("healthy") == "true" else "unhealthy"

    # PUBLIC_INTERFACE
    def status_with_details(self) -> Tuple[str, Dict[str, str]]:
        """
        Return a tuple (status, details) where:
        - status: disabled|misconfigured|healthy|unhealthy
        - details: see check_health() return dict
        """
        return self.status(), self._last_status_details or {}

    # PUBLIC_INTERFACE
    @property
    def driver(self) -> Optional[Driver]:
        """Expose the underlying driver (or None if disabled/unavailable)."""
        return self._driver
