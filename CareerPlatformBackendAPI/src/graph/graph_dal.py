import logging
from typing import Any, Dict, List, Optional

from neo4j import Driver

logger = logging.getLogger(__name__)


class GraphDAL:
    """
    Data Access Layer for Neo4j interactions:
    - Roles
    - Competencies
    - REQUIRES relationships (Role -> Competency)
    - ADJACENT_TO relationships (Role <-> Role)
    """

    def __init__(self, driver: Driver) -> None:
        self._driver = driver

    # PUBLIC_INTERFACE
    def upsert_role(
        self,
        role_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        source: Optional[str] = None,
        version: Optional[str] = None,
    ) -> None:
        """Create or update a Role node by id."""
        metadata = metadata or {}
        cypher = """
        MERGE (r:Role {id: $id})
        SET r.name = COALESCE($name, r.name),
            r.description = COALESCE($description, r.description),
            r.metadata = COALESCE($metadata, r.metadata),
            r.source = COALESCE($source, r.source),
            r.version = COALESCE($version, r.version),
            r.updatedAt = timestamp()
        """
        with self._driver.session() as session:
            session.run(
                cypher,
                id=role_id,
                name=name,
                description=description,
                metadata=metadata,
                source=source,
                version=version,
            )

    # PUBLIC_INTERFACE
    def get_roles(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Retrieve a list of roles.

        Returns a list of dictionaries with fields: id, name, description, metadata, source, version.
        """
        cypher = """
        MATCH (r:Role)
        RETURN r.id AS id,
               r.name AS name,
               r.description AS description,
               r.metadata AS metadata,
               r.source AS source,
               r.version AS version
        ORDER BY r.id
        LIMIT $limit
        """
        with self._driver.session() as session:
            records = session.run(cypher, limit=limit)
            return [dict(r) for r in records]

    # PUBLIC_INTERFACE
    def get_role_by_id(self, role_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a single role by id.

        Returns a dict with fields: id, name, description, metadata, source, version, or None if not found.
        """
        cypher = """
        MATCH (r:Role {id: $id})
        RETURN r.id AS id,
               r.name AS name,
               r.description AS description,
               r.metadata AS metadata,
               r.source AS source,
               r.version AS version
        """
        with self._driver.session() as session:
            rec = session.run(cypher, id=role_id).single()
            return dict(rec) if rec else None

    # PUBLIC_INTERFACE
    def upsert_competency(
        self,
        competency_id: str,
        name: Optional[str] = None,
        definition: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        source: Optional[str] = None,
        version: Optional[str] = None,
    ) -> None:
        """Create or update a Competency node by id."""
        metadata = metadata or {}
        cypher = """
        MERGE (c:Competency {id: $id})
        SET c.name = COALESCE($name, c.name),
            c.definition = COALESCE($definition, c.definition),
            c.metadata = COALESCE($metadata, c.metadata),
            c.source = COALESCE($source, c.source),
            c.version = COALESCE($version, c.version),
            c.updatedAt = timestamp()
        """
        with self._driver.session() as session:
            session.run(
                cypher,
                id=competency_id,
                name=name,
                definition=definition,
                metadata=metadata,
                source=source,
                version=version,
            )

    # PUBLIC_INTERFACE
    def upsert_requires_edge(
        self,
        role_id: str,
        competency_id: str,
        required_level: Optional[str] = None,
        version: Optional[str] = None,
        source: Optional[str] = None,
        valid_from: Optional[str] = None,
        valid_to: Optional[str] = None,
    ) -> None:
        """
        Create or update a REQUIRES relationship from Role -> Competency.

        valid_from/to are ISO-8601 strings if provided.
        """
        cypher = """
        MATCH (r:Role {id: $role_id})
        MATCH (c:Competency {id: $competency_id})
        MERGE (r)-[rel:REQUIRES]->(c)
        SET rel.requiredLevel = COALESCE($required_level, rel.requiredLevel),
            rel.version = COALESCE($version, rel.version),
            rel.source = COALESCE($source, rel.source),
            rel.validFrom = COALESCE($valid_from, rel.validFrom),
            rel.validTo = COALESCE($valid_to, rel.validTo),
            rel.updatedAt = timestamp()
        """
        with self._driver.session() as session:
            session.run(
                cypher,
                role_id=role_id,
                competency_id=competency_id,
                required_level=required_level,
                version=version,
                source=source,
                valid_from=valid_from,
                valid_to=valid_to,
            )

    # PUBLIC_INTERFACE
    def upsert_adjacency(
        self,
        role_a: str,
        role_b: str,
        score: Optional[float] = None,
        rationale: Optional[str] = None,
        version: Optional[str] = None,
        source: Optional[str] = None,
        bidirectional: bool = True,
    ) -> None:
        """
        Create or update an ADJACENT_TO relationship between Role A and Role B.
        Optionally create both A->B and B->A to model undirected adjacency.
        """
        cypher = """
        MERGE (a:Role {id: $role_a})
        MERGE (b:Role {id: $role_b})
        MERGE (a)-[adj:ADJACENT_TO]->(b)
        SET adj.score = COALESCE($score, adj.score),
            adj.rationale = COALESCE($rationale, adj.rationale),
            adj.version = COALESCE($version, adj.version),
            adj.source = COALESCE($source, adj.source),
            adj.updatedAt = timestamp()
        """
        with self._driver.session() as session:
            session.run(
                cypher,
                role_a=role_a,
                role_b=role_b,
                score=score,
                rationale=rationale,
                version=version,
                source=source,
            )
            if bidirectional:
                session.run(
                    cypher,
                    role_a=role_b,
                    role_b=role_a,
                    score=score,
                    rationale=rationale,
                    version=version,
                    source=source,
                )

    # PUBLIC_INTERFACE
    def get_role_adjacency(
        self,
        user_id: Optional[str] = None,
        current_role_id: Optional[str] = None,
        target_role_id: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve adjacent/suggested roles for a given current role.
        If target_role_id is provided, prefer roles closer to the target via a two-hop path.
        Returns a list of dicts with id, name, description, score, and rationale.
        """
        # If both provided, bias results shared by both paths
        if current_role_id and target_role_id:
            cypher = """
            MATCH (cur:Role {id: $current_role_id})- [adj1:ADJACENT_TO]->(sug:Role)
            OPTIONAL MATCH (sug)-[adj2:ADJACENT_TO]->(tgt:Role {id: $target_role_id})
            WITH sug, adj1, adj2
            RETURN sug.id AS id,
                   sug.name AS name,
                   sug.description AS description,
                   COALESCE(adj1.score, 0) + COALESCE(adj2.score, 0) AS score,
                   COALESCE(adj1.rationale, '') + CASE WHEN adj2 IS NULL OR adj2.rationale IS NULL THEN '' ELSE ' | ' + adj2.rationale END AS rationale
            ORDER BY score DESC
            LIMIT $limit
            """
            params = {"current_role_id": current_role_id, "target_role_id": target_role_id, "limit": limit}
        elif current_role_id:
            cypher = """
            MATCH (cur:Role {id: $current_role_id})- [adj:ADJACENT_TO]->(sug:Role)
            RETURN sug.id AS id,
                   sug.name AS name,
                   sug.description AS description,
                   adj.score AS score,
                   adj.rationale AS rationale
            ORDER BY adj.score DESC
            LIMIT $limit
            """
            params = {"current_role_id": current_role_id, "limit": limit}
        else:
            # Fallback: just return top roles by degree/score
            cypher = """
            MATCH (a:Role)-[adj:ADJACENT_TO]->(b:Role)
            WITH b, sum(COALESCE(adj.score, 0)) AS s
            RETURN b.id AS id, b.name AS name, b.description AS description, s AS score, '' AS rationale
            ORDER BY s DESC
            LIMIT $limit
            """
            params = {"limit": limit}

        with self._driver.session() as session:
            records = session.run(cypher, **params)
            return [dict(r) for r in records]
