"""Custom exceptions for WorldOfTaxonomy."""


class WorldOfTaxonomyError(Exception):
    """Base exception for all WorldOfTaxonomy errors."""


class NodeNotFoundError(WorldOfTaxonomyError):
    """Raised when a classification node is not found."""

    def __init__(self, system_id: str, code: str):
        self.system_id = system_id
        self.code = code
        super().__init__(f"Node '{code}' not found in system '{system_id}'")


class SystemNotFoundError(WorldOfTaxonomyError):
    """Raised when a classification system is not found."""

    def __init__(self, system_id: str):
        self.system_id = system_id
        super().__init__(f"Classification system '{system_id}' not found")


class IngestionError(WorldOfTaxonomyError):
    """Raised when data ingestion fails."""

    def __init__(self, source: str, reason: str):
        self.source = source
        self.reason = reason
        super().__init__(f"Ingestion failed for '{source}': {reason}")


class DatabaseError(WorldOfTaxonomyError):
    """Raised when a database operation fails."""
