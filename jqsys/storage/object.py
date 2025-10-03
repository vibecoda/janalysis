"""Object storage abstraction for document/JSON data.

Provides a high-level interface for document database operations with support
for various backends (MongoDB, PostgreSQL, Redis, DocumentDB, etc.).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum
from typing import Any


class SortOrder(Enum):
    """Sort order for queries."""

    ASCENDING = 1
    DESCENDING = -1


@dataclass
class IndexDefinition:
    """Definition for a database index."""

    fields: list[tuple[str, SortOrder]]
    unique: bool = False
    sparse: bool = False
    name: str | None = None


@dataclass
class QueryResult:
    """Result from a query operation."""

    documents: list[dict[str, Any]]
    count: int
    has_more: bool
    cursor: str | None


class ObjectStorageBackend(ABC):
    """Abstract base class for object/document storage backends.

    This interface provides document database operations for storing and
    querying structured data (JSON documents, metadata, etc.).
    """

    @abstractmethod
    def insert_one(self, collection: str, document: dict[str, Any]) -> str:
        """Insert a single document.

        Args:
            collection: Collection/table name
            document: Document to insert

        Returns:
            ID of the inserted document
        """
        pass

    @abstractmethod
    def insert_many(self, collection: str, documents: list[dict[str, Any]]) -> list[str]:
        """Insert multiple documents.

        Args:
            collection: Collection/table name
            documents: List of documents to insert

        Returns:
            List of inserted document IDs
        """
        pass

    @abstractmethod
    def find_one(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, bool] | None = None,
    ) -> dict[str, Any] | None:
        """Find a single document.

        Args:
            collection: Collection/table name
            filter: Query filter (MongoDB-style)
            projection: Fields to include/exclude

        Returns:
            Document if found, None otherwise
        """
        pass

    @abstractmethod
    def find(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, bool] | None = None,
        sort: list[tuple[str, SortOrder]] | None = None,
        limit: int | None = None,
        skip: int = 0,
    ) -> Iterator[dict[str, Any]]:
        """Find multiple documents.

        Args:
            collection: Collection/table name
            filter: Query filter (MongoDB-style)
            projection: Fields to include/exclude
            sort: Sort specification
            limit: Maximum number of documents to return
            skip: Number of documents to skip

        Yields:
            Matching documents
        """
        pass

    @abstractmethod
    def find_with_cursor(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, bool] | None = None,
        sort: list[tuple[str, SortOrder]] | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> QueryResult:
        """Find documents with cursor-based pagination.

        Args:
            collection: Collection/table name
            filter: Query filter
            projection: Fields to include/exclude
            sort: Sort specification
            limit: Maximum number of documents to return
            cursor: Pagination cursor from previous query

        Returns:
            Query result with documents and pagination info
        """
        pass

    @abstractmethod
    def update_one(
        self, collection: str, filter: dict[str, Any], update: dict[str, Any], upsert: bool = False
    ) -> tuple[int, int]:
        """Update a single document.

        Args:
            collection: Collection/table name
            filter: Query filter to match document
            update: Update operations (MongoDB-style: $set, $inc, etc.)
            upsert: Insert if not found

        Returns:
            Tuple of (matched_count, modified_count)
        """
        pass

    @abstractmethod
    def update_many(
        self, collection: str, filter: dict[str, Any], update: dict[str, Any]
    ) -> tuple[int, int]:
        """Update multiple documents.

        Args:
            collection: Collection/table name
            filter: Query filter to match documents
            update: Update operations

        Returns:
            Tuple of (matched_count, modified_count)
        """
        pass

    @abstractmethod
    def replace_one(
        self,
        collection: str,
        filter: dict[str, Any],
        document: dict[str, Any],
        upsert: bool = False,
    ) -> tuple[int, int]:
        """Replace a single document.

        Args:
            collection: Collection/table name
            filter: Query filter to match document
            document: New document to replace with
            upsert: Insert if not found

        Returns:
            Tuple of (matched_count, modified_count)
        """
        pass

    @abstractmethod
    def delete_one(self, collection: str, filter: dict[str, Any]) -> int:
        """Delete a single document.

        Args:
            collection: Collection/table name
            filter: Query filter to match document

        Returns:
            Number of documents deleted (0 or 1)
        """
        pass

    @abstractmethod
    def delete_many(self, collection: str, filter: dict[str, Any]) -> int:
        """Delete multiple documents.

        Args:
            collection: Collection/table name
            filter: Query filter to match documents

        Returns:
            Number of documents deleted
        """
        pass

    @abstractmethod
    def count(self, collection: str, filter: dict[str, Any] | None = None) -> int:
        """Count documents matching filter.

        Args:
            collection: Collection/table name
            filter: Query filter

        Returns:
            Number of matching documents
        """
        pass

    @abstractmethod
    def aggregate(
        self, collection: str, pipeline: list[dict[str, Any]]
    ) -> Iterator[dict[str, Any]]:
        """Run an aggregation pipeline.

        Args:
            collection: Collection/table name
            pipeline: Aggregation pipeline stages (MongoDB-style)

        Yields:
            Result documents
        """
        pass

    @abstractmethod
    def create_index(self, collection: str, index: IndexDefinition) -> str:
        """Create an index on a collection.

        Args:
            collection: Collection/table name
            index: Index definition

        Returns:
            Index name
        """
        pass

    @abstractmethod
    def drop_index(self, collection: str, index_name: str) -> None:
        """Drop an index from a collection.

        Args:
            collection: Collection/table name
            index_name: Name of index to drop
        """
        pass

    @abstractmethod
    def list_indexes(self, collection: str) -> list[dict[str, Any]]:
        """List all indexes on a collection.

        Args:
            collection: Collection/table name

        Returns:
            List of index definitions
        """
        pass

    @abstractmethod
    def drop_collection(self, collection: str) -> None:
        """Drop an entire collection.

        Args:
            collection: Collection/table name
        """
        pass

    @abstractmethod
    def collection_exists(self, collection: str) -> bool:
        """Check if a collection exists.

        Args:
            collection: Collection/table name

        Returns:
            True if collection exists
        """
        pass

    @abstractmethod
    def list_collections(self) -> list[str]:
        """List all collections in the database.

        Returns:
            List of collection names
        """
        pass


class ObjectStorage:
    """High-level object storage interface with pluggable backends."""

    def __init__(self, backend: ObjectStorageBackend, database: str):
        """Initialize object storage.

        Args:
            backend: Storage backend implementation
            database: Database/namespace to use
        """
        self._backend = backend
        self._database = database

    def insert_one(self, collection: str, document: dict[str, Any]) -> str:
        """Insert a single document."""
        return self._backend.insert_one(collection, document)

    def insert_many(self, collection: str, documents: list[dict[str, Any]]) -> list[str]:
        """Insert multiple documents."""
        return self._backend.insert_many(collection, documents)

    def find_one(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, bool] | None = None,
    ) -> dict[str, Any] | None:
        """Find a single document."""
        return self._backend.find_one(collection, filter, projection)

    def find(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, bool] | None = None,
        sort: list[tuple[str, SortOrder]] | None = None,
        limit: int | None = None,
        skip: int = 0,
    ) -> Iterator[dict[str, Any]]:
        """Find multiple documents."""
        return self._backend.find(collection, filter, projection, sort, limit, skip)

    def find_all(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, bool] | None = None,
        sort: list[tuple[str, SortOrder]] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Find all matching documents and return as a list."""
        return list(self.find(collection, filter, projection, sort, limit))

    def find_paginated(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, bool] | None = None,
        sort: list[tuple[str, SortOrder]] | None = None,
        page_size: int = 100,
        cursor: str | None = None,
    ) -> QueryResult:
        """Find documents with cursor-based pagination."""
        return self._backend.find_with_cursor(
            collection, filter, projection, sort, page_size, cursor
        )

    def update_one(
        self, collection: str, filter: dict[str, Any], update: dict[str, Any], upsert: bool = False
    ) -> tuple[int, int]:
        """Update a single document."""
        return self._backend.update_one(collection, filter, update, upsert)

    def update_many(
        self, collection: str, filter: dict[str, Any], update: dict[str, Any]
    ) -> tuple[int, int]:
        """Update multiple documents."""
        return self._backend.update_many(collection, filter, update)

    def replace_one(
        self,
        collection: str,
        filter: dict[str, Any],
        document: dict[str, Any],
        upsert: bool = False,
    ) -> tuple[int, int]:
        """Replace a single document."""
        return self._backend.replace_one(collection, filter, document, upsert)

    def delete_one(self, collection: str, filter: dict[str, Any]) -> int:
        """Delete a single document."""
        return self._backend.delete_one(collection, filter)

    def delete_many(self, collection: str, filter: dict[str, Any]) -> int:
        """Delete multiple documents."""
        return self._backend.delete_many(collection, filter)

    def count(self, collection: str, filter: dict[str, Any] | None = None) -> int:
        """Count documents matching filter."""
        return self._backend.count(collection, filter)

    def aggregate(self, collection: str, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run an aggregation pipeline and return results as a list."""
        return list(self._backend.aggregate(collection, pipeline))

    def create_index(
        self,
        collection: str,
        fields: str | list[str] | list[tuple[str, SortOrder]],
        unique: bool = False,
        sparse: bool = False,
        name: str | None = None,
    ) -> str:
        """Create an index on a collection.

        Args:
            collection: Collection/table name
            fields: Field name, list of fields, or list of (field, order) tuples
            unique: Whether index should enforce uniqueness
            sparse: Whether to index documents missing the field
            name: Optional index name

        Returns:
            Index name
        """
        # Normalize fields to list of tuples
        if isinstance(fields, str):
            field_list = [(fields, SortOrder.ASCENDING)]
        elif isinstance(fields, list) and all(isinstance(f, str) for f in fields):
            field_list = [(f, SortOrder.ASCENDING) for f in fields]
        else:
            field_list = fields

        index = IndexDefinition(fields=field_list, unique=unique, sparse=sparse, name=name)
        return self._backend.create_index(collection, index)

    def drop_index(self, collection: str, index_name: str) -> None:
        """Drop an index from a collection."""
        self._backend.drop_index(collection, index_name)

    def list_indexes(self, collection: str) -> list[dict[str, Any]]:
        """List all indexes on a collection."""
        return self._backend.list_indexes(collection)

    def drop_collection(self, collection: str) -> None:
        """Drop an entire collection."""
        self._backend.drop_collection(collection)

    def collection_exists(self, collection: str) -> bool:
        """Check if a collection exists."""
        return self._backend.collection_exists(collection)

    def list_collections(self) -> list[str]:
        """List all collections in the database."""
        return self._backend.list_collections()


# Custom exceptions


class ObjectStorageError(Exception):
    """Base exception for object storage errors."""

    pass


class DocumentNotFoundError(ObjectStorageError):
    """Raised when a document is not found."""

    pass


class DuplicateKeyError(ObjectStorageError):
    """Raised when inserting a document with a duplicate unique key."""

    pass


class ObjectStorageConnectionError(ObjectStorageError):
    """Raised when connection to storage backend fails."""

    pass


class InvalidQueryError(ObjectStorageError):
    """Raised when a query is invalid."""

    pass
