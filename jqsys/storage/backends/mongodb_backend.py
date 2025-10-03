"""MongoDB backend implementation for object storage."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

from bson import ObjectId
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import (
    ConnectionFailure,
    OperationFailure,
)
from pymongo.errors import (
    DuplicateKeyError as MongoDuplicateKeyError,
)

from ..object import (
    DuplicateKeyError,
    IndexDefinition,
    InvalidQueryError,
    ObjectStorageBackend,
    ObjectStorageConnectionError,
    ObjectStorageError,
    QueryResult,
    SortOrder,
)

logger = logging.getLogger(__name__)


class MongoDBBackend(ObjectStorageBackend):
    """MongoDB implementation of object storage backend."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        database: str = "jqsys",
        username: str | None = None,
        password: str | None = None,
        **kwargs,
    ):
        """Initialize MongoDB backend.

        Args:
            host: MongoDB host
            port: MongoDB port
            database: Database name
            username: Optional username for authentication
            password: Optional password for authentication
            **kwargs: Additional arguments passed to MongoClient
        """
        self._database_name = database

        try:
            # Build connection string
            if username and password:
                uri = f"mongodb://{username}:{password}@{host}:{port}/"
            else:
                uri = f"mongodb://{host}:{port}/"

            self._client = MongoClient(uri, **kwargs)
            self._db = self._client[database]

            # Test connection
            self._client.admin.command("ping")
            logger.info(f"Connected to MongoDB database: {database}")

        except ConnectionFailure as e:
            raise ObjectStorageConnectionError(f"Failed to connect to MongoDB: {e}")

    def _get_collection(self, collection: str):
        """Get a collection object."""
        return self._db[collection]

    def _convert_id(self, document: dict[str, Any]) -> dict[str, Any]:
        """Convert MongoDB ObjectId to string."""
        if document and "_id" in document:
            document["_id"] = str(document["_id"])
        return document

    def _prepare_filter(self, filter: dict[str, Any] | None) -> dict[str, Any]:
        """Prepare filter, converting string IDs to ObjectId if needed."""
        if not filter:
            return {}

        # Convert _id string to ObjectId
        if "_id" in filter and isinstance(filter["_id"], str):
            try:
                filter["_id"] = ObjectId(filter["_id"])
            except Exception:
                pass  # Keep as string if not a valid ObjectId

        return filter

    def _convert_sort(
        self, sort: list[tuple[str, SortOrder]] | None
    ) -> list[tuple[str, int]] | None:
        """Convert SortOrder enum to pymongo constants."""
        if not sort:
            return None

        return [
            (field, ASCENDING if order == SortOrder.ASCENDING else DESCENDING)
            for field, order in sort
        ]

    def insert_one(self, collection: str, document: dict[str, Any]) -> str:
        """Insert a single document into MongoDB."""
        try:
            coll = self._get_collection(collection)
            result = coll.insert_one(document)
            logger.debug(f"Inserted document into {collection}: {result.inserted_id}")
            return str(result.inserted_id)

        except MongoDuplicateKeyError as e:
            raise DuplicateKeyError(f"Duplicate key error: {e}")
        except Exception as e:
            raise ObjectStorageError(f"Failed to insert document: {e}")

    def insert_many(self, collection: str, documents: list[dict[str, Any]]) -> list[str]:
        """Insert multiple documents into MongoDB."""
        try:
            coll = self._get_collection(collection)
            result = coll.insert_many(documents)
            logger.debug(f"Inserted {len(result.inserted_ids)} documents into {collection}")
            return [str(id) for id in result.inserted_ids]

        except MongoDuplicateKeyError as e:
            raise DuplicateKeyError(f"Duplicate key error: {e}")
        except Exception as e:
            raise ObjectStorageError(f"Failed to insert documents: {e}")

    def find_one(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, bool] | None = None,
    ) -> dict[str, Any] | None:
        """Find a single document in MongoDB."""
        try:
            coll = self._get_collection(collection)
            filter = self._prepare_filter(filter)
            result = coll.find_one(filter, projection)
            return self._convert_id(result) if result else None

        except Exception as e:
            raise ObjectStorageError(f"Failed to find document: {e}")

    def find(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, bool] | None = None,
        sort: list[tuple[str, SortOrder]] | None = None,
        limit: int | None = None,
        skip: int = 0,
    ) -> Iterator[dict[str, Any]]:
        """Find multiple documents in MongoDB."""
        try:
            coll = self._get_collection(collection)
            filter = self._prepare_filter(filter)
            sort_spec = self._convert_sort(sort)

            cursor = coll.find(filter, projection)

            if sort_spec:
                cursor = cursor.sort(sort_spec)
            if skip > 0:
                cursor = cursor.skip(skip)
            if limit:
                cursor = cursor.limit(limit)

            for doc in cursor:
                yield self._convert_id(doc)

        except Exception as e:
            raise ObjectStorageError(f"Failed to find documents: {e}")

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

        Uses _id-based pagination for efficiency.
        """
        try:
            coll = self._get_collection(collection)
            filter = self._prepare_filter(filter) if filter else {}

            # Apply cursor (last _id from previous page)
            if cursor:
                try:
                    last_id = ObjectId(cursor)
                    filter["_id"] = {"$gt": last_id}
                except Exception:
                    raise InvalidQueryError(f"Invalid cursor: {cursor}")

            # Query with limit + 1 to check if there are more results
            sort_spec = self._convert_sort(sort) or [("_id", ASCENDING)]
            cursor_result = coll.find(filter, projection).sort(sort_spec).limit(limit + 1)

            documents = [self._convert_id(doc) for doc in cursor_result]

            # Check if there are more results
            has_more = len(documents) > limit
            if has_more:
                documents = documents[:limit]

            # Get next cursor (last document's _id)
            next_cursor = documents[-1]["_id"] if documents and has_more else None

            return QueryResult(
                documents=documents, count=len(documents), has_more=has_more, cursor=next_cursor
            )

        except InvalidQueryError:
            raise
        except Exception as e:
            raise ObjectStorageError(f"Failed to find documents with cursor: {e}")

    def update_one(
        self, collection: str, filter: dict[str, Any], update: dict[str, Any], upsert: bool = False
    ) -> tuple[int, int]:
        """Update a single document in MongoDB."""
        try:
            coll = self._get_collection(collection)
            filter = self._prepare_filter(filter)
            result = coll.update_one(filter, update, upsert=upsert)

            logger.debug(
                f"Updated document in {collection}: "
                f"matched={result.matched_count}, modified={result.modified_count}"
            )
            return result.matched_count, result.modified_count

        except MongoDuplicateKeyError as e:
            raise DuplicateKeyError(f"Duplicate key error: {e}")
        except OperationFailure as e:
            raise InvalidQueryError(f"Invalid update operation: {e}")
        except Exception as e:
            raise ObjectStorageError(f"Failed to update document: {e}")

    def update_many(
        self, collection: str, filter: dict[str, Any], update: dict[str, Any]
    ) -> tuple[int, int]:
        """Update multiple documents in MongoDB."""
        try:
            coll = self._get_collection(collection)
            filter = self._prepare_filter(filter)
            result = coll.update_many(filter, update)

            logger.debug(
                f"Updated documents in {collection}: "
                f"matched={result.matched_count}, modified={result.modified_count}"
            )
            return result.matched_count, result.modified_count

        except MongoDuplicateKeyError as e:
            raise DuplicateKeyError(f"Duplicate key error: {e}")
        except OperationFailure as e:
            raise InvalidQueryError(f"Invalid update operation: {e}")
        except Exception as e:
            raise ObjectStorageError(f"Failed to update documents: {e}")

    def replace_one(
        self,
        collection: str,
        filter: dict[str, Any],
        document: dict[str, Any],
        upsert: bool = False,
    ) -> tuple[int, int]:
        """Replace a single document in MongoDB."""
        try:
            coll = self._get_collection(collection)
            filter = self._prepare_filter(filter)
            result = coll.replace_one(filter, document, upsert=upsert)

            logger.debug(
                f"Replaced document in {collection}: "
                f"matched={result.matched_count}, modified={result.modified_count}"
            )
            return result.matched_count, result.modified_count

        except MongoDuplicateKeyError as e:
            raise DuplicateKeyError(f"Duplicate key error: {e}")
        except Exception as e:
            raise ObjectStorageError(f"Failed to replace document: {e}")

    def delete_one(self, collection: str, filter: dict[str, Any]) -> int:
        """Delete a single document from MongoDB."""
        try:
            coll = self._get_collection(collection)
            filter = self._prepare_filter(filter)
            result = coll.delete_one(filter)

            logger.debug(f"Deleted document from {collection}: count={result.deleted_count}")
            return result.deleted_count

        except Exception as e:
            raise ObjectStorageError(f"Failed to delete document: {e}")

    def delete_many(self, collection: str, filter: dict[str, Any]) -> int:
        """Delete multiple documents from MongoDB."""
        try:
            coll = self._get_collection(collection)
            filter = self._prepare_filter(filter)
            result = coll.delete_many(filter)

            logger.debug(f"Deleted documents from {collection}: count={result.deleted_count}")
            return result.deleted_count

        except Exception as e:
            raise ObjectStorageError(f"Failed to delete documents: {e}")

    def count(self, collection: str, filter: dict[str, Any] | None = None) -> int:
        """Count documents in MongoDB."""
        try:
            coll = self._get_collection(collection)
            filter = self._prepare_filter(filter)
            return coll.count_documents(filter or {})

        except Exception as e:
            raise ObjectStorageError(f"Failed to count documents: {e}")

    def aggregate(
        self, collection: str, pipeline: list[dict[str, Any]]
    ) -> Iterator[dict[str, Any]]:
        """Run an aggregation pipeline in MongoDB."""
        try:
            coll = self._get_collection(collection)
            cursor = coll.aggregate(pipeline)

            for doc in cursor:
                yield self._convert_id(doc)

        except OperationFailure as e:
            raise InvalidQueryError(f"Invalid aggregation pipeline: {e}")
        except Exception as e:
            raise ObjectStorageError(f"Failed to run aggregation: {e}")

    def create_index(self, collection: str, index: IndexDefinition) -> str:
        """Create an index in MongoDB."""
        try:
            coll = self._get_collection(collection)

            # Convert fields to pymongo format
            keys = [
                (field, ASCENDING if order == SortOrder.ASCENDING else DESCENDING)
                for field, order in index.fields
            ]

            index_name = coll.create_index(
                keys, unique=index.unique, sparse=index.sparse, name=index.name
            )

            logger.info(f"Created index on {collection}: {index_name}")
            return index_name

        except Exception as e:
            raise ObjectStorageError(f"Failed to create index: {e}")

    def drop_index(self, collection: str, index_name: str) -> None:
        """Drop an index from MongoDB."""
        try:
            coll = self._get_collection(collection)
            coll.drop_index(index_name)

            logger.info(f"Dropped index from {collection}: {index_name}")

        except OperationFailure as e:
            if "index not found" in str(e).lower():
                raise ObjectStorageError(f"Index not found: {index_name}")
            raise ObjectStorageError(f"Failed to drop index: {e}")
        except Exception as e:
            raise ObjectStorageError(f"Failed to drop index: {e}")

    def list_indexes(self, collection: str) -> list[dict[str, Any]]:
        """List all indexes in a MongoDB collection."""
        try:
            coll = self._get_collection(collection)
            return list(coll.list_indexes())

        except Exception as e:
            raise ObjectStorageError(f"Failed to list indexes: {e}")

    def drop_collection(self, collection: str) -> None:
        """Drop a collection from MongoDB."""
        try:
            self._db.drop_collection(collection)
            logger.info(f"Dropped collection: {collection}")

        except Exception as e:
            raise ObjectStorageError(f"Failed to drop collection: {e}")

    def collection_exists(self, collection: str) -> bool:
        """Check if a collection exists in MongoDB."""
        try:
            return collection in self._db.list_collection_names()

        except Exception as e:
            raise ObjectStorageError(f"Failed to check collection existence: {e}")

    def list_collections(self) -> list[str]:
        """List all collections in the MongoDB database."""
        try:
            return self._db.list_collection_names()

        except Exception as e:
            raise ObjectStorageError(f"Failed to list collections: {e}")
