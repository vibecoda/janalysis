#!/usr/bin/env python3
"""Demo script for blob storage (MinIO) and object storage (MongoDB) APIs.

This script demonstrates the usage of both storage abstractions:
1. BlobStorage with MinIO backend for binary/file data
2. ObjectStorage with MongoDB backend for document data
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from jqsys.storage.backends.minio_backend import MinIOBackend
from jqsys.storage.backends.mongodb_backend import MongoDBBackend
from jqsys.storage.blob import BlobStorage
from jqsys.storage.object import ObjectStorage, SortOrder


def demo_blob_storage():
    """Demonstrate blob storage operations with MinIO."""
    print("\n" + "=" * 60)
    print("BLOB STORAGE DEMO (MinIO)")
    print("=" * 60)

    # Initialize MinIO backend
    backend = MinIOBackend(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="jqsys-demo",
        secure=False,  # Use HTTP for local testing
    )

    # Create blob storage instance
    blob_storage = BlobStorage(backend, bucket="jqsys-demo")

    # 1. Store a text file
    print("\n1. Storing text blob...")
    text_data = b"Hello, this is a test file!\nCreated at: " + datetime.now().isoformat().encode()
    etag = blob_storage.put(
        key="demo/test.txt",
        data=text_data,
        content_type="text/plain",
        metadata={"author": "demo_script", "version": "1.0"},
    )
    print(f"   Stored with ETag: {etag}")

    # 2. Store a JSON-like file
    print("\n2. Storing JSON blob...")
    import json

    json_data = json.dumps(
        {
            "timestamp": datetime.now().isoformat(),
            "data": {"values": [1, 2, 3, 4, 5]},
            "status": "success",
        },
        indent=2,
    ).encode()
    blob_storage.put(key="demo/data.json", data=json_data, content_type="application/json")
    print("   JSON blob stored")

    # 3. Retrieve a blob
    print("\n3. Retrieving blob...")
    retrieved = blob_storage.get("demo/test.txt")
    print(f"   Retrieved: {retrieved.decode()[:50]}...")

    # 4. Get metadata
    print("\n4. Getting blob metadata...")
    metadata = blob_storage.get_metadata("demo/test.txt")
    print(f"   Key: {metadata.key}")
    print(f"   Size: {metadata.size} bytes")
    print(f"   Content Type: {metadata.content_type}")
    print(f"   Last Modified: {metadata.last_modified}")
    print(f"   Custom Metadata: {metadata.custom_metadata}")

    # 5. List blobs
    print("\n5. Listing blobs with prefix 'demo/'...")
    for blob in blob_storage.list(prefix="demo/"):
        print(f"   - {blob.key} ({blob.size} bytes)")

    # 6. Generate presigned URL
    print("\n6. Generating presigned URL...")
    url = blob_storage.generate_presigned_url(key="demo/test.txt", expiration=timedelta(hours=1))
    print(f"   URL (valid for 1 hour): {url[:60]}...")

    # 7. Check existence
    print("\n7. Checking blob existence...")
    print(f"   demo/test.txt exists: {blob_storage.exists('demo/test.txt')}")
    print(f"   demo/nonexistent.txt exists: {blob_storage.exists('demo/nonexistent.txt')}")

    # 8. Copy blob
    print("\n8. Copying blob...")
    blob_storage.copy("demo/test.txt", "demo/test_copy.txt")
    print("   Blob copied to demo/test_copy.txt")

    # 9. Get blob size
    print("\n9. Getting blob size...")
    size = blob_storage.get_size("demo/test.txt")
    print(f"   Size: {size} bytes")

    # 10. Delete blobs
    print("\n10. Cleaning up...")
    blob_storage.delete("demo/test_copy.txt")
    result = blob_storage.delete_many(["demo/test.txt", "demo/data.json"])
    print(f"   Deleted: {sum(result.values())}/{len(result)} blobs")

    print("\n✓ Blob storage demo completed!")


def demo_object_storage():
    """Demonstrate object storage operations with MongoDB."""
    print("\n" + "=" * 60)
    print("OBJECT STORAGE DEMO (MongoDB)")
    print("=" * 60)

    # Initialize MongoDB backend
    backend = MongoDBBackend(
        host="localhost", port=27017, database="jqsys_demo", username="admin", password="password"
    )

    # Create object storage instance
    obj_storage = ObjectStorage(backend, database="jqsys_demo")

    collection = "demo_collection"

    # Clean up from previous runs
    if obj_storage.collection_exists(collection):
        obj_storage.drop_collection(collection)

    # 1. Insert a single document
    print("\n1. Inserting single document...")
    doc_id = obj_storage.insert_one(
        collection,
        {
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com",
            "created_at": datetime.now().isoformat(),
        },
    )
    print(f"   Inserted with ID: {doc_id}")

    # 2. Insert multiple documents
    print("\n2. Inserting multiple documents...")
    docs = [
        {"name": "Bob", "age": 25, "email": "bob@example.com", "department": "engineering"},
        {"name": "Charlie", "age": 35, "email": "charlie@example.com", "department": "sales"},
        {"name": "Diana", "age": 28, "email": "diana@example.com", "department": "engineering"},
        {"name": "Eve", "age": 32, "email": "eve@example.com", "department": "marketing"},
    ]
    ids = obj_storage.insert_many(collection, docs)
    print(f"   Inserted {len(ids)} documents")

    # 3. Find one document
    print("\n3. Finding single document...")
    doc = obj_storage.find_one(collection, filter={"name": "Alice"})
    print(f"   Found: {doc}")

    # 4. Find multiple documents
    print("\n4. Finding all engineering employees...")
    results = obj_storage.find_all(
        collection, filter={"department": "engineering"}, sort=[("age", SortOrder.ASCENDING)]
    )
    for doc in results:
        print(f"   - {doc['name']}, age {doc['age']}")

    # 5. Update a document
    print("\n5. Updating document...")
    matched, modified = obj_storage.update_one(
        collection,
        filter={"name": "Alice"},
        update={"$set": {"age": 31, "last_updated": datetime.now().isoformat()}},
    )
    print(f"   Matched: {matched}, Modified: {modified}")

    # 6. Count documents
    print("\n6. Counting documents...")
    total = obj_storage.count(collection)
    engineering = obj_storage.count(collection, filter={"department": "engineering"})
    print(f"   Total documents: {total}")
    print(f"   Engineering employees: {engineering}")

    # 7. Create an index
    print("\n7. Creating index on 'email' field...")
    index_name = obj_storage.create_index(
        collection, fields="email", unique=True, name="email_unique_idx"
    )
    print(f"   Created index: {index_name}")

    # 8. List indexes
    print("\n8. Listing indexes...")
    indexes = obj_storage.list_indexes(collection)
    for idx in indexes:
        print(f"   - {idx.get('name')}: {idx.get('key')}")

    # 9. Aggregation
    print("\n9. Running aggregation (average age by department)...")
    pipeline = [
        {"$match": {"department": {"$exists": True}}},
        {"$group": {"_id": "$department", "avg_age": {"$avg": "$age"}, "count": {"$sum": 1}}},
        {"$sort": {"avg_age": -1}},
    ]
    results = obj_storage.aggregate(collection, pipeline)
    for doc in results:
        print(f"   - {doc['_id']}: avg age = {doc['avg_age']:.1f}, count = {doc['count']}")

    # 10. Paginated query
    print("\n10. Testing pagination...")
    page1 = obj_storage.find_paginated(
        collection, sort=[("name", SortOrder.ASCENDING)], page_size=2
    )
    print(f"   Page 1: {len(page1.documents)} documents, has_more = {page1.has_more}")
    for doc in page1.documents:
        print(f"      - {doc['name']}")

    if page1.has_more:
        page2 = obj_storage.find_paginated(
            collection, sort=[("name", SortOrder.ASCENDING)], page_size=2, cursor=page1.cursor
        )
        print(f"   Page 2: {len(page2.documents)} documents, has_more = {page2.has_more}")
        for doc in page2.documents:
            print(f"      - {doc['name']}")

    # 11. Delete documents
    print("\n11. Deleting documents...")
    deleted = obj_storage.delete_many(collection, filter={"age": {"$lt": 30}})
    print(f"   Deleted {deleted} documents with age < 30")

    # 12. List collections
    print("\n12. Listing collections...")
    collections = obj_storage.list_collections()
    print(f"   Collections: {', '.join(collections)}")

    # 13. Cleanup
    print("\n13. Cleaning up...")
    obj_storage.drop_collection(collection)
    print("   Collection dropped")

    print("\n✓ Object storage demo completed!")


def main():
    """Run both demos."""
    print("\n" + "=" * 60)
    print("STORAGE APIs DEMONSTRATION")
    print("=" * 60)
    print("\nThis demo showcases two storage abstractions:")
    print("1. Blob Storage (MinIO) - for binary/file data")
    print("2. Object Storage (MongoDB) - for document/JSON data")
    print("\nMake sure Docker containers are running:")
    print("  docker compose up -d")

    try:
        # Run blob storage demo
        demo_blob_storage()

        # Run object storage demo
        demo_object_storage()

        print("\n" + "=" * 60)
        print("ALL DEMOS COMPLETED SUCCESSFULLY!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
