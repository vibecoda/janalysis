#!/usr/bin/env python
"""Demo script for filesystem blob storage backend."""

from __future__ import annotations

import tempfile
from io import BytesIO
from pathlib import Path

from jqsys.core.storage.backends import FilesystemBackend
from jqsys.core.storage.blob import BlobStorage


def demo_filesystem_storage():
    """Demonstrate filesystem blob storage operations."""
    print("=" * 60)
    print("Filesystem Blob Storage Demo")
    print("=" * 60)

    # Create a temporary directory for demo
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"\n📁 Using temporary directory: {tmpdir}")

        # Initialize filesystem backend
        backend = FilesystemBackend(base_path=tmpdir)
        blob_storage = BlobStorage(backend)

        # 1. Store some blobs
        print("\n1️⃣  Storing blobs...")

        # Store text blob
        text_data = b"Hello, filesystem storage!"
        key1 = "documents/hello.txt"
        etag1 = blob_storage.put(
            key1, text_data, content_type="text/plain", metadata={"author": "demo"}
        )
        print(f"   ✓ Stored '{key1}' (etag: {etag1})")

        # Store binary blob
        binary_data = bytes(range(256))
        key2 = "data/binary.bin"
        etag2 = blob_storage.put(key2, binary_data, content_type="application/octet-stream")
        print(f"   ✓ Stored '{key2}' (etag: {etag2})")

        # Store blob with nested path
        json_data = b'{"message": "nested storage", "value": 42}'
        key3 = "configs/app/settings.json"
        etag3 = blob_storage.put(key3, json_data, content_type="application/json")
        print(f"   ✓ Stored '{key3}' (etag: {etag3})")

        # 2. Retrieve blobs
        print("\n2️⃣  Retrieving blobs...")
        retrieved = blob_storage.get(key1)
        print(f"   ✓ Retrieved '{key1}': {retrieved.decode()}")

        # 3. Check existence
        print("\n3️⃣  Checking existence...")
        print(f"   ✓ '{key1}' exists: {blob_storage.exists(key1)}")
        print(f"   ✓ 'nonexistent.txt' exists: {blob_storage.exists('nonexistent.txt')}")

        # 4. Get metadata
        print("\n4️⃣  Getting metadata...")
        metadata = blob_storage.get_metadata(key1)
        print(f"   ✓ Key: {metadata.key}")
        print(f"   ✓ Size: {metadata.size} bytes")
        print(f"   ✓ Content-Type: {metadata.content_type}")
        print(f"   ✓ Last Modified: {metadata.last_modified}")
        print(f"   ✓ Custom Metadata: {metadata.custom_metadata}")

        # 5. List blobs
        print("\n5️⃣  Listing all blobs...")
        result = list(blob_storage.list())
        print(f"   ✓ Found {len(result)} blobs:")
        for blob in result:
            print(f"      - {blob.key} ({blob.size} bytes)")

        # 6. List blobs with prefix
        print("\n6️⃣  Listing blobs with prefix 'documents/'...")
        result = list(blob_storage.list(prefix="documents/"))
        print(f"   ✓ Found {len(result)} blobs:")
        for blob in result:
            print(f"      - {blob.key}")

        # 7. List prefixes (directory-like listing)
        print("\n7️⃣  Listing prefixes...")
        prefixes = blob_storage.list_prefixes()
        print(f"   ✓ Found {len(prefixes)} prefixes:")
        for prefix in prefixes:
            print(f"      - {prefix}")

        # 8. Copy blob
        print("\n8️⃣  Copying blob...")
        dest_key = "backup/hello-backup.txt"
        blob_storage.copy(key1, dest_key)
        print(f"   ✓ Copied '{key1}' -> '{dest_key}'")

        # 9. Get blob size
        print("\n9️⃣  Getting blob size...")
        size = blob_storage.get_size(key2)
        print(f"   ✓ Size of '{key2}': {size} bytes")

        # 10. Stream I/O
        print("\n🔟 Using stream I/O...")

        # Store from stream
        stream_data = BytesIO(b"Stream data example")
        stream_key = "streams/data.txt"
        blob_storage.put(stream_key, stream_data)
        print(f"   ✓ Stored '{stream_key}' from stream")

        # Retrieve as stream
        with blob_storage.get_stream(stream_key) as stream:
            content = stream.read()
            print(f"   ✓ Retrieved stream: {content.decode()}")

        # 11. Generate file URL
        print("\n1️⃣1️⃣  Generating file URL...")
        url = blob_storage.generate_presigned_url(key1)
        print(f"   ✓ File URL: {url}")

        # 12. Delete blobs
        print("\n1️⃣2️⃣  Deleting blobs...")
        blob_storage.delete(key1)
        print(f"   ✓ Deleted '{key1}'")

        # Delete multiple
        keys_to_delete = [key2, key3]
        results = blob_storage.delete_many(keys_to_delete)
        successful = sum(results.values())
        print(f"   ✓ Deleted {successful}/{len(keys_to_delete)} blobs")

        # 13. Final listing
        print("\n1️⃣3️⃣  Final blob listing...")
        result = list(blob_storage.list())
        print(f"   ✓ Remaining blobs: {len(result)}")
        for blob in result:
            print(f"      - {blob.key}")

        # Show physical directory structure
        print("\n📂 Physical directory structure:")
        base_path = Path(tmpdir)
        for path in sorted(base_path.rglob("*")):
            if path.is_file():
                rel_path = path.relative_to(base_path)
                size = path.stat().st_size
                print(f"   {rel_path} ({size} bytes)")

    print("\n✅ Demo completed!")


if __name__ == "__main__":
    demo_filesystem_storage()
