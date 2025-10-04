#!/usr/bin/env python
"""Demo script for named backend registry system."""

from __future__ import annotations

import tempfile
from pathlib import Path

from jqsys.storage import BlobBackendRegistry, BlobStorage


def demo_basic_usage():
    """Demonstrate basic usage of named backends."""
    print("=" * 70)
    print("Demo 1: Basic Named Backend Usage")
    print("=" * 70)

    # Use named backend directly
    print("\n1️⃣  Using BlobStorage.from_name() with 'dev' backend:")
    storage = BlobStorage.from_name("dev")

    # Store some data
    storage.put("hello.txt", b"Hello from named backend!")
    print("   ✓ Stored 'hello.txt'")

    # Retrieve data
    data = storage.get("hello.txt")
    print(f"   ✓ Retrieved: {data.decode()}")

    # Check where it was stored
    print(f"   ✓ Backend type: {type(storage._backend).__name__}")

    # List blobs
    blobs = list(storage.list())
    print(f"   ✓ Total blobs: {len(blobs)}")


def demo_namespaced_backends():
    """Demonstrate hierarchical namespacing with dot notation."""
    print("\n" + "=" * 70)
    print("Demo 2: Hierarchical Namespacing")
    print("=" * 70)

    # Create storage instances with different namespaces
    print("\n1️⃣  Creating storage instances with namespaces:")

    images_storage = BlobStorage.from_name("dev.images")
    print("   ✓ Created 'dev.images' storage")

    thumbnails_storage = BlobStorage.from_name("dev.images.thumbnails")
    print("   ✓ Created 'dev.images.thumbnails' storage")

    documents_storage = BlobStorage.from_name("dev.documents")
    print("   ✓ Created 'dev.documents' storage")

    # Store data in different namespaces
    print("\n2️⃣  Storing data in different namespaces:")

    images_storage.put("photo.jpg", b"photo data")
    print("   ✓ Stored photo.jpg in 'dev.images'")

    thumbnails_storage.put("photo_thumb.jpg", b"thumbnail data")
    print("   ✓ Stored photo_thumb.jpg in 'dev.images.thumbnails'")

    documents_storage.put("report.pdf", b"report data")
    print("   ✓ Stored report.pdf in 'dev.documents'")

    # List blobs in each namespace
    print("\n3️⃣  Listing blobs by namespace:")

    images_blobs = list(images_storage.list())
    print(f"   ✓ 'dev.images': {len(images_blobs)} blobs")
    for blob in images_blobs:
        print(f"      - {blob.key}")

    thumbnails_blobs = list(thumbnails_storage.list())
    print(f"   ✓ 'dev.images.thumbnails': {len(thumbnails_blobs)} blobs")
    for blob in thumbnails_blobs:
        print(f"      - {blob.key}")

    documents_blobs = list(documents_storage.list())
    print(f"   ✓ 'dev.documents': {len(documents_blobs)} blobs")
    for blob in documents_blobs:
        print(f"      - {blob.key}")

    # Show how base storage sees all namespaces
    print("\n4️⃣  Base 'dev' storage sees everything:")
    base_storage = BlobStorage.from_name("dev")
    all_blobs = list(base_storage.list())
    print(f"   ✓ Total blobs in 'dev': {len(all_blobs)}")
    for blob in all_blobs:
        print(f"      - {blob.key}")


def demo_custom_registry():
    """Demonstrate custom registry with programmatic configuration."""
    print("\n" + "=" * 70)
    print("Demo 3: Custom Registry Configuration")
    print("=" * 70)

    # Create a custom registry with specific configuration
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"\n1️⃣  Creating custom registry with temp directory: {tmpdir}")

        custom_config = {
            "test": {
                "type": "filesystem",
                "base_path": str(Path(tmpdir) / "test_storage"),
            },
            "cache": {
                "type": "filesystem",
                "base_path": str(Path(tmpdir) / "cache_storage"),
            },
        }

        registry = BlobBackendRegistry(configuration=custom_config)
        print("   ✓ Created custom registry")

        # Get backends from custom registry
        print("\n2️⃣  Using backends from custom registry:")

        # Get fully configured backends (with prefix wrapped if needed)
        test_backend = registry.get_backend("test.data")
        test_storage = BlobStorage(test_backend)
        test_storage.put("test.txt", b"test data")
        print("   ✓ Stored data in 'test.data' namespace")

        cache_backend = registry.get_backend("cache.api.responses")
        cache_storage = BlobStorage(cache_backend)
        cache_storage.put("response.json", b'{"cached": true}')
        print("   ✓ Stored data in 'cache.api.responses' namespace")

        # List available backends
        print("\n3️⃣  Available backends in custom registry:")
        for name in registry.list_backends():
            print(f"   ✓ {name}")

        # Verify storage
        print("\n4️⃣  Verifying stored data:")
        data = test_storage.get("test.txt")
        print(f"   ✓ test.txt: {data.decode()}")

        cached = cache_storage.get("response.json")
        print(f"   ✓ response.json: {cached.decode()}")


def demo_registry_features():
    """Demonstrate registry features like caching and name parsing."""
    print("\n" + "=" * 70)
    print("Demo 4: Registry Features")
    print("=" * 70)

    from jqsys.storage import get_default_registry

    registry = get_default_registry()

    # Name parsing
    print("\n1️⃣  Name parsing:")
    names = ["dev", "dev.images", "dev.images.thumbnails.small"]

    for name in names:
        base, prefix = registry.parse_name(name)
        print(f"   '{name}' → base='{base}', prefix='{prefix}'")

    # Backend caching
    print("\n2️⃣  Backend caching (same base backend instance reused):")

    backend1 = registry.get_backend("dev")
    backend2 = registry.get_backend("dev")
    backend3 = registry.get_backend("dev.images")  # Should return same as "dev"

    print(f"   ✓ backend1 ('dev') id: {id(backend1)}")
    print(f"   ✓ backend2 ('dev') id: {id(backend2)}")
    print(f"   ✓ backend3 ('dev.images') id: {id(backend3)}")
    print(f"   ✓ All same instance: {backend1 is backend2 is backend3}")

    # List configured backends
    print("\n3️⃣  Configured backends:")
    for name in registry.list_backends():
        print(f"   ✓ {name}")


def demo_error_handling():
    """Demonstrate error handling for missing backends."""
    print("\n" + "=" * 70)
    print("Demo 5: Error Handling")
    print("=" * 70)

    from jqsys.storage import BackendNotFoundError

    print("\n1️⃣  Attempting to use non-existent backend:")
    try:
        storage = BlobStorage.from_name("nonexistent.backend")
        storage.put("test.txt", b"data")
    except BackendNotFoundError as e:
        print(f"   ✗ Error caught: {e}")
        print("   ✓ Error handling works correctly")


def main():
    """Run all demos."""
    print("\n" + "=" * 70)
    print("Named Backend Registry Demo")
    print("=" * 70)

    try:
        demo_basic_usage()
        demo_namespaced_backends()
        demo_custom_registry()
        demo_registry_features()
        demo_error_handling()

        print("\n" + "=" * 70)
        print("✅ All demos completed successfully!")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
