"""Storage backend implementations."""

from jqsys.core.storage.backends.filesystem_backend import FilesystemBackend
from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

__all__ = [
    "FilesystemBackend",
    "PrefixedBlobBackend",
]

# Conditionally import MinIO and MongoDB backends if dependencies are available
try:
    from jqsys.core.storage.backends.minio_backend import MinIOBackend

    __all__.append("MinIOBackend")
except ImportError:
    pass

try:
    from jqsys.core.storage.backends.mongodb_backend import MongoDBBackend

    __all__.append("MongoDBBackend")
except ImportError:
    pass
