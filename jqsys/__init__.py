"""Lightweight J-Quants utilities for CLI demos.

This package provides:
- Minimal auth using a refresh token from environment/.env
- A thin HTTP client with retries and pagination helpers

Note: Keep this small and focused on terminal demos for exploration.
"""

__all__ = ["auth", "client", "stock", "portfolio"]
