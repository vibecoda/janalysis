#!/usr/bin/env python
"""Simple test runner script."""

import subprocess
import sys
from pathlib import Path


def main():
    """Run pytest with appropriate arguments."""
    project_root = Path(__file__).parent
    
    # Change to project directory
    import os
    os.chdir(project_root)
    
    # Run pytest
    cmd = [
        sys.executable, "-m", "pytest",
        "tests/",
        "-v",
        "--tb=short",
        "--color=yes"
    ]
    
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())