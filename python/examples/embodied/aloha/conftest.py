"""Make standalone example modules importable with pytest's importlib mode.

The CLI scripts are intentionally runnable directly from this directory.
Only tests in this example subtree load this conftest; repository-wide pytest
configuration and installed LakeSoul packages remain unchanged.
"""

from pathlib import Path
import sys

EXAMPLE_DIR = str(Path(__file__).resolve().parent)
if EXAMPLE_DIR not in sys.path:
    sys.path.insert(0, EXAMPLE_DIR)
