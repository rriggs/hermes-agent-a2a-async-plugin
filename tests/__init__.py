"""Top-level tests package marker.

This file is required for ``tests`` to be importable as a
regular Python package (so that ``pytest_plugins =
["tests.recovery.conftest"]`` resolves in
``tests/conftest.py``). The pytest.ini ``--ignore=__init__.py``
option prevents this file from being collected as a test
module; it lives in the package solely for import resolution.
"""
