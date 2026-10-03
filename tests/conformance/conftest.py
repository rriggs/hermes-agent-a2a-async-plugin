"""
Phase 6 / Task 13 conformance test — fixtures are re-exported at
the ``tests/`` level (see ``tests/conftest.py``). Pytest
auto-loads every conftest.py up the directory tree, so the
recovery fixtures are visible here without any local re-export.

Test isolation: the per-test ``HERMES_HOME`` save-and-restore
is also at the ``tests/`` level (autouse), so the recovery
harness's env teardown cannot leak across test directories.
"""
