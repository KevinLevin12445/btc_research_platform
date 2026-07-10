"""
Version constants, stamped into every state DB record and reported in
every generated report — the concrete implementation of Part II's "Data
Versioning" requirement (schema version, feature version, software
version, creation timestamp on everything, for reproducibility).

Bump these deliberately when the corresponding logic changes in a way
that would produce different output for the same input. A month recorded
with an older SCHEMA_VERSION than what's current is a signal (not
necessarily an error) that it may be worth re-ingesting under the current
logic — the state DB makes that comparison possible at a glance.
"""

SCHEMA_VERSION = "2.0"     # tick Parquet layout (schema.py TICK_SCHEMA)
FEATURE_VERSION = "2.0"    # bars/features generation logic
SOFTWARE_VERSION = "2.0.0"  # overall platform version, matches src/__init__.py
