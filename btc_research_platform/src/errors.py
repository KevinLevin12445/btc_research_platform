"""Shared exception types used across the pipeline."""


class CorruptedArchiveError(Exception):
    """Raised when a source file (ZIP or CSV) fails structural/CRC/checksum
    validation or cannot be parsed into the expected schema."""


class DiskSpaceGuardError(Exception):
    """Raised when free disk space drops below the configured safety margin."""


class VerificationFailedError(Exception):
    """Raised when independent post-write verification of a committed
    Parquet file does not match what was recorded during ingestion. A month
    that raises this is NOT marked done, regardless of how ingestion itself
    went — see verification.py module docstring."""


class WrongMarketError(Exception):
    """Raised when a source file's schema/characteristics indicate it is
    NOT USD-M Futures data (e.g. matches the Spot 7-column layout instead
    of the Futures 6-column layout). This is a hard stop, not a warning —
    see README "Two-Venue Research Model" for why mixing markets into the
    Futures database is a correctness issue, not a cosmetic one."""
