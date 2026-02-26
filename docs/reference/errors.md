# Error Reference

## `FrameKitError`

Base error type carrying a `code` from `ErrorCode`.

## `ColumnNotFoundError`

Raised when selecting, mutating, joining, or reshaping with a missing column name.

## `TypeMismatchError`

Raised when an operation receives incompatible value or column types.

## `ShapeMismatchError`

Raised when row counts are incompatible (for example, `assign` with different lengths).

## `ParseError`

Raised when parsers cannot decode or validate an input payload.

## `IOError`

Raised when reading/writing files or remote sources fails.
