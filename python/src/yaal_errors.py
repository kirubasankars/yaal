# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

class YaalError(Exception):
    """Base error for Yaal configuration and descriptor problems."""


class DescriptorNotFoundError(YaalError):
    """Raised when a descriptor path has no SQL files or cannot be built."""


class UnsupportedDatabaseUrlError(YaalError):
    """Raised when a database URL scheme is missing or unsupported."""


class PathEscapeError(YaalError):
    """Raised when a descriptor path resolves outside the API root."""


class SortDirError(YaalError):
    """Soft error: unknown or invalid sort()/dir() runtime value (no SQL execute)."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


class YaalQueryError(YaalError):
    """Raised when a shaped query result contains validation or execution errors."""

    def __init__(self, errors):
        super().__init__("Query returned validation or execution errors.")
        self.errors = errors
