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
