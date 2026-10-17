# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Compile Yaal SQL through the shared library."""

import ctypes
import json
import os
import sys
from pathlib import Path


class CompileError(Exception):
    """The compiler rejected the SQL or the parameters."""


def compile(sql, params=None, *, placeholder="question"):
    """Return ``{"statements": [{"sql", "parameters"}]}``.

    ``placeholder`` is ``question`` (``?``) or ``numbered`` (``$1``, ``$2``, ...).
    Omitted parameters are an empty object.
    """
    if params is None:
        params = {}
    placeholder = placeholder or "question"
    if "\0" in sql or "\0" in placeholder:
        raise CompileError("sql and placeholder must not contain a NUL character")
    raw = _invoke(sql, json.dumps(params, allow_nan=False), placeholder)
    data = json.loads(raw)
    if "statements" not in data:
        raise CompileError(data.get("error", raw))
    return data


def _invoke(sql, params_json, placeholder):
    lib = _library()
    ptr = lib.yaal_compile(
        sql.encode(),
        params_json.encode(),
        placeholder.encode(),
    )
    if not ptr:
        raise CompileError("yaal_compile returned null")
    try:
        return ctypes.cast(ptr, ctypes.c_char_p).value.decode()
    finally:
        lib.yaal_free(ptr)


def _library():
    if getattr(_library, "cached", None) is not None:
        return _library.cached
    path = _library_path()
    lib = ctypes.CDLL(path)
    lib.yaal_compile.argtypes = [
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_char_p,
    ]
    lib.yaal_compile.restype = ctypes.c_void_p
    lib.yaal_free.argtypes = [ctypes.c_void_p]
    lib.yaal_free.restype = None
    _library.cached = lib
    return lib


def _library_path():
    env = os.environ.get("YAAL_LIB")
    if env:
        return env
    name = {
        "win32": "yaal_ffi.dll",
        "darwin": "libyaal_ffi.dylib",
    }.get(sys.platform, "libyaal_ffi.so")
    starts = [Path(__file__).resolve(), Path.cwd()]
    for start in starts:
        directory = start if start.is_dir() else start.parent
        while True:
            for profile in ("debug", "release"):
                candidate = directory / "target" / profile / name
                if candidate.is_file():
                    return str(candidate)
            parent = directory.parent
            if parent == directory:
                break
            directory = parent
    raise CompileError("yaal shared library not found; set YAAL_LIB")
