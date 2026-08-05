# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Ahead-of-time descriptor compile/export/import (token twigs preserved)."""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path

from jsonschema import Draft4Validator, FormatChecker

PRECOMPILE_VERSION = 1


def export_descriptor(descriptor):
    """Deep-copy a trunk for JSON export: drop validators, keep twig tokens."""
    data = copy.deepcopy(descriptor)
    _strip_validators(data)
    return data


def import_descriptor(data):
    """Load a precompiled trunk dict and rebuild Draft4 validators from schemas."""
    descriptor = copy.deepcopy(data)
    descriptor.pop("_yaal_precompile", None)
    _attach_validators(descriptor)
    return descriptor


def _strip_validators(descriptor):
    if not isinstance(descriptor, dict):
        return
    descriptor.pop("_validators", None)
    for branch in descriptor.get("branches") or []:
        _strip_validators(branch)


def _attach_validators(trunk):
    model = trunk.get("model") or {}
    validators = {}
    for key in ("args", "payload"):
        schema = model.get(key)
        if schema:
            validators[key] = Draft4Validator(
                schema=schema, format_checker=FormatChecker()
            )
        else:
            validators[key] = None
    trunk["_validators"] = validators


def discover_output_mappers(api_root, path):
    """Return [None, ...] including alternate mappers from $.output.<name>.yaml/json."""
    folder = Path(api_root) / path
    mappers = [None]
    if not folder.is_dir():
        return mappers
    seen = set()
    for pattern in ("$.output.*.yaml", "$.output.*.json"):
        for f in folder.glob(pattern):
            # $.output.summary.yaml -> summary
            stem = f.name
            if stem.startswith("$.output.") and (
                stem.endswith(".yaml") or stem.endswith(".json")
            ):
                name = stem[len("$.output.") :]
                name = name.rsplit(".", 1)[0]
                if name and name not in seen:
                    seen.add(name)
                    mappers.append(name)
    return mappers


def artifact_filename(path, output_mapper=None):
    if output_mapper:
        return "%s#%s.json" % (path, output_mapper)
    return "%s.json" % path


def compile_api(api_root, out_dir, *, list_paths=None):
    """Compile all descriptors under api_root into JSON files under out_dir.

    Returns list of written relative artifact paths.
    """
    from yaal import Yaal

    api_root = str(api_root)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if list_paths is None:
        from yaal_cli import list_descriptors

        paths = list_descriptors(Path(api_root))
    else:
        paths = list(list_paths)

    y = Yaal(api_root, debug=True)
    written = []
    for path in paths:
        for mapper in discover_output_mappers(api_root, path):
            descriptor = y.create_descriptor(path, mapper)
            data = export_descriptor(descriptor)
            data["_yaal_precompile"] = {
                "version": PRECOMPILE_VERSION,
                "path": path,
                "output_mapper": mapper,
            }
            rel = artifact_filename(path, mapper)
            dest = out_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, sort_keys=True)
                f.write("\n")
            written.append(rel.replace(os.sep, "/"))
    return written


def load_precompiled_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return import_descriptor(data)


def resolve_precompiled_path(precompiled_dir, descriptor_path, output_mapper=None):
    rel = artifact_filename(descriptor_path, output_mapper)
    return Path(precompiled_dir) / rel
