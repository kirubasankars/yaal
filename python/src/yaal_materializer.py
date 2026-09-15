# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Map shaped query results (dict/list graphs) to dataclasses or plain objects."""

from __future__ import annotations

import dataclasses
import enum
import uuid
from datetime import date, datetime
from typing import Any, Dict, get_args, get_origin, get_type_hints, List, Optional, Type, TypeVar, Union

from yaal_errors import YaalQueryError

T = TypeVar("T")


def throw_if_errors(shaped_result):
    if not isinstance(shaped_result, dict):
        return
    errors = shaped_result.get("errors")
    if not errors:
        return
    normalized = _normalize_errors(errors)
    if normalized:
        raise YaalQueryError(normalized)


def map_object(cls: Type[T], shaped_result) -> T:
    if shaped_result is None:
        raise TypeError("shaped_result cannot be None")
    if isinstance(shaped_result, cls):
        return shaped_result
    if not isinstance(shaped_result, dict):
        raise TypeError(
            "Expected a shaped object (dict) but got %s" % type(shaped_result).__name__
        )
    instance = cls()
    map_into(shaped_result, instance)
    return instance


def map_list(cls: Type[T], shaped_result) -> List[T]:
    if shaped_result is None:
        return []
    if isinstance(shaped_result, list) and shaped_result and all(
        isinstance(x, cls) for x in shaped_result
    ):
        return list(shaped_result)
    items = _normalize_list(shaped_result)
    return [map_object(cls, item) if isinstance(item, dict) else item for item in items]


def map_into(shaped_result, into):
    if into is None:
        raise TypeError("into cannot be None")
    if not isinstance(shaped_result, dict):
        raise TypeError(
            "Expected a shaped object (dict) but got %s"
            % (type(shaped_result).__name__ if shaped_result is not None else "null")
        )
    bindings = _type_bindings(type(into))
    for key, field_type, is_list, elem_type in bindings:
        raw = _dict_get_insensitive(shaped_result, key)
        if raw is None and not _dict_has_insensitive(shaped_result, key):
            continue
        if is_list:
            _set_list_property(into, key, raw, elem_type)
            continue
        if isinstance(raw, dict):
            nested = getattr(into, key, None)
            if nested is None:
                nested = map_object(_resolve_type(field_type), raw)
                setattr(into, key, nested)
            else:
                map_into(raw, nested)
            continue
        setattr(into, key, _convert_value(raw, _resolve_type(field_type)))


def _type_bindings(cls):
    if dataclasses.is_dataclass(cls):
        for field in dataclasses.fields(cls):
            ft = field.type
            if isinstance(ft, str):
                hints = get_type_hints(cls)
                ft = hints.get(field.name, ft)
            is_list, elem = _split_list_type(ft)
            yield field.name, ft, is_list, elem
        return

    hints = get_type_hints(cls)
    for name in hints:
        ft = hints[name]
        is_list, elem = _split_list_type(ft)
        yield name, ft, is_list, elem


def _split_list_type(field_type):
    field_type = _resolve_type(field_type)
    origin = get_origin(field_type)
    if origin in (list, List):
        args = get_args(field_type)
        return True, args[0] if args else Any
    return False, None


def _resolve_type(field_type):
    origin = get_origin(field_type)
    if origin is Union:
        args = [a for a in get_args(field_type) if a is not type(None)]
        return args[0] if args else field_type
    return field_type


def _set_list_property(target, key, raw, elem_type):
    if raw is None:
        setattr(target, key, None)
        return
    items = _normalize_list(raw)
    mapped = []
    for item in items:
        if item is None:
            mapped.append(None)
        elif isinstance(item, dict):
            elem_cls = _resolve_type(elem_type)
            if isinstance(elem_cls, type):
                mapped.append(map_object(elem_cls, item))
            else:
                mapped.append(item)
        else:
            mapped.append(_convert_value(item, _resolve_type(elem_type)))

    existing = getattr(target, key, None)
    if isinstance(existing, list):
        existing.clear()
        existing.extend(mapped)
        return
    setattr(target, key, mapped)


def _normalize_list(value):
    if isinstance(value, list):
        return value
    if isinstance(value, (tuple,)):
        return list(value)
    raise TypeError("Expected a shaped array (list) but got %s" % type(value).__name__)


def _to_snake_case(name):
    if not name:
        return name
    parts = []
    for i, c in enumerate(name):
        if c.isupper():
            if i > 0:
                parts.append("_")
            parts.append(c.lower())
        else:
            parts.append(c)
    return "".join(parts)


def _dict_get_insensitive(d, key):
    for candidate in (key, _to_snake_case(key)):
        if candidate in d:
            return d[candidate]
        candidate_lower = candidate.lower()
        for k, v in d.items():
            if k.lower() == candidate_lower:
                return v
    return None


def _dict_has_insensitive(d, key):
    for candidate in (key, _to_snake_case(key)):
        if candidate in d:
            return True
        candidate_lower = candidate.lower()
        if any(k.lower() == candidate_lower for k in d):
            return True
    return False


def _convert_value(value, target_type):
    if value is None:
        return None
    resolved = _resolve_type(target_type)
    if resolved is Any:
        return value
    if isinstance(resolved, type) and isinstance(value, resolved):
        return value
    if isinstance(resolved, type) and issubclass(resolved, enum.Enum):
        if isinstance(value, str):
            return resolved[value]
        return resolved(value)
    if resolved is uuid.UUID and isinstance(value, str):
        return uuid.UUID(value)
    if resolved is datetime and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    if resolved is date and isinstance(value, str):
        return date.fromisoformat(value)
    if resolved in (int, float, str, bool):
        return resolved(value)
    return value


def _normalize_errors(errors_obj):
    if isinstance(errors_obj, dict):
        return [errors_obj]
    if isinstance(errors_obj, list):
        return [e for e in errors_obj if isinstance(e, dict)]
    return []

