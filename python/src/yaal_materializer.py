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


def yaal_ignore(**field_kwargs):
    """Dataclass field marker: skip during strict typed mapping."""
    metadata = dict(field_kwargs.pop("metadata", {}))
    metadata["yaal_ignore"] = True
    return dataclasses.field(metadata=metadata, **field_kwargs)


def throw_if_errors(shaped_result):
    if not isinstance(shaped_result, dict):
        return
    errors = shaped_result.get("errors")
    if not errors:
        return
    normalized = _normalize_errors(errors)
    if normalized:
        raise YaalQueryError(normalized)


def map_object(cls: Type[T], shaped_result, *, strict: bool = True) -> T:
    if shaped_result is None:
        raise TypeError("shaped_result cannot be None")
    if isinstance(shaped_result, cls):
        return shaped_result
    if not isinstance(shaped_result, dict):
        raise TypeError(
            "Expected a shaped object (dict) but got %s" % type(shaped_result).__name__
        )
    _ensure_has_bindable_properties(cls)
    instance = cls()
    _map_into(shaped_result, instance, strict=strict)
    return instance


def map_list(cls: Type[T], shaped_result, *, strict: bool = True) -> List[T]:
    if shaped_result is None:
        return []
    if isinstance(shaped_result, list) and shaped_result and all(
        isinstance(x, cls) for x in shaped_result
    ):
        return list(shaped_result)
    items = _normalize_list(shaped_result)
    return [
        map_object(cls, item, strict=strict) if isinstance(item, dict) else item
        for item in items
    ]


def map_into(shaped_result, into):
    if into is None:
        raise TypeError("into cannot be None")
    if not isinstance(shaped_result, dict):
        raise TypeError(
            "Expected a shaped object (dict) but got %s"
            % (type(shaped_result).__name__ if shaped_result is not None else "null")
        )
    _map_into(shaped_result, into, strict=False)


def _map_into(shaped_result, into, *, strict: bool):
    bindings = _type_bindings(type(into))
    for key, field_type, is_list, elem_type in bindings:
        if not _dict_has_insensitive(shaped_result, key):
            if strict:
                raise ValueError(_missing_column_message(key, type(into)))
            continue
        raw = _dict_get_insensitive(shaped_result, key)
        if is_list:
            _set_list_property(into, key, raw, elem_type, strict=strict)
            continue
        if isinstance(raw, dict):
            nested = getattr(into, key, None)
            elem_cls = _resolve_type(field_type)
            if nested is None:
                nested = map_object(elem_cls, raw, strict=strict)
                setattr(into, key, nested)
            else:
                _map_into(raw, nested, strict=strict)
            continue
        setattr(into, key, _convert_value(raw, _resolve_type(field_type)))


def _ensure_has_bindable_properties(cls):
    if not any(True for _ in _type_bindings(cls)):
        raise TypeError(
            "Type %s has no mappable fields. Add public fields or mark client-only "
            "fields with yaal_ignore()." % cls.__name__
        )


def _missing_column_message(key, owner_type):
    snake = _to_snake_case(key)
    tried = key if snake.lower() == key.lower() else "%s, %s" % (key, snake)
    return (
        "Field '%s' on type %s has no matching column in query result (tried %s)."
        % (key, owner_type.__name__, tried)
    )


def _type_bindings(cls):
    if dataclasses.is_dataclass(cls):
        for field in dataclasses.fields(cls):
            if field.metadata.get("yaal_ignore"):
                continue
            ft = field.type
            if isinstance(ft, str):
                hints = get_type_hints(cls)
                ft = hints.get(field.name, ft)
            is_list, elem = _split_list_type(ft)
            yield field.name, ft, is_list, elem
        return

    hints = get_type_hints(cls)
    ignored = getattr(cls, "__yaal_ignore__", ())
    for name in hints:
        if name in ignored:
            continue
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


def _set_list_property(target, key, raw, elem_type, *, strict: bool):
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
                mapped.append(map_object(elem_cls, item, strict=strict))
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
