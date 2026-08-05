# Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
# Use of this source code is governed by a MIT style
# license that can be found in the LICENSE file.

"""Validate Yaal output YAML schemas (flat-only field maps)."""


_ERR_BARE_TYPE = (
    "bare properties.type object|array is not allowed; "
    "use flat fields under properties (root type already sets array/object)"
)


def normalize_output_model(model):
    """Validate output models and recurse into named branch schemas.

    Flat field maps only. Bare ``type: object|array`` under ``properties`` is
    rejected (including the old nested item-wrapper form). A JSON field named
    ``type`` must use ``type: { mapped: col }``.
    """
    if not isinstance(model, dict):
        return model

    model = dict(model)
    props = model.get("properties")
    if not isinstance(props, dict):
        return model

    bare = props.get("type")
    if bare in ("object", "array"):
        raise TypeError(_ERR_BARE_TYPE)

    new_props = {}
    for k, v in props.items():
        if (
            isinstance(v, dict)
            and "mapped" not in v
            and ("type" in v or "properties" in v)
        ):
            new_props[k] = normalize_output_model(v)
        else:
            new_props[k] = v
    model["properties"] = new_props
    return model
