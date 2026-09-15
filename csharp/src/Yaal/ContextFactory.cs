// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Yaal.Descriptors;

namespace Yaal;

public static class ContextFactory
{
    public static Shape CreateContext(Branch descriptor, object? payload = null, object? args = null)
    {
        var model = descriptor.Model;

        Dictionary<string, object?>? argsSchema = null;
        Dictionary<string, object?>? payloadSchema = null;

        if (model != null)
        {
            argsSchema = model.Args;
            payloadSchema = model.Payload;
        }

        var argsShape = new Shape(schema: argsSchema);
        if (args != null)
        {
            var argsDict = JsonUtil.ToDict(args) ?? JsonUtil.ObjectToDictionary(args);
            foreach (var (k, v) in argsDict)
                argsShape.SetProp(k, v);
        }

        var paramsShape = new Shape(data: new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["path"] = descriptor.Path,
            ["$run_id"] = Guid.NewGuid().ToString(),
        });

        var extras = new Dictionary<string, Shape>(StringComparer.Ordinal)
        {
            ["$params"] = paramsShape,
            ["$args"] = argsShape,
        };

        object? payloadData = payload;
        if (payload != null && payload is not Dictionary<string, object?> and not IList<object?>)
            payloadData = JsonUtil.ToDict(payload) ?? JsonUtil.NormalizeValue(payload);

        return new Shape(
            schema: payloadSchema,
            data: payloadData,
            extras: extras);
    }
}
