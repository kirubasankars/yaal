using Yaal.Descriptors;

namespace Yaal;

public static class ContextFactory
{
    public static Shape CreateContext(Branch descriptor, object? payload = null, object? args = null)
    {
        var model = descriptor.Model;
        var validators = descriptor.Validators;

        Dictionary<string, object?>? argsSchema = null;
        Dictionary<string, object?>? payloadSchema = null;
        Json.Schema.JsonSchema? argsValidator = null;
        Json.Schema.JsonSchema? payloadValidator = null;

        if (model != null && validators != null)
        {
            argsSchema = model.Args;
            payloadSchema = model.Payload;
            validators.TryGetValue("args", out argsValidator);
            validators.TryGetValue("payload", out payloadValidator);
        }

        var argsShape = new Shape(schema: argsSchema, validator: argsValidator);
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
            validator: payloadValidator,
            data: payloadData,
            extras: extras);
    }
}
