// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Parameters;
using Yaal.Execution;
using Yaal.Sql;

namespace Yaal.Providers;

public sealed class ClickHouseContextManager : IDataProviderContextManager
{
    private readonly string _connectionString;

    public ClickHouseContextManager(DatabaseOptions options)
    {
        var host = options.Host ?? "127.0.0.1";
        var port = int.TryParse(options.Port, out var p) ? p : 8123;
        // Python native default is 9000; ClickHouse.Client uses HTTP — remap.
        if (port == 9000)
            port = 8123;

        var user = options.Username ?? "default";
        var password = options.Password ?? "";
        var database = options.Database ?? "default";

        var builder = new ClickHouseConnectionStringBuilder
        {
            Host = host,
            Port = (ushort)port,
            Username = user,
            Password = password,
            Database = database,
        };

        var query = options.Query ?? new Dictionary<string, string>();
        if (query.TryGetValue("secure", out var secure) &&
            secure.ToLowerInvariant() is "1" or "true" or "yes" or "on")
        {
            builder.Protocol = "https";
        }

        _connectionString = builder.ConnectionString;
    }

    public IDataProvider GetContext() => new ClickHouseDataProvider(_connectionString);
}

public sealed class ClickHouseDataProvider : IDataProvider
{
    private readonly string _connectionString;
    private ClickHouseConnection? _client;

    public ClickHouseDataProvider(string connectionString)
    {
        _connectionString = connectionString;
    }

    public void Begin()
    {
        _client = new ClickHouseConnection(_connectionString);
        _client.Open();
    }

    public void End()
    {
        var client = _client;
        _client = null;
        client?.Dispose();
    }

    public void Error()
    {
        var client = _client;
        _client = null;
        try { client?.Dispose(); } catch { /* ignore */ }
    }

    public (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
        Twig twig, Shape inputShape, DataProviderHelper helper)
    {
        var client = _client!;
        var sql = helper.GetExecutableContent("%s", twig, inputShape);
        var args = helper.BuildParameters(sql, inputShape, (_, v) => v);
        var (content, _) = PlaceholderUtil.ToNumbered(
            sql.Content,
            args.Count,
            i => "{p" + i + ":" + ClickHouseTypeName(args[i]) + "}");

        using var cmd = client.CreateCommand();
        cmd.CommandText = content;
        for (var i = 0; i < args.Count; i++)
        {
            cmd.Parameters.Add(new ClickHouseDbParameter
            {
                ParameterName = "p" + i,
                Value = args[i] ?? DBNull.Value,
            });
        }

        var rows = new List<IDictionary<string, object?>>();
        using var reader = cmd.ExecuteReader();
        if (reader.FieldCount > 0)
        {
            var namesCols = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var name = reader.GetName(i);
                var dot = name.LastIndexOf('.');
                namesCols[i] = dot >= 0 ? name[(dot + 1)..] : name;
            }

            while (reader.Read())
            {
                var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                for (var i = 0; i < reader.FieldCount; i++)
                    row[namesCols[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                rows.Add(row);
            }
        }

        return (rows, null);
    }

    private static string ClickHouseTypeName(object? value) =>
        value switch
        {
            null => "Nullable(Nothing)",
            bool => "UInt8",
            byte => "UInt8",
            short => "Int16",
            int => "Int32",
            long => "Int64",
            float => "Float32",
            double => "Float64",
            decimal => "Decimal128(9)",
            DateTime => "DateTime",
            DateTimeOffset => "DateTime",
            Guid => "UUID",
            byte[] => "String",
            _ => "String",
        };
}
