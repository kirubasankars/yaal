// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Microsoft.Data.Sqlite;
using Yaal.Execution;
using Yaal.Sql;

namespace Yaal.Providers;

public sealed class SqliteContextManager : IDataProviderContextManager
{
    private readonly DatabaseOptions _options;

    public SqliteContextManager(DatabaseOptions options)
    {
        _options = options;
    }

    public IDataProvider GetContext() => new SqliteDataProvider(_options);
}

public sealed class SqliteDataProvider : IDataProvider
{
    private readonly DatabaseOptions _options;
    private readonly string _database;
    private SqliteConnection? _con;

    public SqliteDataProvider(DatabaseOptions options)
    {
        _options = options;
        _database = string.IsNullOrEmpty(options.Database) ? ":memory:" : options.Database!;
    }

    public void Begin()
    {
        var query = _options.Query;
        if (query is { Count: > 0 })
        {
            var qs = string.Join("&", query.Select(kv =>
                Uri.EscapeDataString(kv.Key) + "=" + Uri.EscapeDataString(kv.Value)));
            var uri = _database == ":memory:"
                ? "file::memory:?" + qs
                : "file:" + _database + "?" + qs;
            _con = new SqliteConnection("Data Source=" + uri + ";Mode=ReadWriteCreate");
        }
        else
        {
            _con = new SqliteConnection("Data Source=" + _database);
        }
        _con.Open();
    }

    public void End()
    {
        var con = _con;
        _con = null;
        if (con == null)
            return;
        try
        {
            // Microsoft.Data.Sqlite autocommits by default per statement unless in explicit transaction.
            // Match Python: commit then close.
        }
        finally
        {
            con.Dispose();
        }
    }

    public void Error()
    {
        var con = _con;
        _con = null;
        if (con == null)
            return;
        con.Dispose();
    }

    public (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
        Twig twig, Shape inputShape, DataProviderHelper helper)
    {
        var con = _con!;
        var sql = helper.GetExecutableContent("?", twig, inputShape);
        using var cmd = con.CreateCommand();
        cmd.CommandText = sql.Content;
        var args = helper.BuildParameters(sql, inputShape, GetValue);
        for (var i = 0; i < args.Count; i++)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = "$p" + i;
            p.Value = args[i] ?? DBNull.Value;
            cmd.Parameters.Add(p);
        }

        // Rewrite ? placeholders to named $pN for Microsoft.Data.Sqlite
        if (args.Count > 0)
        {
            var parts = sql.Content.Split('?');
            if (parts.Length - 1 == args.Count)
            {
                var rendered = parts[0];
                for (var i = 0; i < args.Count; i++)
                    rendered += "$p" + i + parts[i + 1];
                cmd.CommandText = rendered;
            }
        }

        var rows = new List<IDictionary<string, object?>>();
        using var reader = cmd.ExecuteReader();
        if (reader.FieldCount > 0)
        {
            while (reader.Read())
            {
                var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                for (var i = 0; i < reader.FieldCount; i++)
                    row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                rows.Add(row);
            }
        }

        long lastId = 0;
        using (var idCmd = con.CreateCommand())
        {
            idCmd.CommandText = "SELECT last_insert_rowid()";
            var scalar = idCmd.ExecuteScalar();
            if (scalar != null && scalar != DBNull.Value)
                lastId = Convert.ToInt64(scalar);
        }

        return (rows, lastId);
    }

    private static object? GetValue(string parameterType, object? value)
    {
        if (parameterType == "blob" && value is byte[] bytes)
            return bytes;
        return value;
    }
}
