using Npgsql;
using Yaal.Execution;
using Yaal.Sql;

namespace Yaal.Providers;

public sealed class PostgresContextManager : IDataProviderContextManager
{
    private readonly string _connectionString;

    public PostgresContextManager(DatabaseOptions options)
    {
        var port = int.TryParse(options.Port, out var p) ? p : 5432;
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = options.Host,
            Port = port,
            Username = options.Username,
            Password = options.Password,
            Database = options.Database,
        };
        var query = options.Query ?? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var key in new[]
                 {
                     "sslmode", "sslcert", "sslkey", "sslrootcert", "connect_timeout",
                     "application_name", "options", "pooling", "maximum pool size", "maxpoolsize",
                     "pool_size", "minimum pool size", "minpoolsize"
                 })
        {
            if (!query.TryGetValue(key, out var value))
                continue;
            switch (key.ToLowerInvariant())
            {
                case "sslmode":
                    builder.SslMode = Enum.Parse<SslMode>(value, true);
                    break;
                case "connect_timeout":
                    builder.Timeout = int.Parse(value);
                    break;
                case "application_name":
                    builder.ApplicationName = value;
                    break;
                case "options":
                    builder.Options = value;
                    break;
                case "pooling":
                    builder.Pooling = value.Equals("1", StringComparison.OrdinalIgnoreCase)
                                      || value.Equals("true", StringComparison.OrdinalIgnoreCase)
                                      || value.Equals("yes", StringComparison.OrdinalIgnoreCase);
                    break;
                case "maximum pool size":
                case "maxpoolsize":
                case "pool_size":
                    builder.MaxPoolSize = int.Parse(value);
                    break;
                case "minimum pool size":
                case "minpoolsize":
                    builder.MinPoolSize = int.Parse(value);
                    break;
            }
        }
        _connectionString = builder.ConnectionString;
    }

    public IDataProvider GetContext() => new PostgresDataProvider(_connectionString);
}

public sealed class PostgresDataProvider : IDataProvider
{
    private readonly string _connectionString;
    private NpgsqlConnection? _conn;
    private NpgsqlTransaction? _tx;

    public PostgresDataProvider(string connectionString)
    {
        _connectionString = connectionString;
    }

    public void Begin()
    {
        _conn = new NpgsqlConnection(_connectionString);
        _conn.Open();
        _tx = _conn.BeginTransaction();
    }

    public void End()
    {
        var conn = _conn;
        var tx = _tx;
        _conn = null;
        _tx = null;
        if (conn == null)
            return;
        try
        {
            tx?.Commit();
        }
        finally
        {
            tx?.Dispose();
            conn.Dispose();
        }
    }

    public void Error()
    {
        var conn = _conn;
        var tx = _tx;
        _conn = null;
        _tx = null;
        if (conn == null)
            return;
        try { tx?.Rollback(); } catch { /* ignore */ }
        tx?.Dispose();
        conn.Dispose();
    }

    public (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
        Twig twig, Shape inputShape, DataProviderHelper helper)
    {
        var con = _conn!;
        var sql = helper.GetExecutableContent("%s", twig, inputShape);
        var args = helper.BuildParameters(sql, inputShape, (_, v) => v);
        var (content, _) = PlaceholderUtil.ToNumbered(sql.Content, args.Count, i => "$" + (i + 1));

        using var cmd = new NpgsqlCommand(content, con, _tx);
        for (var i = 0; i < args.Count; i++)
            cmd.Parameters.AddWithValue(args[i] ?? DBNull.Value);

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

        return (rows, LastInsertedId(rows));
    }

    private static object? LastInsertedId(List<IDictionary<string, object?>> rows)
    {
        if (rows.Count != 1)
            return null;
        var row = rows[0];
        if (row.TryGetValue("id", out var id))
            return id;
        if (row.Count == 1)
            return row.Values.First();
        return null;
    }
}
