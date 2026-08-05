using MySqlConnector;
using Yaal.Execution;
using Yaal.Sql;

namespace Yaal.Providers;

public sealed class MySqlContextManager : IDataProviderContextManager
{
    private readonly string _connectionString;

    public MySqlContextManager(DatabaseOptions options)
    {
        var port = int.TryParse(options.Port, out var p) ? p : 3306;
        var builder = new MySqlConnectionStringBuilder
        {
            Server = options.Host,
            Port = (uint)port,
            UserID = options.Username,
            Password = options.Password,
            Database = options.Database,
        };
        var query = options.Query ?? new Dictionary<string, string>();
        foreach (var key in new[] { "charset", "collation", "ssl_ca", "ssl_cert", "ssl_key", "connection_timeout", "use_pure", "autocommit" })
        {
            if (!query.TryGetValue(key, out var value))
                continue;
            switch (key)
            {
                case "charset":
                    builder.CharacterSet = value;
                    break;
                case "connection_timeout":
                    builder.ConnectionTimeout = uint.Parse(value);
                    break;
                case "ssl_ca":
                    builder.SslCa = value;
                    break;
                case "ssl_cert":
                    builder.SslCert = value;
                    break;
                case "ssl_key":
                    builder.SslKey = value;
                    break;
            }
        }
        _connectionString = builder.ConnectionString;
    }

    public IDataProvider GetContext() => new MySqlDataProvider(_connectionString);
}

public sealed class MySqlDataProvider : IDataProvider
{
    private readonly string _connectionString;
    private MySqlConnection? _conn;
    private MySqlTransaction? _tx;

    public MySqlDataProvider(string connectionString)
    {
        _connectionString = connectionString;
    }

    public void Begin()
    {
        _conn = new MySqlConnection(_connectionString);
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
        try { tx?.Commit(); }
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
        var sql = DataProviderHelper.GetExecutableContent("%s", twig, inputShape);
        var args = helper.BuildParameters(sql, inputShape, (_, v) => v);
        var (content, names) = PlaceholderUtil.ToNumbered(sql.Content, args.Count, i => "@p" + i);

        using var cmd = new MySqlCommand(content, con, _tx);
        for (var i = 0; i < args.Count; i++)
            cmd.Parameters.AddWithValue(names[i], args[i] ?? DBNull.Value);

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

        return (rows, cmd.LastInsertedId);
    }
}
