using System.Text.RegularExpressions;

namespace Yaal;

public sealed class DatabaseOptions
{
    public string? Username { get; set; }
    public string? Password { get; set; }
    public string? Host { get; set; }
    public string? Port { get; set; }
    public string? Database { get; set; }
    public Dictionary<string, string>? Query { get; set; }
}

public static class DatabaseUrl
{
    private static readonly Regex Pattern = new(
        @"^(?<name>[\w\+]+)://
            (?:
                (?<username>[^:/]*)
                (?::(?<password>[^/]*))?
            @)?
            (?:
                (?<host>[^/:]*)
                (?::(?<port>[^/]*))?
            )?
            (?:/(?<database>.*))?
            $",
        RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled);

    public static (string Scheme, DatabaseOptions Options) Parse(string connectionUrl)
    {
        var m = Pattern.Match(connectionUrl);
        if (!m.Success)
        {
            throw new ArgumentException(
                "Could not parse database URL '" + connectionUrl + "'. Expected forms like " +
                "sqlite3:////abs/path.db, sqlite3://./rel/path.db, " +
                "postgresql://user:pass@host:5432/db, mysql://user:pass@host:3306/db, " +
                "clickhouse://user:pass@host:9000/db");
        }

        var options = new DatabaseOptions
        {
            Username = NullIfEmpty(m.Groups["username"].Value),
            Password = NullIfEmpty(m.Groups["password"].Value),
            Host = NullIfEmpty(m.Groups["host"].Value),
            Port = NullIfEmpty(m.Groups["port"].Value),
            Database = NullIfEmpty(m.Groups["database"].Value),
        };

        if (options.Database != null)
        {
            var tokens = options.Database.Split('?', 2);
            options.Database = tokens[0];
            if (tokens.Length > 1)
            {
                options.Query = ParseQuery(tokens[1]);
            }
        }

        if (options.Username != null)
            options.Username = Uri.UnescapeDataString(options.Username.Replace('+', ' '));
        if (options.Password != null)
            options.Password = Uri.UnescapeDataString(options.Password.Replace('+', ' '));

        var providerName = m.Groups["name"].Value;
        if (providerName == "sqlite3")
            options = NormalizeSqliteOptions(options);

        return (providerName, options);
    }

    private static DatabaseOptions NormalizeSqliteOptions(DatabaseOptions options)
    {
        var database = options.Database ?? "";
        var host = options.Host;

        if (host == ".")
            database = database.Length > 0 ? "./" + database : ".";
        else if (!string.IsNullOrEmpty(host))
            database = host + (database.Length > 0 ? "/" + database : "");

        return new DatabaseOptions
        {
            Username = options.Username,
            Password = options.Password,
            Host = null,
            Port = options.Port,
            Database = database,
            Query = options.Query,
        };
    }

    private static Dictionary<string, string> ParseQuery(string query)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var part in query.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var kv = part.Split('=', 2);
            var key = Uri.UnescapeDataString(kv[0].Replace('+', ' '));
            var value = kv.Length > 1 ? Uri.UnescapeDataString(kv[1].Replace('+', ' ')) : "";
            result[key] = value;
        }
        return result;
    }

    private static string? NullIfEmpty(string value) =>
        string.IsNullOrEmpty(value) ? null : value;
}
