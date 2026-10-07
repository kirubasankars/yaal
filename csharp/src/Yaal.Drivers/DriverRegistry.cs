// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Yaal.Providers;

namespace Yaal.Drivers;

/// <summary>Opens one URL provider for the CLI, the example, and tests.</summary>
public static class DriverRegistry
{
    public static IDataProvider Open(string databaseUri)
    {
        var (providerName, options) = DatabaseUrl.Parse(databaseUri);
        var manager = providerName switch
        {
            "postgresql" => PostgresProviderFactory.Create(options),
            "mysql" => MySqlProviderFactory.Create(options),
            "clickhouse" => ClickHouseProviderFactory.Create(options),
            "sqlite3" => SqliteProviderFactory.Create(options),
            _ => throw new UnsupportedDatabaseUrlException(
                $"Unsupported database URL scheme '{providerName}'. " +
                "Supported schemes: sqlite3, postgresql, mysql, clickhouse"),
        };
        return manager.GetContext();
    }
}
