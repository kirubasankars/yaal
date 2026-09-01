// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Reflection;
using System.Runtime.CompilerServices;

namespace Yaal.Providers;

internal static class SqliteProviderFactory
{
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static IDataProviderContextManager Create(DatabaseOptions options) =>
        ProviderFactory.CreateOrThrow(options, "SQLite", "Microsoft.Data.Sqlite", static o => new SqliteContextManager(o));
}

internal static class PostgresProviderFactory
{
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static IDataProviderContextManager Create(DatabaseOptions options) =>
        ProviderFactory.CreateOrThrow(options, "PostgreSQL", "Npgsql", static o => new PostgresContextManager(o));
}

internal static class MySqlProviderFactory
{
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static IDataProviderContextManager Create(DatabaseOptions options) =>
        ProviderFactory.CreateOrThrow(options, "MySQL", "MySqlConnector", static o => new MySqlContextManager(o));
}

internal static class ClickHouseProviderFactory
{
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static IDataProviderContextManager Create(DatabaseOptions options) =>
        ProviderFactory.CreateOrThrow(options, "ClickHouse", "ClickHouse.Client", static o => new ClickHouseContextManager(o));
}

file static class ProviderFactory
{
    public static IDataProviderContextManager CreateOrThrow(
        DatabaseOptions options,
        string engine,
        string packageId,
        Func<DatabaseOptions, IDataProviderContextManager> create)
    {
        try
        {
            return create(options);
        }
        catch (Exception ex) when (ex is FileNotFoundException or ReflectionTypeLoadException or TypeLoadException)
        {
            throw new YaalException(
                $"{engine} requires {packageId}. Add it to your app: dotnet add package {packageId}",
                ex);
        }
    }
}
