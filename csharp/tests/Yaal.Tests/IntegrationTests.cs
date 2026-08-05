// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;

namespace Yaal.Tests;

public class IntegrationTests
{
    private static bool IntegrationEnabled =>
        Environment.GetEnvironmentVariable("YAAL_INTEGRATION") == "1";

    private static string FixtureApi =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..", "tests", "fixtures", "api"));

    private static void AssertUserAdmin(object? result)
    {
        var dict = (Dictionary<string, object?>)result!;
        Convert.ToInt64(dict["id"]).Should().Be(1);
        dict["name"]!.ToString().Should().Be("admin");
        var roles = ((System.Collections.IEnumerable)dict["roles"]!).Cast<object>().ToList();
        roles.Should().HaveCount(2);
    }

    [SkippableFact]
    public void Postgres_user_with_nested_roles()
    {
        Skip.IfNot(IntegrationEnabled, "Set YAAL_INTEGRATION=1 to run engine integration tests");

        var y = new Yaal(FixtureApi, debug: true);
        y.SetupDataProvider("db", Environment.GetEnvironmentVariable("YAAL_PG_URL")
                                  ?? "postgresql://yaal:yaal@127.0.0.1:54329/yaal");
        AssertUserAdmin(y.Query("user/get", args: new { id = 1 }));
    }

    [SkippableFact]
    public void Mysql_user_with_nested_roles()
    {
        Skip.IfNot(IntegrationEnabled, "Set YAAL_INTEGRATION=1 to run engine integration tests");

        var y = new Yaal(FixtureApi, debug: true);
        y.SetupDataProvider("db", Environment.GetEnvironmentVariable("YAAL_MYSQL_URL")
                                  ?? "mysql://yaal:yaal@127.0.0.1:33069/yaal");
        AssertUserAdmin(y.Query("user/get", args: new { id = 1 }));
    }

    [SkippableFact]
    public void Clickhouse_user_with_nested_roles()
    {
        Skip.IfNot(IntegrationEnabled, "Set YAAL_INTEGRATION=1 to run engine integration tests");

        var y = new Yaal(FixtureApi, debug: true);
        y.SetupDataProvider("db", Environment.GetEnvironmentVariable("YAAL_CH_URL")
                                  ?? "clickhouse://yaal:yaal@127.0.0.1:9000/yaal");
        AssertUserAdmin(y.Query("user/get", args: new { id = 1 }));
    }
}
