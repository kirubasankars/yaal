// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Microsoft.Data.Sqlite;
using Yaal;

var repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "api"));
var dbPath = Path.Combine(Path.GetTempPath(), "yaal-example-" + Guid.NewGuid().ToString("N") + ".db");

await using (var con = new SqliteConnection("Data Source=" + dbPath))
{
    await con.OpenAsync();
    await using var cmd = con.CreateCommand();
    cmd.CommandText = "SELECT 1";
    await cmd.ExecuteScalarAsync();
}

var y = new Yaal.Yaal(repoRoot, debug: true);
y.SetupDataProvider("db", "sqlite3:///" + dbPath);

var users = y.Query("user/get");
foreach (var row in (System.Collections.IEnumerable)users!)
{
    if (row is IDictionary<string, object?> dict)
        Console.WriteLine($"{dict["id"]} {dict["name"]}");
}
