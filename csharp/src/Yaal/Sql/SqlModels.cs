// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal.Sql;

public sealed class SqlToken
{
    public string Type { get; set; } = "";
    public string Value { get; set; } = "";
    public int? Group { get; set; }
    public string? Name { get; set; }
    public bool Nullable { get; set; }
    public string? NullableParameter { get; set; }
    public List<ParamDecl>? Parameters { get; set; }
    public object? Content { get; set; }
}

public sealed class ParamDecl
{
    public string Name { get; set; } = "";
    public string Type { get; set; } = "";
}

public sealed class Twig
{
    public List<SqlToken> Content { get; set; } = new();
    public List<ParamDecl> Parameters { get; set; } = new();
    public List<string>? Nullable { get; set; }
    public string Connection { get; set; } = "db";
}

public sealed class SqlAst
{
    public Dictionary<string, ParamDecl>? Parameters { get; set; }
    public List<Twig>? SqlStmts { get; set; }
}

public sealed class CompiledSql
{
    public string Content { get; set; } = "";
    public List<ParamDecl> Parameters { get; set; } = new();
}
