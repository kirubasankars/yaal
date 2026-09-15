// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.Json.Serialization;

namespace Yaal.Sql;

public sealed class SqlToken
{
    public string Type { get; set; } = "";
    public string Value { get; set; } = "";
    public int? Group { get; set; }
    public string? Name { get; set; }
    public bool Nullable { get; set; }
    [JsonPropertyName("nullable_parameter")]
    public string? NullableParameter { get; set; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public List<ParamDecl>? Parameters { get; set; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public object? Content { get; set; }
    /// <summary>sort()/dir() parameter name (e.g. $args.sort).</summary>
    public string? Param { get; set; }
    /// <summary>Allowlisted key → SQL expression for sort().</summary>
    public Dictionary<string, string>? Choices { get; set; }
}

public sealed class ParamDecl
{
    public string Name { get; set; } = "";
    public string Type { get; set; } = "";
    public bool Required { get; set; }
    public object? Default { get; set; }
    public bool HasDefault { get; set; }
}

public sealed class Twig
{
    public List<SqlToken> Content { get; set; } = new();
    public List<ParamDecl> Parameters { get; set; } = new();
    public List<string>? Nullable { get; set; }
    public string Connection { get; set; } = "db";
    /// <summary>True when twig contains sort() tokens; false skips runtime sort/dir resolution.</summary>
    [JsonPropertyName("has_sort_dir")]
    public bool? HasSortDir { get; set; }
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
