// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.RegularExpressions;
using FluentAssertions;
using Yaal.Execution;
using Yaal.Sql;

namespace Yaal.Tests;

/// <summary>C# parity for the Python optional_groups/array hardening matrix.</summary>
public class OptionalGroupsHardeningTests
{
    private const string GroupsSql =
        "--(pairs blob)--\nselect * from t where optional_groups({{pairs}}, id = {{id}})\n";

    private const string GroupsInSql =
        "--(pairs blob)--\nselect * from t where optional_groups({{pairs}}, id in ({{ids}}))\n";

    private static string NormalizeWs(string sql) => Regex.Replace(sql, @"\s+", " ").Trim();

    private static Twig Twig(string sql) => SqlParser.Parse(Lexer.Lex(sql), "$")!.SqlStmts![0];

    private static Shape ShapeWith(object? pairs) =>
        new(data: new Dictionary<string, object?> { ["pairs"] = pairs });

    private static Dictionary<string, object?> Row(string field, object? value) =>
        new(StringComparer.OrdinalIgnoreCase) { [field] = value };

    private static (string Sql, List<object?> Values) CompileAndBind(string sql, object? pairs)
    {
        var helper = new DataProviderHelper();
        var shape = ShapeWith(pairs);
        var compiled = helper.GetExecutableContent("?", Twig(sql), shape);
        var values = helper.BuildParameters(compiled, shape, (_, v) => v);
        return (NormalizeWs(compiled.Content), values);
    }

    [Fact]
    public void List_of_rows_compiles_and_binds()
    {
        var (sql, values) = CompileAndBind(
            GroupsSql,
            new List<object?> { Row("id", 1L), Row("id", 2L) });

        sql.Should().Be("select * from t where (id = ?) or (id = ?)");
        values.Should().Equal(1L, 2L);
    }

    [Fact]
    public void Json_string_blob_compiles_and_binds()
    {
        var (sql, values) = CompileAndBind(GroupsSql, "[{\"id\": 1}, {\"id\": 2}]");

        sql.Should().Be("select * from t where (id = ?) or (id = ?)");
        values.Should().Equal(1L, 2L);
    }

    [Fact]
    public void Json_bytes_blob_compiles_and_binds()
    {
        var (sql, values) = CompileAndBind(
            GroupsSql, System.Text.Encoding.UTF8.GetBytes("[{\"id\": 7}]"));

        sql.Should().Be("select * from t where (id = ?)");
        values.Should().Equal(7L);
    }

    [Fact]
    public void Json_string_blob_expands_in_list_per_row()
    {
        var (sql, values) = CompileAndBind(
            GroupsInSql, "[{\"ids\": [1, 2]}, {\"ids\": [3]}]");

        sql.Should().Be("select * from t where (id in (?, ?)) or (id in (?))");
        values.Should().Equal(1L, 2L, 3L);
    }

    [Fact]
    public void Json_object_source_rejected()
    {
        Action act = () => CompileAndBind(GroupsSql, "{\"id\": 1}");
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*must be a JSON array of objects*");
    }

    [Fact]
    public void Malformed_json_source_rejected()
    {
        Action act = () => CompileAndBind(GroupsSql, "not json");
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*must be a JSON array of objects*");
    }

    [Fact]
    public void Scalar_source_rejected()
    {
        Action act = () => GroupRuntimeUtil.CoerceOptionalGroupsBlob(5, "pairs");
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*must be a JSON array of objects*");
    }

    [Fact]
    public void Null_source_stays_null()
    {
        GroupRuntimeUtil.CoerceOptionalGroupsBlob(null, "pairs").Should().BeNull();
    }

    [Fact]
    public void Nested_object_row_value_rejected()
    {
        Action act = () => CompileAndBind(
            GroupsSql,
            new List<object?> { Row("id", Row("nested", 1L)) });

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*{{id}}*pairs*must be a scalar value*");
    }

    [Fact]
    public void Nested_object_inside_in_list_rejected()
    {
        Action act = () => CompileAndBind(
            GroupsInSql,
            new List<object?> { Row("ids", new List<object?> { 1L, Row("nested", 2L) }) });

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*must be a scalar value*");
    }

    [Fact]
    public void Non_object_row_rejected()
    {
        Action act = () => CompileAndBind(GroupsSql, new List<object?> { 1L, 2L });
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*rows must be objects*");
    }

    [Fact]
    public void Null_row_value_rejected()
    {
        Action act = () => CompileAndBind(
            GroupsSql, new List<object?> { Row("id", null) });

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*missing optional_groups field*");
    }

    [Fact]
    public void Empty_in_list_rejected()
    {
        Action act = () => CompileAndBind(
            GroupsInSql, new List<object?> { Row("ids", new List<object?>()) });

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*IN list cannot be empty*");
    }

    [Fact]
    public void Byte_array_row_value_binds_as_single_placeholder()
    {
        var blob = new byte[] { 1, 2 };
        var (sql, values) = CompileAndBind(
            GroupsSql, new List<object?> { Row("id", blob) });

        sql.Should().Be("select * from t where (id = ?)");
        values.Should().Equal(blob);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    [InlineData("[]")]
    public void Blob_absence_spellings(string? value)
    {
        ParamRuntimeUtil.NullableValueIsAbsent("blob", value).Should().BeTrue();
    }

    [Fact]
    public void Empty_list_blob_is_absent()
    {
        ParamRuntimeUtil.NullableValueIsAbsent("blob", new List<object?>())
            .Should().BeTrue();
    }

    [Fact]
    public void Populated_blob_is_present()
    {
        ParamRuntimeUtil.NullableValueIsAbsent(
            "blob", new List<object?> { Row("id", 1L) }).Should().BeFalse();
        ParamRuntimeUtil.NullableValueIsAbsent("blob", "[{\"id\": 1}]").Should().BeFalse();
    }

    [Fact]
    public void Malformed_blob_is_not_treated_as_absent()
    {
        ParamRuntimeUtil.NullableValueIsAbsent("blob", "not json").Should().BeFalse();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("[]")]
    public void Zero_rows_elides_whole_clause(string? pairs)
    {
        object? value = pairs;
        if (pairs == null)
            value = new List<object?>();
        var (sql, values) = CompileAndBind(GroupsSql, value);
        sql.Should().Be("select * from t");
        values.Should().BeEmpty();
    }

    [Fact]
    public void Null_blob_elides_clause_through_helper()
    {
        var (sql, values) = CompileAndBind(GroupsSql, null);
        sql.Should().Be("select * from t");
        values.Should().BeEmpty();
    }

    [Fact]
    public void Missing_blob_value_errors()
    {
        var shape = new Shape(data: new Dictionary<string, object?>());
        Action act = () => GroupRuntimeUtil.GroupShapesForCompile(
            Twig(GroupsSql), shape.GetProp, Array.Empty<string>());

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*missing blob value*");
    }

    [Fact]
    public void Row_count_change_recompiles()
    {
        var helper = new DataProviderHelper();
        var twig = Twig(GroupsSql);
        var one = helper.GetExecutableContent(
            "?", twig, ShapeWith(new List<object?> { Row("id", 1L) }));
        var two = helper.GetExecutableContent(
            "?", twig, ShapeWith(new List<object?> { Row("id", 1L), Row("id", 2L) }));

        NormalizeWs(one.Content).Should().NotBe(NormalizeWs(two.Content));
    }

    [Fact]
    public void Array_param_rejects_string_value()
    {
        var twig = Twig("--(ids integer[])--\nselect * from t where id in ({{ids}})\n");
        var shape = new Shape(data: new Dictionary<string, object?> { ["ids"] = "abc" });

        Action act = () => ParamRuntimeUtil.ArrayLengthsForCompile(
            twig, shape.GetProp, Array.Empty<string>());

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*must be a sequence*");
    }

    [Fact]
    public void Array_param_accepts_list_value()
    {
        var twig = Twig("--(ids integer[])--\nselect * from t where id in ({{ids}})\n");
        var shape = new Shape(data: new Dictionary<string, object?>
        {
            ["ids"] = new List<object?> { 1L, 2L, 3L },
        });

        var lengths = ParamRuntimeUtil.ArrayLengthsForCompile(
            twig, shape.GetProp, Array.Empty<string>());

        lengths["ids"].Should().Be(3);
    }
}
