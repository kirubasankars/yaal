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
        "--(pairs blob)--\nselect * from t where optional_groups_or({{pairs}}, id = {{id}})\n";

    private const string GroupsInSql =
        "--(pairs blob)--\nselect * from t where optional_groups_or({{pairs}}, id in ({{ids}}))\n";

    private const string TwoGroupsAndSql =
        "--(pairs blob, pairs1 blob)--\nselect * from t where optional_groups_or({{pairs}}, id = {{id}})" +
        " and optional_groups_or({{pairs1}}, id = {{id}})\n";

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

    private static (string Sql, List<object?> Values) CompileAndBindTwo(
        object? pairs, object? pairs1)
    {
        var helper = new DataProviderHelper();
        var shape = new Shape(data: new Dictionary<string, object?>
        {
            ["pairs"] = pairs,
            ["pairs1"] = pairs1,
        });
        var compiled = helper.GetExecutableContent("?", Twig(TwoGroupsAndSql), shape);
        var values = helper.BuildParameters(compiled, shape, (_, v) => v);
        return (NormalizeWs(compiled.Content), values);
    }

    private const string ArgsTemplateSql =
        "--($args.pairs blob, $args.flag integer)--\nselect * from t where optional_groups_or({{$args.pairs}}," +
        " id = {{id}} and flag = {{$args.flag}})\n";

    // $args.* resolves through the extras shape; Shape data may not hold $-prefixed keys.
    private static (string Sql, List<object?> Values) CompileAndBindArgs(
        string sql, Dictionary<string, object?> args)
    {
        var helper = new DataProviderHelper();
        var shape = new Shape(extras: new Dictionary<string, Shape>
        {
            ["$args"] = new Shape(data: args),
        });
        var compiled = helper.GetExecutableContent("?", Twig(sql), shape);
        var values = helper.BuildParameters(compiled, shape, (_, v) => v);
        return (NormalizeWs(compiled.Content), values);
    }

    private static List<string> GroupFieldsOf(string sql) =>
        Twig(sql).Content.First(t => t.Type == "brace" && t.GroupSource != null).GroupFields!;

    [Fact]
    public void Undeclared_args_name_in_body_errors()
    {
        Action act = () => Twig(
            "--($args.pairs blob)--\nselect * from t where optional_groups_or({{$args.pairs}}," +
            " id = {{id}} and other = {{$args.nope}})\n");
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*type missing for {{$args.nope}}*");
    }

    [Fact]
    public void Body_with_only_args_placeholders_errors()
    {
        Action act = () => Twig(
            "--($args.pairs blob)--\nselect * from t where optional_groups_or({{$args.pairs}}," +
            " id = {{$args.id}})\n");
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*at least one*");
    }

    [Fact]
    public void Bare_field_beside_declared_args_param()
    {
        GroupFieldsOf(
            "--($args.pairs blob, $args.id integer)--\nselect * from t where" +
            " optional_groups_or({{$args.pairs}}, a = {{id}})\n")
            .Should().Equal("id");
    }

    [Fact]
    public void Declared_header_param_repeats_per_branch()
    {
        var (sql, values) = CompileAndBindArgs(ArgsTemplateSql, new Dictionary<string, object?>
        {
            ["pairs"] = new List<object?> { Row("id", 1L), Row("id", 2L) },
            ["flag"] = 9L,
        });

        sql.Should().Be("select * from t where ((id = ? and flag = ?) or (id = ? and flag = ?))");
        values.Should().Equal(1L, 9L, 2L, 9L);
    }

    [Fact]
    public void Header_param_is_not_a_row_key()
    {
        GroupFieldsOf(ArgsTemplateSql).Should().Equal("id");
    }

    [Fact]
    public void Args_row_key_with_per_row_in_list()
    {
        var (sql, values) = CompileAndBindArgs(
            "--($args.pairs blob)--\nselect * from t where optional_groups_or({{$args.pairs}}, id in ({{ids}}))\n",
            new Dictionary<string, object?>
            {
                ["pairs"] = new List<object?>
                {
                    Row("ids", new List<object?> { 1L, 2L }),
                    Row("ids", new List<object?> { 3L }),
                },
            });

        sql.Should().Be("select * from t where ((id in (?, ?)) or (id in (?)))");
        values.Should().Equal(1L, 2L, 3L);
    }

    [Fact]
    public void Blob_source_in_body_rejected()
    {
        Action act = () => Twig(
            "--($args.pairs blob)--\nselect * from t where optional_groups_or({{$args.pairs}}," +
            " id = {{$args.pairs}})\n");
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*must not be used in its body*");
    }

    [Fact]
    public void Array_header_param_in_body_rejected()
    {
        Action act = () => Twig(
            "--($args.pairs blob, $args.ids integer[])--\nselect * from t where optional_groups_or({{$args.pairs}}," +
            " id in ({{$args.ids}}))\n");
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*cannot use array parameter*");
    }

    [Fact]
    public void Bare_row_key_declared_bare_in_header_rejected()
    {
        Action act = () => Twig(
            "--(pairs blob, id integer)--\nselect * from t where optional_groups_or({{pairs}}," +
            " col1 = {{id}})\n");
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*must not be declared*");
    }

    [Fact]
    public void Multi_row_group_is_parenthesized_beside_and()
    {
        var (sql, values) = CompileAndBindTwo(
            new List<object?> { Row("id", 1L), Row("id", 3L) },
            new List<object?> { Row("id", 2L) });

        sql.Should().Be("select * from t where ((id = ?) or (id = ?)) and (id = ?)");
        values.Should().Equal(1L, 3L, 2L);
    }

    [Fact]
    public void Single_row_group_is_not_double_wrapped()
    {
        var (sql, values) = CompileAndBindTwo(
            new List<object?> { Row("id", 1L) },
            new List<object?> { Row("id", 2L) });

        sql.Should().Be("select * from t where (id = ?) and (id = ?)");
        values.Should().Equal(1L, 2L);
    }

    [Fact]
    public void Elided_group_leaves_other_group_intact()
    {
        var (sql, values) = CompileAndBindTwo(
            new List<object?> { Row("id", 1L), Row("id", 3L) },
            null);

        sql.Should().Be("select * from t where ((id = ?) or (id = ?))");
        values.Should().Equal(1L, 3L);
    }

    private const string NestedGroupsSql =
        "--($args.pairs blob, $args.x integer)--\nselect * from t where z = 1 and optional(a = {{$args.x}}" +
        " and optional_groups_or({{$args.pairs}}, id = {{id}}))\n";

    private const string OptionalInGroupBodySql =
        "--($args.pairs blob, $args.flag integer)--\nselect * from t where optional_groups_or({{$args.pairs}}," +
        " col1 = {{cv1}} and optional(col2 = {{$args.flag}}))\n";

    private static List<string> OptionalParamsOf(string sql) =>
        Twig(sql).Content
            .First(t => t.Type == "brace" && t.NullableParameters is { Count: > 0 })
            .NullableParameters!;

    [Fact]
    public void Nested_group_does_not_gate_the_optional()
    {
        OptionalParamsOf(NestedGroupsSql).Should().Equal("$args.x");
    }

    [Fact]
    public void Nested_multi_row_group_keeps_its_own_parentheses()
    {
        var (sql, values) = CompileAndBindArgs(NestedGroupsSql, new Dictionary<string, object?>
        {
            ["pairs"] = new List<object?> { Row("id", 1L), Row("id", 2L) },
            ["x"] = 5L,
        });

        sql.Should().Be("select * from t where z = 1 and (a = ? and ((id = ?) or (id = ?)))");
        values.Should().Equal(5L, 1L, 2L);
    }

    [Fact]
    public void Nested_single_row_group_is_not_double_wrapped()
    {
        var (sql, values) = CompileAndBindArgs(NestedGroupsSql, new Dictionary<string, object?>
        {
            ["pairs"] = new List<object?> { Row("id", 1L) },
            ["x"] = 5L,
        });

        sql.Should().Be("select * from t where z = 1 and (a = ? and (id = ?))");
        values.Should().Equal(5L, 1L);
    }

    [Fact]
    public void Nested_group_leading_inside_the_optional()
    {
        var (sql, values) = CompileAndBindArgs(
            "--($args.pairs blob, $args.x integer)--\nselect * from t where" +
            " optional(optional_groups_or({{$args.pairs}}, id = {{id}}) and a = {{$args.x}})\n",
            new Dictionary<string, object?>
            {
                ["pairs"] = new List<object?> { Row("id", 1L), Row("id", 7L) },
                ["x"] = 5L,
            });

        sql.Should().Be("select * from t where (((id = ?) or (id = ?)) and a = ?)");
        values.Should().Equal(1L, 7L, 5L);
    }

    [Fact]
    public void Nested_group_all_absent_elides_the_whole_optional()
    {
        var (sql, values) = CompileAndBindArgs(
            NestedGroupsSql, new Dictionary<string, object?>());

        sql.Should().Be("select * from t where z = 1");
        values.Should().BeEmpty();
    }

    [Fact]
    public void Nested_group_blob_absent_keeps_the_rest_of_the_optional()
    {
        var (sql, values) = CompileAndBindArgs(NestedGroupsSql, new Dictionary<string, object?>
        {
            ["x"] = 5L,
        });

        sql.Should().Be("select * from t where z = 1 and (a = ?)");
        values.Should().Equal(5L);
    }

    private const string GroupOnlyOptionalSql =
        "--($args.pairs blob)--\nselect * from t where z = 1 and" +
        " optional(optional_groups_or({{$args.pairs}}, id = {{id}}))\n";

    [Fact]
    public void Optional_wrapping_only_a_group()
    {
        var (sql, values) = CompileAndBindArgs(
            GroupOnlyOptionalSql, new Dictionary<string, object?>
            {
                ["pairs"] = new List<object?> { Row("id", 1L), Row("id", 2L) },
            });

        sql.Should().Be("select * from t where z = 1 and (((id = ?) or (id = ?)))");
        values.Should().Equal(1L, 2L);
    }

    [Fact]
    public void Optional_wrapping_only_a_group_elides_without_empty_parens()
    {
        var (absent, absentValues) = CompileAndBindArgs(
            GroupOnlyOptionalSql, new Dictionary<string, object?>());
        absent.Should().Be("select * from t where z = 1");
        absentValues.Should().BeEmpty();

        var (empty, emptyValues) = CompileAndBindArgs(
            GroupOnlyOptionalSql, new Dictionary<string, object?>
            {
                ["pairs"] = new List<object?>(),
            });
        empty.Should().Be("select * from t where z = 1");
        emptyValues.Should().BeEmpty();
    }

    [Fact]
    public void Optional_wrapping_only_a_group_drops_sole_where()
    {
        var (sql, values) = CompileAndBindArgs(
            "--($args.pairs blob)--\nselect * from t where" +
            " optional(optional_groups_or({{$args.pairs}}, id = {{id}}))\n",
            new Dictionary<string, object?>());

        sql.Should().Be("select * from t");
        values.Should().BeEmpty();
    }

    [Fact]
    public void Optional_inside_a_group_body_keys_off_header_param()
    {
        OptionalParamsOf(OptionalInGroupBodySql).Should().Equal("$args.flag");

        var (sql, values) = CompileAndBindArgs(
            OptionalInGroupBodySql, new Dictionary<string, object?>
            {
                ["pairs"] = new List<object?> { Row("cv1", 7L), Row("cv1", 8L) },
                ["flag"] = 1L,
            });

        sql.Should().Be(
            "select * from t where ((col1 = ? and (col2 = ?)) or (col1 = ? and (col2 = ?)))");
        values.Should().Equal(7L, 1L, 8L, 1L);
    }

    [Fact]
    public void Optional_inside_a_group_body_elides_in_every_branch()
    {
        var (sql, values) = CompileAndBindArgs(
            OptionalInGroupBodySql, new Dictionary<string, object?>
            {
                ["pairs"] = new List<object?> { Row("cv1", 7L), Row("cv1", 8L) },
            });

        sql.Should().Be("select * from t where ((col1 = ?) or (col1 = ?))");
        values.Should().Equal(7L, 8L);
    }

    [Fact]
    public void Optional_inside_a_group_body_on_row_fields_only_rejected()
    {
        Action act = () => Twig(
            "--(pairs blob)--\nselect * from t where optional_groups_or({{pairs}}," +
            " optional(col1 = {{cv1}}))\n");
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*inside optional_groups*");
    }

    [Fact]
    public void List_of_rows_compiles_and_binds()
    {
        var (sql, values) = CompileAndBind(
            GroupsSql,
            new List<object?> { Row("id", 1L), Row("id", 2L) });

        sql.Should().Be("select * from t where ((id = ?) or (id = ?))");
        values.Should().Equal(1L, 2L);
    }

    [Fact]
    public void Json_string_blob_compiles_and_binds()
    {
        var (sql, values) = CompileAndBind(GroupsSql, "[{\"id\": 1}, {\"id\": 2}]");

        sql.Should().Be("select * from t where ((id = ?) or (id = ?))");
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

        sql.Should().Be("select * from t where ((id in (?, ?)) or (id in (?)))");
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

    private const string WhenSql =
        "--($args.apply bool, $args.id integer)--\nselect * from t where z = 1" +
        " and optional_when({{$args.apply}}, u.user_id = {{$args.id}})\n";

    [Fact]
    public void Optional_when_condition_and_body_param_are_both_nullable()
    {
        Twig(WhenSql).Nullable.Should().Equal("$args.apply", "$args.id");
    }

    [Fact]
    public void Optional_when_condition_is_declared_but_never_bound()
    {
        var (sql, values) = CompileAndBindArgs(WhenSql, new Dictionary<string, object?>
        {
            ["apply"] = true,
            ["id"] = 7L,
        });

        sql.Should().Be("select * from t where z = 1 and (u.user_id = ?)");
        values.Should().Equal(7L);
    }

    [Fact]
    public void Optional_when_condition_absent_drops_the_block()
    {
        var (sql, values) = CompileAndBindArgs(WhenSql, new Dictionary<string, object?>
        {
            ["id"] = 7L,
        });

        sql.Should().Be("select * from t where z = 1");
        values.Should().BeEmpty();
    }

    [Fact]
    public void Optional_when_false_condition_keeps_the_block()
    {
        var (sql, values) = CompileAndBindArgs(WhenSql, new Dictionary<string, object?>
        {
            ["apply"] = false,
            ["id"] = 7L,
        });

        sql.Should().Be("select * from t where z = 1 and (u.user_id = ?)");
        values.Should().Equal(7L);
    }

    [Fact]
    public void Optional_when_body_param_absent_drops_the_block()
    {
        var (sql, values) = CompileAndBindArgs(WhenSql, new Dictionary<string, object?>
        {
            ["apply"] = true,
        });

        sql.Should().Be("select * from t where z = 1");
        values.Should().BeEmpty();
    }

    [Fact]
    public void Optional_when_absent_condition_skips_the_body_partial_check()
    {
        const string sql =
            "--($args.apply bool, $args.a integer, $args.b integer)--\nselect * from t where" +
            " optional_when({{$args.apply}}, a = {{$args.a}} and b = {{$args.b}})\n";

        Action act = () => CompileAndBindArgs(sql, new Dictionary<string, object?>
        {
            ["apply"] = true,
            ["a"] = 1L,
        });
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*partial parameters: $args.b*");

        var (elided, values) = CompileAndBindArgs(sql, new Dictionary<string, object?>
        {
            ["a"] = 1L,
        });
        elided.Should().Be("select * from t");
        values.Should().BeEmpty();
    }

    [Fact]
    public void Optional_when_wrapping_only_a_group()
    {
        const string sql =
            "--($args.apply bool, $args.pairs blob)--\nselect * from t where z = 1 and" +
            " optional_when({{$args.apply}}, optional_groups_or({{$args.pairs}}, id = {{id}}))\n";

        var (kept, keptValues) = CompileAndBindArgs(sql, new Dictionary<string, object?>
        {
            ["apply"] = true,
            ["pairs"] = new List<object?> { Row("id", 1L), Row("id", 2L) },
        });
        kept.Should().Be("select * from t where z = 1 and (((id = ?) or (id = ?)))");
        keptValues.Should().Equal(1L, 2L);

        var (noRows, noRowsValues) = CompileAndBindArgs(sql, new Dictionary<string, object?>
        {
            ["apply"] = true,
        });
        noRows.Should().Be("select * from t where z = 1");
        noRowsValues.Should().BeEmpty();

        var (noCond, noCondValues) = CompileAndBindArgs(sql, new Dictionary<string, object?>
        {
            ["pairs"] = new List<object?> { Row("id", 1L) },
        });
        noCond.Should().Be("select * from t where z = 1");
        noCondValues.Should().BeEmpty();
    }

    [Fact]
    public void Optional_when_inside_a_group_body_may_key_off_row_fields_only()
    {
        const string sql =
            "--($args.pairs blob, $args.flag bool)--\nselect * from t where" +
            " optional_groups_or({{$args.pairs}}, c1 = {{cv}}" +
            " and optional_when({{$args.flag}}, c2 = {{cv2}}))\n";

        var rows = new List<object?>
        {
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["cv"] = 1L,
                ["cv2"] = 10L,
            },
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["cv"] = 2L,
                ["cv2"] = 20L,
            },
        };

        var (kept, keptValues) = CompileAndBindArgs(sql, new Dictionary<string, object?>
        {
            ["pairs"] = rows,
            ["flag"] = true,
        });
        kept.Should().Be(
            "select * from t where ((c1 = ? and (c2 = ?)) or (c1 = ? and (c2 = ?)))");
        keptValues.Should().Equal(1L, 10L, 2L, 20L);

        var (elided, elidedValues) = CompileAndBindArgs(sql, new Dictionary<string, object?>
        {
            ["pairs"] = rows,
        });
        elided.Should().Be("select * from t where ((c1 = ?) or (c1 = ?))");
        elidedValues.Should().Equal(1L, 2L);
    }

    [Fact]
    public void Optional_when_inside_a_group_body_rejects_a_bare_condition()
    {
        Action act = () => Twig(
            "--($args.pairs blob, flag bool)--\nselect * from t where" +
            " optional_groups_or({{$args.pairs}}, c1 = {{cv}}" +
            " and optional_when({{flag}}, c2 = {{cv2}}))\n");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*must use a {{$args.param}} condition*");
    }

    [Fact]
    public void Nested_optional_when_is_rejected()
    {
        Action act = () => Twig(
            "--($args.a bool, $args.b bool, $args.x integer)--\nselect * from t where" +
            " optional_when({{$args.a}}, optional_when({{$args.b}}, c = {{$args.x}}))\n");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*nested optional_when*");
    }

    private const string GroupsAndSql =
        "--(pairs blob)--\nselect * from t where optional_groups_and({{pairs}}, id = {{id}})\n";

    [Fact]
    public void Or_and_and_differ_only_in_the_row_joiner()
    {
        var rows = new List<object?> { Row("id", 1L), Row("id", 2L) };
        var (orSql, orValues) = CompileAndBind(GroupsSql, rows);
        var (andSql, andValues) = CompileAndBind(GroupsAndSql, rows);

        orSql.Should().Be("select * from t where ((id = ?) or (id = ?))");
        andSql.Should().Be("select * from t where ((id = ?) and (id = ?))");
        andValues.Should().Equal(orValues);
    }

    [Fact]
    public void And_single_row_is_not_double_wrapped()
    {
        var (sql, values) = CompileAndBind(
            GroupsAndSql, new List<object?> { Row("id", 7L) });

        sql.Should().Be("select * from t where (id = ?)");
        values.Should().Equal(7L);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("[]")]
    public void And_blob_absence_elides_like_or(object? pairs)
    {
        var (sql, values) = CompileAndBind(GroupsAndSql, pairs);

        sql.Should().Be("select * from t");
        values.Should().BeEmpty();
    }

    [Theory]
    [InlineData(GroupsSql, "or")]
    [InlineData(GroupsAndSql, "and")]
    public void Group_join_metadata_is_recorded(string sql, string expected)
    {
        Twig(sql).Content.First(t => t.GroupSource != null).GroupJoin.Should().Be(expected);
    }

    [Fact]
    public void Legacy_optional_groups_spelling_is_rejected()
    {
        Action act = () => Twig(
            "--(pairs blob)--\nselect * from t where optional_groups({{pairs}}, id = {{id}})\n");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*optional_groups(...) was renamed*optional_groups_or(...)*optional_groups_and(...)*");
    }

    [Theory]
    [InlineData("optional_groups_or", "optional_groups_or")]
    [InlineData("optional_groups_or", "optional_groups_and")]
    [InlineData("optional_groups_and", "optional_groups_or")]
    [InlineData("optional_groups_and", "optional_groups_and")]
    public void Nested_groups_rejected_for_either_keyword(string outer, string inner)
    {
        Action act = () => Twig(
            "--(pairs blob, pairs1 blob)--\nselect * from t where " + outer +
            "({{pairs}}, " + inner + "({{pairs1}}, id = {{id}}))\n");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*nested optional_groups*");
    }
}
