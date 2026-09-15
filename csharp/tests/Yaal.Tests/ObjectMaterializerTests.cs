// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Microsoft.Data.Sqlite;

namespace Yaal.Tests;

public class ObjectMaterializerTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _flagsPath;
    private readonly Yaal _yaal;

    private static string RepoRoot =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", ".."));

    private static string FixtureApi => Path.Combine(RepoRoot, "tests", "fixtures", "api");

    public ObjectMaterializerTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), "yaal-poco-" + Guid.NewGuid().ToString("N") + ".db");
        _flagsPath = Path.Combine(Path.GetTempPath(), "yaal-poco-flags-" + Guid.NewGuid().ToString("N") + ".db");

        using (var con = new SqliteConnection("Data Source=" + _dbPath))
        {
            con.Open();
            using var cmd = con.CreateCommand();
            cmd.CommandText = File.ReadAllText(Path.Combine(RepoRoot, "docker", "sqlite", "schema.sql"));
            cmd.ExecuteNonQuery();
        }

        using (var con = new SqliteConnection("Data Source=" + _flagsPath))
        {
            con.Open();
            using var cmd = con.CreateCommand();
            cmd.CommandText = File.ReadAllText(Path.Combine(RepoRoot, "docker", "sqlite", "flags_schema.sql"));
            cmd.ExecuteNonQuery();
        }

        _yaal = new Yaal(FixtureApi, debug: true);
        _yaal.SetupDataProvider("db", "sqlite3:///" + _dbPath);
        _yaal.SetupDataProvider("flags", "sqlite3:///" + _flagsPath);
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_flagsPath); } catch { /* ignore */ }
    }

    [Fact]
    public void Map_scalar_and_nested_properties()
    {
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 1L,
            ["name"] = "admin",
            ["roles"] = new List<object?>
            {
                new Dictionary<string, object?> { ["id"] = 10L, ["name"] = "Administrator" },
                new Dictionary<string, object?> { ["id"] = 20L, ["name"] = "User" },
            },
        };

        var user = ObjectMaterializer.Map<UserDto>(shaped);
        user.Id.Should().Be(1);
        user.Name.Should().Be("admin");
        user.Roles.Should().HaveCount(2);
        user.Roles![0].Name.Should().Be("Administrator");
    }

    [Fact]
    public void MapInto_preserves_unmapped_properties_and_reuses_list()
    {
        var existingRole = new RoleDto { Id = 999, Name = "keep-me" };
        var user = new UserDto
        {
            Id = 42,
            Name = "placeholder",
            Roles = new List<RoleDto> { existingRole },
        };

        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 1L,
            ["name"] = "admin",
            ["roles"] = new List<object?>
            {
                new Dictionary<string, object?> { ["id"] = 10L, ["name"] = "Administrator" },
            },
        };

        ObjectMaterializer.MapInto(shaped, user);

        user.Id.Should().Be(1);
        user.Name.Should().Be("admin");
        user.Roles.Should().HaveCount(1);
        user.Roles![0].Name.Should().Be("Administrator");
        user.Roles[0].Should().NotBeSameAs(existingRole);
    }

    [Fact]
    public void MapList_maps_each_row()
    {
        var shaped = new List<object?>
        {
            new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "a", ["active"] = 1L },
            new Dictionary<string, object?> { ["id"] = 2L, ["name"] = "b", ["active"] = 0L },
        };

        var users = ObjectMaterializer.MapList<UserRowDto>(shaped);
        users.Should().HaveCount(2);
        users[0].Name.Should().Be("a");
        users[1].Id.Should().Be(2);
    }

    [Fact]
    public void ThrowIfErrors_raises_for_soft_errors()
    {
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["errors"] = new List<object?>
            {
                new Dictionary<string, object?> { ["message"] = "bad input" },
            },
        };

        Action act = () => ObjectMaterializer.ThrowIfErrors(shaped);
        act.Should().Throw<YaalQueryException>()
            .Which.Errors.Should().ContainSingle(e => e["message"]!.ToString() == "bad input");
    }

    [Fact]
    public void Materialize_static_helpers_work()
    {
        var shaped = new Dictionary<string, object?> { ["id"] = 7L, ["name"] = "x" };
        var user = Yaal.Materialize<UserRowDto>(shaped);
        user.Id.Should().Be(7);
    }

    [Fact]
    public void Query_User_get_materializes_object_with_roles()
    {
        var user = _yaal.Query<UserDto>("user/get", args: new { id = 1 });
        user.Id.Should().Be(1);
        user.Name.Should().Be("admin");
        user.Roles.Should().NotBeNull().And.HaveCount(2);
        user.Roles!.Select(r => r.Name).Should().Contain(new[] { "Administrator", "User" });
    }

    [Fact]
    public void QueryList_user_list_materializes_rows()
    {
        var users = _yaal.QueryList<UserRowDto>("user/list", args: new { active = 1 });
        users.Should().NotBeEmpty();
        users.Should().OnlyContain(u => u.Active == 1);
    }

    [Fact]
    public void Query_User_nested_materializes_child_sql_graph()
    {
        var user = _yaal.Query<UserDto>("user/nested", args: new { id = 1 });
        user.Id.Should().Be(1);
        user.Roles.Should().NotBeNull().And.HaveCount(2);
    }

    [Fact]
    public void QueryInto_hydrates_existing_instance()
    {
        var user = new UserDto { Name = "placeholder" };
        var same = _yaal.QueryInto("user/get", user, args: new { id = 1 });

        same.Should().BeSameAs(user);
        user.Id.Should().Be(1);
        user.Name.Should().Be("admin");
        user.Roles.Should().NotBeNull().And.HaveCount(2);
    }

    [Fact]
    public void QueryInto_rejects_array_descriptor()
    {
        var row = new UserRowDto();
        Action act = () => _yaal.QueryInto("user/list", row, args: new { active = 1 });
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*array*");
    }

    [Fact]
    public void Map_ignores_extra_dict_keys()
    {
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 1L,
            ["name"] = "admin",
            ["extra"] = "ignored",
        };

        var user = ObjectMaterializer.Map<UserRowDto>(shaped);
        user.Id.Should().Be(1);
        user.Name.Should().Be("admin");
    }

    [Fact]
    public void Map_case_insensitive_property_names()
    {
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["ID"] = 5L,
            ["NAME"] = "x",
        };

        var user = ObjectMaterializer.Map<UserRowDto>(shaped);
        user.Id.Should().Be(5);
        user.Name.Should().Be("x");
    }

    [Fact]
    public void MapList_empty_returns_empty_list()
    {
        ObjectMaterializer.MapList<UserRowDto>(null).Should().BeEmpty();
        ObjectMaterializer.MapList<UserRowDto>(new List<object?>()).Should().BeEmpty();
    }

    [Fact]
    public void Map_null_nested_property_sets_null()
    {
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 1L,
            ["name"] = "admin",
            ["roles"] = null,
        };

        var user = ObjectMaterializer.Map<UserDto>(shaped);
        user.Roles.Should().BeNull();
    }

    [Fact]
    public void MapInto_creates_list_when_property_null()
    {
        var user = new UserDto { Id = 99, Roles = null };
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 1L,
            ["roles"] = new List<object?>
            {
                new Dictionary<string, object?> { ["id"] = 10L, ["name"] = "Administrator" },
            },
        };

        ObjectMaterializer.MapInto(shaped, user);

        user.Roles.Should().NotBeNull().And.HaveCount(1);
        user.Roles![0].Name.Should().Be("Administrator");
    }

    [Fact]
    public void MaterializeInto_static_helper()
    {
        var user = new UserDto { Name = "before" };
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 3L,
            ["name"] = "after",
        };

        Yaal.MaterializeInto(shaped, user);

        user.Id.Should().Be(3);
        user.Name.Should().Be("after");
    }

    [Fact]
    public void Query_List_syntax_for_array()
    {
        var users = _yaal.Query<List<UserRowDto>>("user/list", args: new { active = 1 });
        users.Should().HaveCount(2);
    }

    [Fact]
    public void Query_user_get_single_role()
    {
        var user = _yaal.Query<UserDto>("user/get", args: new { id = 2 });
        user.Id.Should().Be(2);
        user.Name.Should().Be("guest");
        user.Roles.Should().NotBeNull().And.HaveCount(1);
        user.Roles![0].Name.Should().Be("User");
    }

    [Fact]
    public void QueryList_empty_filter()
    {
        var users = _yaal.QueryList<UserRowDto>("user/list", args: new { active = 0 });
        users.Should().BeEmpty();
    }

    [Fact]
    public void Map_null_throws()
    {
        Action act = () => ObjectMaterializer.Map<UserRowDto>(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Map_list_instead_of_dict_throws()
    {
        Action act = () => ObjectMaterializer.Map<UserRowDto>(new List<object?>());
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*shaped object*");
    }

    [Fact]
    public void MapInto_null_target_throws()
    {
        var shaped = new Dictionary<string, object?> { ["id"] = 1L };
        Action act = () => ObjectMaterializer.MapInto(shaped, null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void MapInto_list_instead_of_dict_throws()
    {
        var user = new UserDto();
        Action act = () => ObjectMaterializer.MapInto(new List<object?>(), user);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*shaped object*");
    }

    [Fact]
    public void MapList_scalar_throws()
    {
        Action act = () => ObjectMaterializer.MapList<UserRowDto>("not-a-list");
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*shaped array*");
    }

    [Fact]
    public void Map_nested_list_wrong_item_type_throws()
    {
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 1L,
            ["roles"] = new List<object?> { 1, 2 },
        };

        Action act = () => ObjectMaterializer.Map<UserDto>(shaped);
        act.Should().Throw<Exception>();
    }

    [Fact]
    public void Map_type_without_parameterless_ctor_throws()
    {
        var shaped = new Dictionary<string, object?> { ["value"] = 1L };

        Action act = () => ObjectMaterializer.Map<NoDefaultCtorDto>(shaped);
        act.Should().Throw<Exception>();
    }

    [Fact]
    public void Materialize_null_throws()
    {
        Action act = () => Yaal.Materialize<UserRowDto>(null);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Query_object_descriptor_with_list_generic_on_array_fails()
    {
        Action act = () => _yaal.Query<UserRowDto>("user/list", args: new { active = 1 });
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*QueryList*");
    }

    [Fact]
    public void QueryList_on_object_descriptor_fails()
    {
        Action act = () => _yaal.QueryList<UserDto>("user/get", args: new { id = 1 });
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*object*");
    }

    [Fact]
    public void QueryInto_null_into_throws()
    {
        Action act = () => _yaal.QueryInto<UserDto>("user/get", null!, args: new { id = 1 });
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Query_soft_validation_errors_throw()
    {
        Action act = () => _yaal.Query<UserRowDto>("user/list", args: new { sort = "nope" });
        act.Should().Throw<YaalQueryException>()
            .Which.Errors.Should().NotBeEmpty();
    }

    [Fact]
    public void QueryList_soft_errors_throw()
    {
        Action act = () => _yaal.QueryList<UserRowDto>("user/list", args: new { sort = "nope" });
        act.Should().Throw<YaalQueryException>()
            .Which.Errors.Should().NotBeEmpty();
    }

    [Fact]
    public void QueryInto_soft_errors_throw_without_mutating_target()
    {
        var user = new UserDto { Id = 99, Name = "unchanged" };
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["errors"] = new List<object?>
            {
                new Dictionary<string, object?> { ["message"] = "bad input" },
            },
        };

        Action act = () =>
        {
            ObjectMaterializer.ThrowIfErrors(shaped);
            ObjectMaterializer.MapInto(shaped, user);
        };

        act.Should().Throw<YaalQueryException>();
        user.Id.Should().Be(99);
        user.Name.Should().Be("unchanged");
        user.Roles.Should().BeNull();
    }

    [Fact]
    public void Map_converts_enum_guid_and_datetime()
    {
        var guid = Guid.Parse("a1111111-1111-1111-1111-111111111111");
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["status"] = "Active",
            ["id"] = guid.ToString(),
            ["created"] = "2024-01-15T10:30:00",
        };

        var dto = ObjectMaterializer.Map<ConversionDto>(shaped);
        dto.Status.Should().Be(StatusEnum.Active);
        dto.Id.Should().Be(guid);
        dto.Created.Should().Be(new DateTime(2024, 1, 15, 10, 30, 0));
    }

    [Fact]
    public void Map_invalid_guid_throws()
    {
        var shaped = new Dictionary<string, object?> { ["id"] = "not-a-guid" };
        Action act = () => ObjectMaterializer.Map<ConversionDto>(shaped);
        act.Should().Throw<FormatException>();
    }

    [Fact]
    public void Map_incompatible_type_throws()
    {
        var shaped = new Dictionary<string, object?> { ["count"] = "not-a-number" };
        Action act = () => ObjectMaterializer.Map<NullableDto>(shaped);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Cannot convert*");
    }

    [Fact]
    public void Map_empty_dict_produces_default_poco()
    {
        var user = ObjectMaterializer.Map<UserRowDto>(new Dictionary<string, object?>());
        user.Id.Should().Be(0);
        user.Name.Should().BeNull();
    }

    [Fact]
    public void MapInto_reuses_existing_nested_object()
    {
        var profile = new ProfileDto { Bio = "keep-me" };
        var user = new UserWithProfileDto { Profile = profile };
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 1L,
            ["profile"] = new Dictionary<string, object?> { ["name"] = "admin" },
        };

        ObjectMaterializer.MapInto(shaped, user);

        user.Profile.Should().BeSameAs(profile);
        profile.Name.Should().Be("admin");
        profile.Bio.Should().Be("keep-me");
    }

    [Fact]
    public void MapInto_leaves_missing_top_level_properties_unchanged()
    {
        var user = new UserDto { Id = 42, Name = "unchanged" };
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 1L,
        };

        ObjectMaterializer.MapInto(shaped, user);

        user.Id.Should().Be(1);
        user.Name.Should().Be("unchanged");
    }

    [Fact]
    public void Map_array_property_materializes()
    {
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["items"] = new List<object?>
            {
                new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "a" },
            },
        };

        var dto = ObjectMaterializer.Map<ArrayPropDto>(shaped);
        dto.Items.Should().NotBeNull().And.HaveCount(1);
        dto.Items![0].Name.Should().Be("a");
    }

    [Fact]
    public void ThrowIfErrors_single_dict_errors()
    {
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["errors"] = new Dictionary<string, object?> { ["message"] = "one error" },
        };

        Action act = () => ObjectMaterializer.ThrowIfErrors(shaped);
        act.Should().Throw<YaalQueryException>()
            .Which.Errors.Should().ContainSingle(e => e["message"]!.ToString() == "one error");
    }

    [Fact]
    public void Materialize_error_dict_throws()
    {
        var shaped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["errors"] = new List<object?>
            {
                new Dictionary<string, object?> { ["message"] = "fail" },
            },
        };

        Action act = () => Yaal.Materialize<UserRowDto>(shaped);
        act.Should().Throw<YaalQueryException>();
    }

    [Fact]
    public void Query_user_page_materializes_nested_branches()
    {
        var page = _yaal.Query<PageDto>("user/page", args: new { page = 1, page_size = 10 });
        page.Paging.Should().NotBeNull();
        page.Paging!.Page.Should().Be(1);
        page.Paging.PageSize.Should().Be(10);
        page.Data.Should().NotBeNull().And.NotBeEmpty();
        page.Data![0].Roles.Should().NotBeNull();
    }

    [Fact]
    public void Query_user_get_nonexistent_returns_default_poco()
    {
        var user = _yaal.Query<UserDto>("user/get", args: new { id = 9999 });
        user.Id.Should().Be(0);
        user.Name.Should().BeNull();
        user.Roles.Should().BeNull();
    }

    public enum StatusEnum
    {
        Active,
        Inactive,
    }

    public sealed class ConversionDto
    {
        public StatusEnum Status { get; set; }
        public Guid Id { get; set; }
        public DateTime Created { get; set; }
    }

    public sealed class NullableDto
    {
        public int Count { get; set; }
    }

    public sealed class ArrayPropDto
    {
        public RoleDto[]? Items { get; set; }
    }

    public sealed class PageDto
    {
        public PagingDto? Paging { get; set; }
        public List<UserDto>? Data { get; set; }
    }

    public sealed class ProfileDto
    {
        public string? Name { get; set; }
        public string? Bio { get; set; }
    }

    public sealed class UserWithProfileDto
    {
        public int Id { get; set; }
        public ProfileDto? Profile { get; set; }
    }

    public sealed class PagingDto
    {
        public int Page { get; set; }
        public int PageSize { get; set; }
        public int TotalCount { get; set; }
    }

    public sealed class NoDefaultCtorDto
    {
        public NoDefaultCtorDto(int value) => Value = value;
        public int Value { get; set; }
    }

    public sealed class UserDto
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public List<RoleDto>? Roles { get; set; }
    }

    public sealed class RoleDto
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    public sealed class UserRowDto
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public int Active { get; set; }
    }
}
