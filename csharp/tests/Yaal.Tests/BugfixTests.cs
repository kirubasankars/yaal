// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using System.Collections;
using System.Data;
using System.Data.Common;
using Yaal.Descriptors;
using Yaal.Execution;
using Yaal.Providers;
using Yaal.Sql;

namespace Yaal.Tests;

public class BugfixTests
{
    [Fact]
    public void Array_params_not_cached_across_items()
    {
        var helper = new DataProviderHelper();
        var sql = new CompiledSql
        {
            Content = "x",
            Parameters = new List<ParamDecl> { new() { Name = "id", Type = "integer" } },
        };
        helper.BuildParameters(sql, new Shape(data: new Dictionary<string, object?> { ["id"] = 1 }), (_, v) => v)
            .Should().Equal(1L);
        helper.BuildParameters(sql, new Shape(data: new Dictionary<string, object?> { ["id"] = 2 }), (_, v) => v)
            .Should().Equal(2L);
    }

    [Fact]
    public void Zero_is_converted()
    {
        var helper = new DataProviderHelper();
        var sql = new CompiledSql
        {
            Content = "x",
            Parameters = new List<ParamDecl> { new() { Name = "n", Type = "integer" } },
        };
        helper.BuildParameters(sql, new Shape(data: new Dictionary<string, object?> { ["n"] = 0 }), (_, v) => v)
            .Should().Equal(0L);
    }

    [Fact]
    public void Mode_error_cleans_up_connection()
    {
        var leak = new LeakProvider();
        var descriptor = new Branch
        {
            Path = "p",
            Connections = new List<string> { "db" },
            InputType = "object",
            Method = "$",
            Twigs = new List<Twig>
            {
                new() { Connection = "db", Content = new List<SqlToken>(), Parameters = new List<ParamDecl>() },
            },
            Model = new DescriptorModel { Output = null },
            OutputType = "array",
        };
        var ctx = ContextFactory.CreateContext(new Branch { Path = "p" });
        var (rows, errors) = Executor.ExecuteBranch(
            descriptor, true, new Dictionary<string, IDataProvider> { ["db"] = leak }, ctx,
            new List<IDictionary<string, object?>>());
        rows.Should().BeNull();
        errors.Should().NotBeNull().And.NotBeEmpty();
        leak.Begun.Should().BeTrue();
        leak.Ended.Should().BeFalse();
        leak.Errored.Should().BeTrue();
    }

    [Fact]
    public void Use_parent_rows_skips_twigs()
    {
        var dp = new CountingProvider();
        var branch = new Branch
        {
            InputType = "object",
            UseParentRows = true,
            Method = "$.roles",
            Name = "roles",
            Twigs = new List<Twig>
            {
                new() { Connection = "db", Content = new List<SqlToken>(), Parameters = new List<ParamDecl>() },
            },
            OutputType = "array",
        };
        var ctx = ContextFactory.CreateContext(new Branch { Path = "p" });
        var parent = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["user_id"] = 1, ["role_id"] = 1 },
        };
        var (rows, err) = Executor.ExecuteBranch(
            branch, false, new Dictionary<string, IDataProvider> { ["db"] = dp }, ctx, parent);
        err.Should().BeNull();
        dp.Calls.Should().Be(0);
        rows.Should().HaveCount(1);
        rows![0].Should().ContainKey("user_id");
    }

    [Fact]
    public void Clickhouse_provider_uses_injected_dbconnection_for_execute()
    {
        var fake = new FakeDbConnection();
        var provider = new ClickHouseDataProvider(() => fake);
        provider.Begin();

        var twig = new Twig
        {
            Connection = "db",
            Content = new List<SqlToken>(),
            Parameters = new List<ParamDecl>(),
        };

        var shape = new Shape(data: new Dictionary<string, object?>());
        var helper = new DataProviderHelper();

        var (rows, lastInsertedId) = provider.Execute(twig, shape, helper);

        fake.OpenCallCount.Should().Be(1);
        fake.CreateCommandCallCount.Should().Be(1);
        rows.Should().HaveCount(1);
        rows[0]["value"].Should().Be(42);
        lastInsertedId.Should().BeNull();

        provider.End();
    }

    private sealed class LeakProvider : IDataProvider
    {
        public bool Begun { get; private set; }
        public bool Ended { get; private set; }
        public bool Errored { get; private set; }

        public void Begin() => Begun = true;
        public void End() => Ended = true;
        public void Error() => Errored = true;

        public (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
            Twig twig, Shape inputShape, DataProviderHelper helper) =>
            (new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?> { ["$mode"] = "error", ["message"] = "boom" },
            }, null);
    }

    private sealed class CountingProvider : IDataProvider
    {
        public int Calls { get; private set; }
        public void Begin() { }
        public void End() { }
        public void Error() { }

        public (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
            Twig twig, Shape inputShape, DataProviderHelper helper)
        {
            Calls++;
            return (new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?> { ["role_id"] = 1 },
            }, null);
        }
    }

    private sealed class FakeDbConnection : DbConnection
    {
        private ConnectionState _state = ConnectionState.Closed;

        public int OpenCallCount { get; private set; }
        public int CreateCommandCallCount { get; private set; }

        public override string ConnectionString { get; set; } = "";
        public override string Database => "fake";
        public override string DataSource => "fake";
        public override string ServerVersion => "1.0";
        public override ConnectionState State => _state;

        public override void Open()
        {
            OpenCallCount++;
            _state = ConnectionState.Open;
        }

        public override void Close() => _state = ConnectionState.Closed;

        protected override DbCommand CreateDbCommand()
        {
            CreateCommandCallCount++;
            return new FakeDbCommand(this);
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
            throw new NotSupportedException();

        public override void ChangeDatabase(string databaseName) =>
            throw new NotSupportedException();
    }

    private sealed class FakeDbCommand : DbCommand
    {
        private readonly FakeDbParameterCollection _parameters = new();
        private readonly DbConnection _connection;

        public FakeDbCommand(DbConnection connection)
        {
            _connection = connection;
        }

        public override string CommandText { get; set; } = "";
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; } = CommandType.Text;
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection DbConnection { get => _connection; set { } }
        protected override DbParameterCollection DbParameterCollection => _parameters;
        protected override DbTransaction? DbTransaction { get; set; }

        public override void Cancel() { }
        public override int ExecuteNonQuery() => 0;
        public override object ExecuteScalar() => 0;
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => new FakeDbParameter();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => new FakeDbDataReader();
    }

    private sealed class FakeDbDataReader : DbDataReader
    {
        private bool _read;

        public override int FieldCount => 1;
        public override bool HasRows => true;
        public override bool IsClosed => false;
        public override int RecordsAffected => 0;
        public override int Depth => 0;
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(0);

        public override bool Read()
        {
            if (_read)
                return false;
            _read = true;
            return true;
        }

        public override bool NextResult() => false;
        public override string GetName(int ordinal) => "value";
        public override string GetDataTypeName(int ordinal) => "Int32";
        public override Type GetFieldType(int ordinal) => typeof(int);
        public override object GetValue(int ordinal) => 42;
        public override int GetValues(object[] values)
        {
            values[0] = 42;
            return 1;
        }
        public override int GetOrdinal(string name) => 0;
        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
        public override int GetInt32(int ordinal) => 42;
        public override long GetInt64(int ordinal) => 42;
        public override float GetFloat(int ordinal) => 42;
        public override double GetDouble(int ordinal) => 42;
        public override string GetString(int ordinal) => "42";
        public override decimal GetDecimal(int ordinal) => 42;
        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
        public override bool IsDBNull(int ordinal) => false;
        public override IEnumerator<object> GetEnumerator() => new List<object>().GetEnumerator();
        public override DataTable GetSchemaTable() => new();
    }

    private sealed class FakeDbParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> _items = new();

        public override int Count => _items.Count;
        public override object SyncRoot => ((System.Collections.ICollection)_items).SyncRoot;
        public override int Add(object value)
        {
            _items.Add((DbParameter)value);
            return _items.Count - 1;
        }
        public override void AddRange(Array values)
        {
            foreach (var value in values)
                _items.Add((DbParameter)value!);
        }
        public override void Clear() => _items.Clear();
        public override bool Contains(object value) => _items.Contains((DbParameter)value);
        public override bool Contains(string value) => _items.Any(p => p.ParameterName == value);
        public override void CopyTo(Array array, int index) => ((System.Collections.ICollection)_items).CopyTo(array, index);
        public override IEnumerator GetEnumerator() => _items.GetEnumerator();
        public override int IndexOf(object value) => _items.IndexOf((DbParameter)value);
        public override int IndexOf(string parameterName) => _items.FindIndex(p => p.ParameterName == parameterName);
        public override void Insert(int index, object value) => _items.Insert(index, (DbParameter)value);
        public override void Remove(object value) => _items.Remove((DbParameter)value);
        public override void RemoveAt(int index) => _items.RemoveAt(index);
        public override void RemoveAt(string parameterName)
        {
            var idx = IndexOf(parameterName);
            if (idx >= 0)
                _items.RemoveAt(idx);
        }
        protected override DbParameter GetParameter(int index) => _items[index];
        protected override DbParameter GetParameter(string parameterName) => _items[IndexOf(parameterName)];
        protected override void SetParameter(int index, DbParameter value) => _items[index] = value;
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var idx = IndexOf(parameterName);
            if (idx >= 0)
                _items[idx] = value;
            else
                _items.Add(value);
        }
    }

    private sealed class FakeDbParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
        public override bool IsNullable { get; set; }
        public override string ParameterName { get; set; } = "";
        public override string SourceColumn { get; set; } = "";
        public override object? Value { get; set; }
        public override bool SourceColumnNullMapping { get; set; }
        public override int Size { get; set; }
        public override void ResetDbType() { }
    }
}
