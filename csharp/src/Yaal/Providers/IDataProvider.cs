// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal.Providers;

public interface IDataProviderContextManager
{
    IDataProvider GetContext();
}

/// <summary>
/// Application- or engine-supplied execution. Yaal compiles SQL, then calls
/// <see cref="Execute"/> with the statement and bind values.
/// </summary>
public interface IDataProvider
{
    /// <summary>Placeholder character used when compiling, <c>?</c> or <c>%s</c>.</summary>
    string Placeholder { get; }

    void Begin();

    (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
        string sql, IReadOnlyList<object?> parameters);

    void End();
    void Error();
}

/// <summary>Optional blob/engine conversion applied while binding parameters.</summary>
public interface IValueConvertingProvider
{
    object? ConvertValue(string paramType, object? value);
}
