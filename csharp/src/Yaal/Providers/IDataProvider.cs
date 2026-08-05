// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Yaal.Execution;
using Yaal.Sql;

namespace Yaal.Providers;

public interface IDataProviderContextManager
{
    IDataProvider GetContext();
}

public interface IDataProvider
{
    void Begin();
    (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
        Twig twig, Shape inputShape, DataProviderHelper helper);
    void End();
    void Error();
}
