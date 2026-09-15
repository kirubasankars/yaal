// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal;

public sealed class YaalQueryException : YaalException
{
    public IReadOnlyList<IDictionary<string, object?>> Errors { get; }

    public YaalQueryException(IReadOnlyList<IDictionary<string, object?>> errors)
        : base("Query returned validation or execution errors.")
    {
        Errors = errors;
    }
}
