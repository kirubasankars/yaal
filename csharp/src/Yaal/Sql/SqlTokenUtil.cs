// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal.Sql;

internal static class SqlTokenUtil
{
    internal static bool IsWordLike(SqlToken token) =>
        token.Type is "word" or "sql";

    internal static bool TokenEquals(SqlToken token, string word) =>
        IsWordLike(token) &&
        token.Value.Trim().Equals(word, StringComparison.OrdinalIgnoreCase);

    internal static bool HasSignificantSqlContent(IEnumerable<SqlToken> content) =>
        content.Any(t => IsWordLike(t) && !string.IsNullOrWhiteSpace(t.Value));
}
