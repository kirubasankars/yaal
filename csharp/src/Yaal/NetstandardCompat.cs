// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Collections.Generic;

namespace Yaal;

internal static class NetstandardCompat
{
    public static void Deconstruct<TKey, TValue>(
        this KeyValuePair<TKey, TValue> pair, out TKey key, out TValue value)
    {
        key = pair.Key;
        value = pair.Value;
    }

    public static Dictionary<TKey, TValue> CopyDictionary<TKey, TValue>(
        IReadOnlyDictionary<TKey, TValue>? source, IEqualityComparer<TKey> comparer)
    {
        var copy = new Dictionary<TKey, TValue>(comparer);
        if (source == null)
            return copy;
        foreach (var pair in source)
            copy.Add(pair.Key, pair.Value);
        return copy;
    }
}
