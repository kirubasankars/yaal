// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal;

/// <summary>
/// Skip a POCO property during typed materialization. Ignored properties are not
/// required in the shaped query result and are never set from SQL output.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class YaalIgnoreAttribute : Attribute;
