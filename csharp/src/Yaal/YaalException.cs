// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal;

public class YaalException : Exception
{
    public YaalException(string message) : base(message) { }
    public YaalException(string message, Exception inner) : base(message, inner) { }
}

public class DescriptorNotFoundException : YaalException
{
    public DescriptorNotFoundException(string message) : base(message) { }
}

public class UnsupportedDatabaseUrlException : YaalException
{
    public UnsupportedDatabaseUrlException(string message) : base(message) { }
}

public class PathEscapeException : YaalException
{
    public PathEscapeException(string message) : base(message) { }
}
