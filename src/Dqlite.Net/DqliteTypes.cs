using System;
using System.Collections.Generic;
using System.Text;

namespace Dqlite.Net
{
    public enum DqliteTypes : byte
    {
        Integer = 1,
        Float = 2,
        Text = 3,
        Blob = 4,
        Null = 5,
        ISO8601 = 10,
        Boolean = 11,
    }
}
