using System;
using System.Collections.Generic;
using System.Text;

namespace Dqlite.Net
{
    public class DqliteException : Exception
    {
        public ulong ErrorCode { get; }
        public DqliteException(ulong errorCode,  string message, Exception ex = null) : base(message, ex)
        {
            this.ErrorCode = errorCode;
        }
    }
}
