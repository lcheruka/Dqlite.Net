using System;
using System.Collections.Generic;
using System.Text;

namespace Dqlite.Net
{
    internal class StatementResult
    {
        public ulong LastRowId { get; set; }
        public ulong RowCount { get; set; }
    }
}
