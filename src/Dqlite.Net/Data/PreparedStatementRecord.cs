using System;
using System.Collections.Generic;
using System.Text;

namespace Dqlite.Net
{
    public class PreparedStatementRecord
    {
        public uint Id { get; set; }
        public uint DatabaseId { get; set; }
        public ulong ParameterCount { get; set; }
    }
}
