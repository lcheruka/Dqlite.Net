using System;
using System.Collections.Generic;
using System.Text;

namespace Dqlite.Net
{
    internal class PreparedStatement
    {
        public uint Id { get; set; }
        public uint DatabaseId { get; set; }
        public ulong ParameterCount { get; set; }
    }
}
