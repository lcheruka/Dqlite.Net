using System;
using System.Collections.Generic;
using System.Text;

namespace Dqlite.Net
{
    public class DatabaseFile
    {
        public string Name { get; set; }
        public int Size { get; set; }
        public byte[] Data { get; set; }
    }
}
