using System;
using System.Collections.Generic;
using System.Text;

namespace Dqlite.Net
{
    public class DatabaseDump
    {
        public DatabaseFile Main { get; set; }
        public DatabaseFile Log { get; set; }

        public DatabaseDump()
        {
            this.Main = new DatabaseFile();
            this.Log = new DatabaseFile();
        }
    }
}
