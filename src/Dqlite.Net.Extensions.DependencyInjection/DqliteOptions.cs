using System;

namespace Dqlite.Net
{
    public class DqliteOptions
    {
        public ulong Id {get;set;}
        public string Address {get;set;}
        public string DataDir {get;set;}
        public DqliteNodeOptions NodeOptions {get; internal set; }

        public DqliteOptions()
        {
            this.NodeOptions = new DqliteNodeOptions();
        }
    }
}