using System;

namespace Dqlite.Net
{
    public class DQliteOptions
    {
        public ulong Id {get;set;}
        public string Address {get;set;}
        public string DataDir {get;set;}
        public DqliteConnectionStringBuilder ConnectionOptions { get; internal set; }
        public DqliteNodeOptions NodeOptions {get; internal set; }

        public DQliteOptions(){
            this.ConnectionOptions = new DqliteConnectionStringBuilder();
            this.NodeOptions = new DqliteNodeOptions();
        }
    }
}