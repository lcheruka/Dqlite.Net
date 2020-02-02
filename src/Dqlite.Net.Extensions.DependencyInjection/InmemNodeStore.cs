using System;

namespace Dqlite.Net
{
    public class InmemNodeStore : IDqliteNodeStore
    {
        private string[] nodes;

        public InmemNodeStore(string[] nodes = null)
        {
            this.nodes = nodes ?? new string[0];
        }

        public string[] Get()
            => this.nodes;

        public void Put(string[] nodes)
            => this.nodes = nodes ?? new string[0];
    }
}