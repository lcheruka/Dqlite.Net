using System;

namespace Dqlite.Net
{
    public class DqliteNodeInfo
    {
        public ulong Id { get; set; }
        public string Address { get; set;}
        public DqliteNodeRoles Role {get;set;}
    }
}