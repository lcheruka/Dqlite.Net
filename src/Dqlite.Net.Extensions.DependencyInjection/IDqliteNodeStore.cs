using System;

namespace Dqlite.Net
{
    public interface IDqliteNodeStore
    {
        string[] Get();
        void Put(string[] nodes);
    }
}