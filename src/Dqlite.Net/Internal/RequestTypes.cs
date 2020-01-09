using System;

namespace Dqlite.Net
{
    internal enum RequestTypes : byte
    {
        RequestLeader = 0,
        RequestClient = 1,
        RequestHeartbeat = 2,
        RequestOpen = 3,
        RequestPrepare = 4,
        RequestExec = 5,
        RequestQuery = 6,
        RequestFinalize = 7,
        RequestExecSQL = 8,
        RequestQuerySQL = 9,
        RequestInterrupt = 10,
        RequestJoin = 12,
        RequestPromote = 13,
        RequestRemove = 14,
        RequestDump = 15,
        RequestCluster = 16,
    }
}