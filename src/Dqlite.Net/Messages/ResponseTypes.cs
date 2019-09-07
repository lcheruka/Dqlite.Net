using System;

namespace Dqlite.Net.Messages
{
    public enum ResponseTypes : byte
    {
        ResponseFailure = 0,
        ResponseNode = 1,
        ResponseNodeLegacy = 1,
        ResponseWelcome = 2,
        ResponseNodes = 3,
        ResponseDb = 4,
        ResponseStmt = 5,
        ResponseResult = 6,
        ResponseRows = 7,
        ResponseEmpty = 8,
        ResponseFiles = 9,
    }
}