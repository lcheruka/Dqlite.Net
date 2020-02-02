using System;
using System.Collections.Generic;
using Xunit;

namespace Dqlite.Net
{
    public class UtilsTests
    {
        [Theory]
        [InlineData("SELECT * FROM sample WHERE id = @id", "SELECT * FROM sample WHERE id = ?", 1)]
        [InlineData("SELECT * FROM sample WHERE id = @id AND type =@type", "SELECT * FROM sample WHERE id = ? AND type =?", 2)]
        [InlineData("SELECT *, @value FROM sample WHERE id IN (@id1, @id2)", "SELECT *, ? FROM sample WHERE id IN (?, ?)", 3)]
        [InlineData("", "", 0)]
        [InlineData(null, null, 0)]
        public void ParseSqlTest(string sqlText, string expected, int parameterCount)
        {
            var actual = Utils.ParseSql(sqlText, out var parameterNames);
            Assert.Equal(expected, actual);
            Assert.Equal(parameterCount, parameterNames.Length);
        }
    }
}