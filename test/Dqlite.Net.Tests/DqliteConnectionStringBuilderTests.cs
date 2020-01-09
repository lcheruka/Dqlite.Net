using System;
using Xunit;

namespace Dqlite.Net
{
    public class DqliteConnectionStringBuilderTests
    {

        [Theory]
        [InlineData("", null, null, null, null, null)]
        [InlineData("Nodes=127.0.0.1:9181", new []{"127.0.0.1:9181"}, null, null, null, null)]
        [InlineData("Nodes=127.0.0.1:9181,127.0.0.1:9182", new []{"127.0.0.1:9181","127.0.0.1:9182"}, null, null, null, null)]
        [InlineData("Nodes=127.0.0.1:9181,127.0.0.1:9182;Data Source=main", new []{"127.0.0.1:9181","127.0.0.1:9182"}, "main", null, null, null)]
        [InlineData("Nodes=127.0.0.1:9181,127.0.0.1:9182;Data Source=main;TcpKeepAlive=True", new []{"127.0.0.1:9181","127.0.0.1:9182"}, "main", true, null, null)]
        [InlineData("Nodes=127.0.0.1:9181,127.0.0.1:9182;Data Source=main;TcpKeepAlive=True;SocketReceiveBufferSize=10", new []{"127.0.0.1:9181","127.0.0.1:9182"}, "main", true, 10, null)]
        [InlineData("Nodes=127.0.0.1:9181,127.0.0.1:9182;Data Source=main;TcpKeepAlive=True;SocketReceiveBufferSize=10;SocketSendBufferSize=20", new []{"127.0.0.1:9181","127.0.0.1:9182"}, "main", true, 10, 20)]
        public void Test(string connectionString, string[] nodes, string dataSource, bool? tcpKeepAlive, int? socketReceiveBufferSize, int? socketSendBufferSize)
        {
            var builder = new DqliteConnectionStringBuilder();
            if(nodes != null)
            {
                builder.Nodes = nodes;
            }
            if(dataSource != null)
            {
                builder.DataSource = dataSource;
            }
            if(tcpKeepAlive != null)
            {
                builder.TcpKeepAlive = tcpKeepAlive.Value;
            }
            if(socketReceiveBufferSize != null)
            {
                builder.SocketReceiveBufferSize = socketReceiveBufferSize.Value;
            }
            if(socketSendBufferSize != null)
            {
                builder.SocketSendBufferSize = socketSendBufferSize.Value;
            }

            Assert.Equal(connectionString, builder.ToString());
        }
    }
}