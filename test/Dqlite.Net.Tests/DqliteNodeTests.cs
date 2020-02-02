using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Dqlite.Net
{
    public class DqliteNodeTests
    {
        [Fact]
        public async Task FailOverTest()
        {
            var dataDir = Directory.CreateDirectory(
                Path.Combine(Path.GetTempPath(), "dqlite_tests_" + Guid.NewGuid()));
            try
            {
                var builder = new DqliteConnectionStringBuilder();
                builder.Nodes = new string[] { "127.0.0.1:5001", "127.0.0.1:5002", "127.0.0.1:5003" };
                using(var node02 = DqliteNode.Create(2, "127.0.0.1:5002", Path.Combine(dataDir.FullName, "2")))
                using(var node03 = DqliteNode.Create(3, "127.0.0.1:5003", Path.Combine(dataDir.FullName, "3")))
                {
                    node02.Start();
                    node03.Start();
                    using(var node01 = DqliteNode.Create(1, "127.0.0.1:5001", Path.Combine(dataDir.FullName, "1")))
                    {
                        node01.Start();
                        using (var client = new DqliteClient(builder, true))
                        {
                            await client.ConnectAsync();
                            await client.AddNodeAsync(2, "127.0.0.1:5002", DqliteNodeRoles.Voter);
                            await client.AddNodeAsync(3, "127.0.0.1:5003", DqliteNodeRoles.Voter);
                            var leader = await client.GetLeaderAsync();
                            var nodes = await client.GetNodesAsync();
                            Assert.Equal(1UL, leader.Id);
                            Assert.Equal(3, nodes.Length);
                        }
                    }

                    using (var cts = new CancellationTokenSource())
                    using (var client = new DqliteClient(builder, true))
                    {
                        cts.CancelAfter(TimeSpan.FromMinutes(1));
                        await client.ConnectAsync(cts.Token);

                        var leader = await client.GetLeaderAsync();
                        var nodes = await client.GetNodesAsync();
                        Assert.NotEqual(1UL, leader.Id);
                        Assert.Equal(3, nodes.Count());
                    }
                }
            }
            finally
            {
                dataDir.Delete(true);
            }

        }
    }
}