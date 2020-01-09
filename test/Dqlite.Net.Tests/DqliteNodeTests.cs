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
        private class NodeProcess : IDisposable
        {
            private static string PATH = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "Dqlite.Net.DemoApp.dll");

            public bool HasExited => this.process.HasExited;

            private readonly Process process;

            public NodeProcess(ulong id, string address, string dataDir)
            {
                this.process = Process.Start("dotnet", $"{PATH} {id} \"{address}\" \"{(Path.Combine(dataDir, "1"))}\"");
            }

            public void Kill()
            {
                if (!this.process.HasExited)
                {
                    this.process.Kill();
                }
            }

            public void Dispose()
            {
                try
                {
                    this.Kill();
                    this.process.Dispose();
                }
                catch
                {

                }
            }
        }


        [Fact]
        public async Task ClusterTest()
        {
            var dataDir = Directory.CreateDirectory(
                Path.Combine(Path.GetTempPath(), "dqlite_tests_" + Guid.NewGuid()));
            try
            {
                DqliteConnectionStringBuilder builder = new DqliteConnectionStringBuilder();
                builder.Nodes = new string[] { "127.0.0.1:5001", "127.0.0.1:5002", "127.0.0.1:5003" };
                using (var node01 = new NodeProcess(1, "127.0.0.1:5001", Path.Combine(dataDir.FullName, "1")))
                using (var node02 = new NodeProcess(2, "127.0.0.1:5002", Path.Combine(dataDir.FullName, "2")))
                using (var node03 = new NodeProcess(3, "127.0.0.1:5003", Path.Combine(dataDir.FullName, "3")))
                {

                    Assert.False(node01.HasExited);
                    Assert.False(node02.HasExited);
                    Assert.False(node03.HasExited);

                    await Task.Delay(10000);

                    using (var client = new DqliteClient(builder))
                    {
                        await client.ConnectAsync();

                        await client.AddNodeAsync(2, "127.0.0.1:5002");
                        Assert.Equal(1ul, (await client.GetLeaderAsync()).Id);

                        await client.PromoteNodeAsync(2);
                        await client.AddNodeAsync(3, "127.0.0.1:5003");
                        await client.PromoteNodeAsync(3);

                        var nodes = await client.EnumerateNodesAsync();
                        Assert.Equal(3, nodes.Count());
                    }

                    using (var client = new DqliteClient(builder))
                    {
                        await client.ConnectAsync();

                        var leader = await client.GetLeaderAsync();
                        var nodes = await client.EnumerateNodesAsync();
                        Assert.Equal(3, nodes.Count());
                    }

                    using (var client = new DqliteClient(builder))
                    {
                        await client.ConnectAsync();

                        var nodes = await client.EnumerateNodesAsync();
                        Assert.Equal(3, nodes.Count());
                    }

                    node01.Kill();

                    using (var cts = new CancellationTokenSource())
                    {
                        cts.CancelAfter(30 * 1000);
                        using (var client = new DqliteClient(builder))
                        {
                            var nodes = await client.EnumerateNodesAsync();
                            Assert.Equal(3, nodes.Count());
                        }
                    }

                    using (var cts = new CancellationTokenSource())
                    {
                        cts.CancelAfter(30 * 1000);
                        using (var client = new DqliteClient(builder))
                        {
                            var nodes = await client.EnumerateNodesAsync();
                            Assert.Equal(3, nodes.Count());
                        }
                    }

                    using (var cts = new CancellationTokenSource())
                    {
                        cts.CancelAfter(30 * 1000);
                        using (var newNode01 = new NodeProcess(1, "127.0.0.1:5004", Path.Combine(dataDir.FullName, "4")))
                        using (var client = new DqliteClient(builder))
                        {
                            
                            Console.WriteLine("Started Removing node");
                            await client.RemoveNodeAsync(1);
                            Console.WriteLine("Finished Removing node");
                            await client.AddNodeAsync(1, "127.0.0.1:5004");
                            await client.PromoteNodeAsync(1);

                            var nodes = await client.EnumerateNodesAsync();
                            Assert.Equal(3, nodes.Count());
                        }
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