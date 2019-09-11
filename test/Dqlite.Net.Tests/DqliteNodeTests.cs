using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dqlite.Net
{
    public class DqliteNodeTests
    {
        [Fact]
        public async Task ClusterTest()
        {
            var dataDir = Directory.CreateDirectory(
                Path.Combine(Path.GetTempPath(), "dqlite_tests_"+Guid.NewGuid()));
            try
            {
                var node01 = DqliteNode.Create(1, "127.0.0.1:5001", dataDir.CreateSubdirectory("1").FullName);
                using(var node02 = DqliteNode.Create(2, "127.0.0.1:5002", dataDir.CreateSubdirectory("2").FullName))
                using(var node03 = DqliteNode.Create(3, "127.0.0.1:5003", dataDir.CreateSubdirectory("3").FullName))
                {
                    node01.Start();
                    node02.Start();
                    node03.Start();

                    using(var client = new DqliteClient())
                    {
                        client.Open("127.0.0.1:5001");
                        client.AddNode(2, "127.0.0.1:5002");
                        client.AddNode(3, "127.0.0.1:5003");
                        client.PromoteNode(2);
                        client.PromoteNode(3);
                        
                        var nodes = client.EnumerateNodes();
                        Assert.Equal(3, nodes.Count());

                        
                    }
                    using(var client = new DqliteClient())
                    {
                        client.Open("127.0.0.1:5002");
                        var leader = client.GetLeader();
                        var nodes = client.EnumerateNodes();
                        Assert.Equal(3, nodes.Count());
                    }
                    using(var client = new DqliteClient())
                    {
                        client.Open("127.0.0.1:5003");
                        
                        var nodes = client.EnumerateNodes();
                        Assert.Equal(3, nodes.Count());
                    }

                    node01.Stop();
                    node01.Dispose();

                    await Task.Delay(2000);
                    
                    using(var client = await DqliteClient.CreateAsync(true, "127.0.0.1:5001","127.0.0.1:5002","127.0.0.1:5003" ))
                    {                        
                        var nodes = client.EnumerateNodes();
                        Assert.Equal(3, nodes.Count());
                    }

                    using(var client = await DqliteClient.CreateAsync(false, "127.0.0.1:5001","127.0.0.1:5002","127.0.0.1:5003" ))
                    {                        
                        var nodes = client.EnumerateNodes();
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