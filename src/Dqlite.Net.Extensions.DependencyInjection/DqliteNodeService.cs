using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Dqlite.Net
{
    public class DqliteNodeService : IHostedService
    {
        private DqliteNode node;
        private readonly DQliteOptions options;

        public DqliteNodeService(DQliteOptions options)
        {
            this.options = options;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Directory.CreateDirectory(options.DataDir);
            this.node = DqliteNode.Create(options.Id, options.Address, options.DataDir, options.NodeOptions);
            this.node?.Start();

            using(var client = new DqliteClient(this.options.ConnectionOptions, true))
            {
                await client.ConnectAsync(cancellationToken);
                var nodes = await client.GetNodesAsync(cancellationToken);
                var node = nodes.FirstOrDefault(x => x.Id == this.options.Id);
                if(node == null)
                {
                    await client.AddNodeAsync(this.options.Id, this.options.Address, cancellationToken);
                    await client.PromoteNodeAsync(this.options.Id, cancellationToken);
                }
                else if(node.Address != this.options.Address)
                {
                    throw new InvalidOperationException("Node with same id already exist with different address");
                }
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            this.node?.Stop();
            return Task.CompletedTask;
        }
    }
}
