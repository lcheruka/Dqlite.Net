using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Dqlite.Net
{
    public class DqliteNodeService : BackgroundService
    {
        private DqliteNode node;
        private readonly IEnumerable<IDqliteService> services;
        private readonly IDqliteNodeStore store;
        private readonly DqliteOptions options;

        public DqliteNodeService(IEnumerable<IDqliteService> services, IDqliteNodeStore store, DqliteOptions options)
        {
            this.services = services;
            this.store = store;
            this.options = options;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            Directory.CreateDirectory(options.DataDir);
            this.node = DqliteNode.Create(options.Id, options.Address, options.DataDir, options.NodeOptions);
            this.node.Start();

            var connectionOptions = new DqliteConnectionStringBuilder()
            {
                Nodes = this.store.Get()
            };
            using(var client = new DqliteClient(connectionOptions, true))
            {
                await client.ConnectAsync(cancellationToken);
                var nodes = await client.GetNodesAsync(cancellationToken);
                var node = nodes.FirstOrDefault(x => x.Id == this.options.Id);
                if(node == null)
                {
                    await client.AddNodeAsync(this.options.Id, this.options.Address, DqliteNodeRoles.Voter, cancellationToken);
                }
                else if(node.Address != this.options.Address)
                {
                    throw new InvalidOperationException("Node with same id already exist with different address");
                }
            }
            await this.ExecuteAsync(x => x.StartAsync(cancellationToken));
            await base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var oldLeader = default(DqliteNodeInfo);
            while(!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    using(var client = new DqliteClient(this.options.Address, false))
                    {
                        while(!cancellationToken.IsCancellationRequested)
                        {
                            var leader = await client.GetLeaderAsync(cancellationToken);
                            if(leader.Id != oldLeader?.Id)
                            {
                                await this.ExecuteAsync(x => x.OnRoleChangeAsync(leader.Id == this.options.Id, cancellationToken), false);
                                oldLeader = leader;
                            }

                            await Task.Delay(1000, cancellationToken);
                        }
                    }
                }
                catch(OperationCanceledException){return;}
                catch
                {
                    await Task.Delay(1000);
                }
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            this.node?.Stop();
            await base.StopAsync(cancellationToken);
            await this.ExecuteAsync(x => x.StopAsync(cancellationToken), false);
        }
        
        private async Task ExecuteAsync(Func<IDqliteService, Task> callback, bool throwOnFirstFailure = true)
        {
            List<Exception> exceptions = null;

            foreach (var service in this.services)
            {
                try
                {
                    await callback(service);
                }
                catch (Exception ex)
                {
                    if (throwOnFirstFailure)
                    {
                        throw;
                    }

                    if (exceptions == null)
                    {
                        exceptions = new List<Exception>();
                    }

                    exceptions.Add(ex);
                }
            }

            // Throw an aggregate exception if there were any exceptions
            if (exceptions != null)
            {
                throw new AggregateException(exceptions);
            }
        }
    }
}
