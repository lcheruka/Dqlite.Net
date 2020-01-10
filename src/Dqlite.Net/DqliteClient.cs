using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using static Dqlite.Net.ResponseParsers;
using static Dqlite.Net.Utils;

namespace Dqlite.Net
{
    public sealed partial class DqliteClient : IDisposable, IAsyncDisposable
    {
        private readonly DqliteConnector connector;

        public DqliteClient(string address, bool ConnectLeader = false)
            : this(new DqliteConnectionStringBuilder(){Nodes = new [] {address}}, ConnectLeader)
        {

        }

        public DqliteClient(DqliteConnectionStringBuilder settings, bool ConnectLeader = false)
        {
            this.connector = new DqliteConnector(settings, ConnectLeader);
        }

        public void Connect()
            => this.connector.Connect();

        public Task ConnectAsync(CancellationToken cancellationToken = default(CancellationToken))
            => this.connector.ConnectAsync(cancellationToken);

        public DqliteNodeInfo GetLeader() 
            => this.connector.GetLeader();

        public Task<DqliteNodeInfo> GetLeaderAsync(CancellationToken cancellationToken = default(CancellationToken)) 
            => this.connector.GetLeaderAsync();

        public  void AddNode(ulong nodeId, string address)
        {
            var length = PadWord( address.Length + 1) + 8;
            var data = (Span<byte>) stackalloc byte[length];
            Requests.Write(data, nodeId, address);
            this.connector.SendRequest(RequestTypes.RequestJoin, data);
            this.connector.ReadResponse<bool>(ParseAknowledgmentResponse);
        }

        public async Task AddNodeAsync(ulong nodeId, string address, CancellationToken cancellationToken = default(CancellationToken))
        {
            var length = PadWord( address.Length + 1) + 8;
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                Requests.Write(data.Span, nodeId, address);
                await this.connector.SendRequestAsync(RequestTypes.RequestJoin, data, cancellationToken);
                await this.connector.ReadResponseAsync<bool>(ParseAknowledgmentResponse, cancellationToken);
            }            
        }

        public void PromoteNode(ulong nodeId)
        {
            const int length = 8;
            var data = (Span<byte>) stackalloc byte[length];
            Requests.Write(data, nodeId);
            this.connector.SendRequest(RequestTypes.RequestPromote, data);
            this.connector.ReadResponse<bool>(ParseAknowledgmentResponse);
        }
        
        public async Task PromoteNodeAsync(ulong nodeId, CancellationToken cancellationToken = default(CancellationToken))
        {
            const int length = 8;
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                Requests.Write(data.Span, nodeId);
                await this.connector.SendRequestAsync(RequestTypes.RequestPromote, data, cancellationToken);
                await this.connector.ReadResponseAsync<bool>(ParseAknowledgmentResponse, cancellationToken);
            }  
        }

        public void RemoveNode(ulong nodeId)
        {
            const int length = 8;
            var data = (Span<byte>) stackalloc byte[length];
            Requests.Write(data, nodeId);
            this.connector.SendRequest(RequestTypes.RequestRemove, data);
            this.connector.ReadResponse<bool>(ParseAknowledgmentResponse);
        }

        public async Task RemoveNodeAsync(ulong nodeId, CancellationToken cancellationToken = default(CancellationToken))
        {
            const int length = 8;
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                Requests.Write(data.Span, nodeId);
                await this.connector.SendRequestAsync(RequestTypes.RequestRemove, data, cancellationToken);
                await this.connector.ReadResponseAsync<bool>(ParseAknowledgmentResponse, cancellationToken);
            }  
        }

        public DqliteNodeInfo[] GetNodes(CancellationToken cancellationToken = default(CancellationToken))
        {
            const int length = 8;
            var data = (Span<byte>) stackalloc byte[length];
            this.connector.SendRequest(RequestTypes.RequestCluster, data);
            return this.connector.ReadResponse<DqliteNodeInfo[]>(ParseNodesResponse);
        }
        
        public async Task<DqliteNodeInfo[]> GetNodesAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            const int length = 8;
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                await this.connector.SendRequestAsync(RequestTypes.RequestCluster, data, cancellationToken);
                return await this.connector.ReadResponseAsync<DqliteNodeInfo[]>(ParseNodesResponse, cancellationToken);
            }
        }
           
        public void Dispose()
        {
            ((IDisposable)connector)?.Dispose();
        }

        public ValueTask DisposeAsync()
        {
            return ((IAsyncDisposable)connector)?.DisposeAsync() ?? new ValueTask(); 
        }
    }
}