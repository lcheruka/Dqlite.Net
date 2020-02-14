using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using static Dqlite.Net.ResponseParsers;
using static Dqlite.Net.Utils;

namespace Dqlite.Net
{
    internal sealed partial class DqliteConnector : IDisposable, IAsyncDisposable
    {
        public async Task ConnectAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            while(true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                foreach(var node in this.Settings.Nodes)
                {
                    try
                    {
                        await ConnectAsync(node, cancellationToken);
                        return;
                    }
                    catch(OperationCanceledException){}
                    catch
                    {
                        await Task.Delay(500);
                    }
                }
            }
        }

        private async Task ConnectAsync(string address, CancellationToken cancellationToken = default(CancellationToken))
        {
            const int length = 8;
            void WriteBuffer(Memory<byte> memory)
            {
                var span = memory.Span;
                span.Write(VERSION);
            }
            while(true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if(!Utils.TryParseEndPoint(address, out var endpoint))
                {
                    throw new FormatException();
                }
                
                var protocolType = endpoint.AddressFamily == AddressFamily.InterNetwork ? ProtocolType.Tcp : ProtocolType.IP;
                var socket = new Socket(endpoint.AddressFamily, SocketType.Stream, protocolType);
                
                try
                {
                    await socket.ConnectAsync(endpoint)
                        .WithCancellation(cancellationToken: cancellationToken);
                    SetSocketOptions(socket);
                    
                    this.stream = new NetworkStream(socket, true);
                    using(var slot = MemoryPool<byte>.Shared.Rent(length))
                    {
                        var data = slot.Memory.Slice(0, length);
                        WriteBuffer(data);
                        await this.stream.WriteAsync(data);
                    }
                    
                    var leader = GetLeader();
                    if(this.connectLeader && leader.Address != address)
                    {
                        this.stream?.Dispose();

                        this.stream = null;
                        socket = null;
                        address = leader.Address;
                        continue;
                    } 
                    break;                
                }
                catch
                {
                    this.stream?.Dispose();
                    socket?.Dispose();
                    this.stream = null;
                    socket = null;
                    throw;
                }
            }
        }

        public async Task<DqliteNodeInfo> GetLeaderAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            const int length = 8;
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                await this.SendRequestAsync(RequestTypes.RequestLeader, data, cancellationToken);
                return await this.ReadResponseAsync<DqliteNodeInfo>(ParseNodeResponse, cancellationToken);
            }           
        }
        
        public async Task<DatabaseRecord> OpenDatabaseAsync(string name, CancellationToken cancellationToken = default(CancellationToken))
        {
            var length = PadWord(name.Length + 1) + 16;
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                Requests.Write(data.Span, name);
                await this.SendRequestAsync(RequestTypes.RequestOpen, data, cancellationToken);
                return await this.ReadResponseAsync<DatabaseRecord>(ParseDatabaseResponse, cancellationToken);
            }
        }

        public async Task<PreparedStatement> PrepareStatementAsync(DatabaseRecord database, string text, CancellationToken cancellationToken = default(CancellationToken))
        {
            var length = PadWord(text.Length + 1) + 8;
            async Task<PreparedStatement> PrepareStatementAsync(ReadOnlyMemory<byte> data)
            {
                await this.SendRequestAsync(RequestTypes.RequestPrepare, data, cancellationToken);
                return await this.ReadResponseAsync<PreparedStatement>(ParsePreparedStatementResponse, cancellationToken);
            }
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                Requests.Write(data.Span, database, text);
                return await RetryAsync<PreparedStatement>(PrepareStatementAsync, data);
            }
        }

        public async Task<StatementResult> ExecuteNonQueryAsync(PreparedStatement preparedStatement, DqliteParameter[] parameters, CancellationToken cancellationToken = default(CancellationToken))
        {
            var length = 8 + GetSize(parameters);  
            async Task<StatementResult> ExecuteNonQueryAsync(ReadOnlyMemory<byte> data)
            {
                await this.SendRequestAsync(RequestTypes.RequestExec, data, cancellationToken);
                return await this.ReadResponseAsync<StatementResult>(ParseStatementResultResponse, cancellationToken);
            }          
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                Requests.Write(data.Span, preparedStatement, parameters);
                return await RetryAsync<StatementResult>(ExecuteNonQueryAsync, data);
            }
        }

        public async Task<DqliteDataRecord> ExecuteQueryAsync(PreparedStatement preparedStatement, DqliteParameter[] parameters, CancellationToken cancellationToken = default(CancellationToken))
        {
            var length = 8 + GetSize(parameters);
            async Task<DqliteDataRecord> ExecuteQueryAsync(ReadOnlyMemory<byte> data)
            {
                await this.SendRequestAsync(RequestTypes.RequestQuery, data, cancellationToken);
                return await this.ReadResponseAsync<DqliteDataRecord>(ParseDataRecordResponse, cancellationToken);
            }   
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                Requests.Write(data.Span, preparedStatement, parameters);
                return await RetryAsync<DqliteDataRecord>(ExecuteQueryAsync, data);
            }
        }

        public async Task FinalizeStatementAsync(PreparedStatement preparedStatement, CancellationToken cancellationToken = default(CancellationToken))
        {
            const int length = 8;
            async Task<bool> FinalizeStatementAsync(ReadOnlyMemory<byte> data)
            {
                await this.SendRequestAsync(RequestTypes.RequestFinalize, data, cancellationToken);
                await this.ReadResponseAsync<bool>(ParseAknowledgmentResponse, cancellationToken);
                return true;
            }  
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                Requests.Write(data.Span, preparedStatement);
                await RetryAsync<bool>(FinalizeStatementAsync, data);
            }
        }

        public async Task<StatementResult> ExecuteNonQueryAsync(DatabaseRecord database, string text, DqliteParameter[] parameters, CancellationToken cancellationToken = default(CancellationToken))
        {
            var length = 8 + PadWord(text.Length+1) + GetSize(parameters);
            async Task<StatementResult> ExecuteNonQueryAsync(ReadOnlyMemory<byte> data)
            {
                await this.SendRequestAsync(RequestTypes.RequestExecSQL, data, cancellationToken);
                return await this.ReadResponseAsync<StatementResult>(ParseStatementResultResponse, cancellationToken);
            }  
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                Requests.Write(data.Span, database, text, parameters);
                return await RetryAsync<StatementResult>(ExecuteNonQueryAsync, data);
            }
        }

        public async Task<DqliteDataRecord> ExecuteQueryAsync(DatabaseRecord database, string text, DqliteParameter[] parameters, CancellationToken cancellationToken = default(CancellationToken))
        {
            var length = 8 + PadWord(text.Length+1) + GetSize(parameters);
            async Task<DqliteDataRecord> ExecuteQueryAsync(ReadOnlyMemory<byte> data)
            {
                await this.SendRequestAsync(RequestTypes.RequestQuerySQL, data, cancellationToken);
                return await this.ReadResponseAsync<DqliteDataRecord>(ParseDataRecordResponse, cancellationToken);
            } 
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                Requests.Write(data.Span, database, text, parameters);
                return await RetryAsync<DqliteDataRecord>(ExecuteQueryAsync, data);
            }
        }

        public async Task InterruptStatementAsync(DatabaseRecord database, CancellationToken cancellationToken = default(CancellationToken))
        {
            const int length = 8;
            async Task<bool> InterruptStatementAsync(ReadOnlyMemory<byte> data)
            {
                await this.SendRequestAsync(RequestTypes.RequestInterrupt, data, cancellationToken);
                while(!await this.ReadResponseAsync<bool>(ParseAknowledgmentResponse, cancellationToken))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }
                return true;
            } 
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Slice(0, length);
                Requests.Write(data.Span, database);
                await RetryAsync<bool>(InterruptStatementAsync, data);
            } 
        }       

        internal async Task SendRequestAsync(RequestTypes type, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default(CancellationToken))
        {
            const int length = 8;
            void WriteBuffer(Memory<byte> memory)
            {
                var span = memory.Span;
                span.Write(data.Length / 8)
                    .Write((byte)type)
                    .Write(REVISION);
            }

            if(data.Length % length != 0)
            {
                throw new InvalidOperationException();
            }

            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var headerMemory = slot.Memory.Slice(0, length);
                WriteBuffer(headerMemory);
                await this.stream.WriteAsync(headerMemory, cancellationToken);
                await this.stream.WriteAsync(data, cancellationToken);
            }
        }

        internal async Task<T> ReadResponseAsync<T>(ResponseParser<T> parser, CancellationToken cancellationToken = default(CancellationToken))
        {
            const int length = 8;
            var header = default(MessageHeader);

            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var memory = slot.Memory;
                var amount = await this.stream.ReadAsync(memory.Slice(0, length), cancellationToken);
                if (amount != length)
                {
                    throw new EndOfStreamException();
                }
                header = MessageHeader.Parse(memory.Span);
            } 

            using(var slot = MemoryPool<byte>.Shared.Rent(header.Size))
            {
                var memory = slot.Memory;
                var amount = await this.stream.ReadAsync(memory.Slice(0, header.Size), cancellationToken);
                if (amount != header.Size)
                {
                    throw new EndOfStreamException();
                }

                return parser((ResponseTypes)header.Type, header.Size, memory);
            }  
        }
    }
}