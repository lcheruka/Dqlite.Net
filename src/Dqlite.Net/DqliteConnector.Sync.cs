using System;
using System.Buffers;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using static Dqlite.Net.ResponseParsers;
using static Dqlite.Net.Utils;

namespace Dqlite.Net
{
    internal sealed partial class DqliteConnector : IDisposable, IAsyncDisposable
    {
        public void Connect(CancellationToken cancellationToken = default(CancellationToken))
        {
            while(true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                foreach(var node in this.Settings.Nodes)
                {
                    try
                    {
                        Connect(node, cancellationToken);
                        return;
                    }
                    catch(OperationCanceledException){}
                    catch
                    {

                    }
                }
            }
        }

        private void Connect(string address, CancellationToken cancellationToken)
        {
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
                    var span = (Span<byte>)stackalloc byte[8];
                    span.Write(VERSION);
                    

                    socket.Connect(endpoint);
                    SetSocketOptions(socket);

                    this.stream = new NetworkStream(socket, true);
                    this.stream.Write(span);
                    this.stream.Flush();
                                        
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

        public DqliteNodeInfo GetLeader()
        {
            const int length = 8;
            var data = (Span<byte>)stackalloc byte[length];
            Requests.Write(data, 0UL);
            SendRequest(RequestTypes.RequestLeader, data);
            return ReadResponse<DqliteNodeInfo>(ParseNodeResponse);
        }

        public DatabaseRecord OpenDatabase(string name)
        {
            var length = PadWord(name.Length + 1) + 16;
            var data = (Span<byte>)stackalloc byte[length];
            Requests.Write(data, name);
            SendRequest(RequestTypes.RequestOpen, data);
            return ReadResponse<DatabaseRecord>(ParseDatabaseResponse);
        }

        public PreparedStatement PrepareStatement(DatabaseRecord database, string text)
        {
            var length = PadWord(text.Length + 1) + 8;
            PreparedStatement PrepareStatement(ReadOnlySpan<byte> data)
            {
                SendRequest(RequestTypes.RequestPrepare, data);
                return ReadResponse<PreparedStatement>(ParsePreparedStatementResponse);
            }
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Span.Slice(0, length);
                Requests.Write(data, database, text);
                return Retry<PreparedStatement>(PrepareStatement, data);
            }
        }

        public StatementResult ExecuteNonQuery(PreparedStatement preparedStatement, DqliteParameter[] parameters)
        {
            var length = 8 + GetSize(parameters);            
            StatementResult ExecuteNonQuery(ReadOnlySpan<byte> data)
            {
                this.SendRequest(RequestTypes.RequestExec, data);
                return this.ReadResponse<StatementResult>(ParseStatementResultResponse);
            }
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Span.Slice(0, length);
                Requests.Write(data, preparedStatement, parameters);
                return Retry<StatementResult>(ExecuteNonQuery, data);
            }
        }

        public DqliteDataRecord ExecuteQuery(PreparedStatement preparedStatement, DqliteParameter[] parameters)
        {
            var length = 8 + GetSize(parameters);
            DqliteDataRecord ExecuteQuery(ReadOnlySpan<byte> data)
            {
                this.SendRequest(RequestTypes.RequestQuery, data);
                return this.ReadResponse<DqliteDataRecord>(ParseDataRecordResponse);
            }
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Span.Slice(0, length);
                Requests.Write(data, preparedStatement, parameters);
                return Retry(ExecuteQuery, data);
            }
        }

        public void FinalizeStatement(PreparedStatement preparedStatement)
        {
            const int length = 8;
            bool FinalizeStatement(ReadOnlySpan<byte> data)
            {
                this.SendRequest(RequestTypes.RequestFinalize, data);
                return this.ReadResponse<bool>(ParseAknowledgmentResponse);
            }
            var data = (Span<byte>) stackalloc byte[length];
            Requests.Write(data, preparedStatement);
            Retry<bool>(FinalizeStatement, data);
        }

        public StatementResult ExecuteNonQuery(DatabaseRecord database, string text, DqliteParameter[] parameters)
        {
            var length = 8 + PadWord(text.Length+1) + GetSize(parameters);
            StatementResult ExecuteNonQuery(ReadOnlySpan<byte> data)
            {
                this.SendRequest(RequestTypes.RequestExecSQL, data);
                return this.ReadResponse<StatementResult>(ParseStatementResultResponse);
            }
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Span.Slice(0, length);
                Requests.Write(data, database, text, parameters);                
                return Retry<StatementResult>(ExecuteNonQuery, data);
            }
        }

        public DqliteDataRecord ExecuteQuery(DatabaseRecord database, string text, DqliteParameter[] parameters)
        {
            var length = 8 + PadWord(text.Length+1) + GetSize(parameters);
            DqliteDataRecord ExecuteQuery(ReadOnlySpan<byte> data)
            {
                this.SendRequest(RequestTypes.RequestQuerySQL, data);
                return this.ReadResponse<DqliteDataRecord>(ParseDataRecordResponse);
            }
            using(var slot = MemoryPool<byte>.Shared.Rent(length))
            {
                var data = slot.Memory.Span.Slice(0, length);
                Requests.Write(data, database, text, parameters);
                return Retry<DqliteDataRecord>(ExecuteQuery, data);
            }
        }

        public void InterruptStatement(DatabaseRecord database)
        {
            const int length = 8;
            bool InterruptStatement(ReadOnlySpan<byte> data)
            {
                this.SendRequest(RequestTypes.RequestInterrupt, data);
                while(!this.ReadResponse<bool>(ParseAknowledgmentResponse))
                {

                }
                return true;
            }
            var data = (Span<byte>) stackalloc byte[length];
            Requests.Write(data, database);
            Retry<bool>(InterruptStatement, data);
        }

        internal void SendRequest(RequestTypes type, ReadOnlySpan<byte> span)
        {
            if(span.Length % 8 != 0)
            {
                throw new InvalidOperationException();
            }

            var header = (Span<byte>)stackalloc byte[8];
            header.Write(span.Length / 8)
                .Write((byte)type)
                .Write(REVISION);
            stream.Write(header);
            stream.Write(span);
        }

        internal T ReadResponse<T>(ResponseParser<T> parser)
        {
            var headerSpan = (Span<byte>) stackalloc byte[8];

            var amount = stream.Read(headerSpan);
            if (amount != headerSpan.Length)
            {
                throw new EndOfStreamException();
            }
            var header = MessageHeader.Parse(headerSpan);

            using(var slot = MemoryPool<byte>.Shared.Rent(header.Size))
            {
                var memory = slot.Memory.Slice(0, header.Size);
                if (stream.Read(memory.Span) != header.Size)
                {
                    throw new EndOfStreamException();
                }
                return parser((ResponseTypes)header.Type, header.Size, memory);
            }
        }
    }
}