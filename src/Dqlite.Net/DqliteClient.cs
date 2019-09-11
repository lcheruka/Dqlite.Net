using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Dqlite.Net.Messages;
using static Dqlite.Net.Utils;

namespace Dqlite.Net
{
    public class DqliteClient : IDisposable
    {
        public bool Connected => this.client?.Connected ?? false;
        private readonly ulong VERSION = 1;
        private readonly byte REVISION = 0;

        private Stream stream;
        private readonly TcpClient client;

        public DqliteClient()
        {
            this.client = new TcpClient();
            this.client.NoDelay = true;
        }

        public void Open(string address)
        {
            if(!TryParseAddress(address, out var host, out var port))
            {
                throw new FormatException("Invalid address format");
            }
            var span = (Span<byte>)stackalloc byte[8];
            span.Write(VERSION);

            this.client.Connect(host, port);
            this.stream = this.client.GetStream();
            this.stream.Write(span);
            this.stream.Flush();

            try
            {
                GetLeader();
            }
            catch(Exception ex)
            {
                throw new DqliteException(1, "Failed to connect to server", ex);
            }
        }

        public NodeRecord GetLeader()
        {
            var span = (Span<byte>)stackalloc byte[8];
            this.SendMessage(RequestTypes.RequestLeader,  span);
            return this.ReadMessage()
                .AsNodeResponse();
        }

        public void RegisterClient(ulong clientId)
        {
            var span = (Span<byte>)stackalloc byte[8];
            span.Write(clientId);
            this.SendMessage(RequestTypes.RequestClient,  span);
            this.ReadMessage()
                .AsWelcomeResponse();
        }

        public DatabaseRecord OpenDatabase(string name)
        {
            var length = PadWord(name.Length + 1) + 16;
            var span = (Span<byte>)stackalloc byte[length];
            span.Write(name);

            this.SendMessage(RequestTypes.RequestOpen,  span);
            return this.ReadMessage()
                .AsDatabaseResponse();
        }

        public PreparedStatementRecord PrepareStatement(DatabaseRecord database, string text)
        {
            var length = PadWord(text.Length + 1) + 8;
            var span = (Span<byte>)stackalloc byte[length];
            span.Write((ulong)database.Id)
                .Write(text);

            this.SendMessage(RequestTypes.RequestPrepare,  span);
            return this.ReadMessage().AsPreparedStatementResponse();
        }

        public StatementResult ExecuteStatement(PreparedStatementRecord preparedStatement, params DqliteParameter[] parameters)
        {
            var length = 8 + GetSize(parameters);
            var span = (Span<byte>)new byte[length];

            span.Write(preparedStatement.DatabaseId)
                .Write(preparedStatement.Id)
                .Write(parameters);

            this.SendMessage(RequestTypes.RequestExec,  span);
            return this.ReadMessage().AsStatementResultResponse();
        }

        public DqliteDataRecord ExecuteQuery(PreparedStatementRecord preparedStatement, params DqliteParameter[] parameters)
        {
            var length = 8 + GetSize(parameters);
            var span = (Span<byte>)new byte[length];

            span.Write(preparedStatement.DatabaseId)
                .Write(preparedStatement.Id)
                .Write(parameters);

            this.SendMessage(RequestTypes.RequestQuery,  span);
            return new DqliteDataRecord(this);
        }

        public void FinalizeStatement(PreparedStatementRecord preparedStatement)
        {
            var span = (Span<byte>)stackalloc byte[8];

            span.Write(preparedStatement.DatabaseId)
                .Write(preparedStatement.Id);

            this.SendMessage(RequestTypes.RequestFinalize,  span);
            ReadMessage().AsAknowledgmentResponse();
        }

        public StatementResult ExecuteNonQuery(DatabaseRecord database, string text, params DqliteParameter[] parameters)
        {
            var length = 8 + PadWord(text.Length+1) + GetSize(parameters);
            var span = (Span<byte>)new byte[length];

            span.Write((ulong)database.Id)
                .Write(text)
                .Write(parameters);

            this.SendMessage(RequestTypes.RequestExecSQL,  span);
            return this.ReadMessage().AsStatementResultResponse();
        }

        public DqliteDataRecord ExecuteQuery(DatabaseRecord database, string text, params DqliteParameter[] parameters)
        {
            var length = 8 + PadWord(text.Length+1) + GetSize(parameters);
            var span = (Span<byte>)new byte[length];

            span.Write((ulong)database.Id)
                .Write(text)
                .Write(parameters);

            this.SendMessage(RequestTypes.RequestQuerySQL,  span);
            return new DqliteDataRecord(this);
        }

        public void InterruptStatement(DatabaseRecord database)
        {
            var span = (Span<byte>)new byte[8];
            span.Write((ulong)database.Id);

            this.SendMessage(RequestTypes.RequestInterrupt,  span);
            var response = default(Message);
            while (!(response?.AsAknowledgmentResponse() ?? false))
            {
                response = this.ReadMessage();
            }
        }

        public void AddNode(ulong nodeId, string address)
        {
            var length = PadWord( address.Length + 1) + 8;
            var span = (Span<byte>)stackalloc byte[length];
            span.Write(nodeId)
                .Write(address);

            this.SendMessage(RequestTypes.RequestJoin,  span);
            this.ReadMessage()
                .AsAknowledgmentResponse();
        }

        public void PromoteNode(ulong nodeId)
        {
            var span = (Span<byte>)stackalloc byte[8];
            span.Write(nodeId);

            this.SendMessage(RequestTypes.RequestPromote,  span);
            this.ReadMessage()
                .AsAknowledgmentResponse();
        }

        public void RemoveNode(ulong nodeId)
        {
            var span = (Span<byte>)stackalloc byte[8];
            span.Write(nodeId);

            this.SendMessage(RequestTypes.RequestRemove,  span);
            this.ReadMessage()
                .AsAknowledgmentResponse();
        }

        public DatabaseDump DumpDatabase(string name)
        {
            var length = PadWord(name.Length + 1);
            var span = (Span<byte>)stackalloc byte[length];
            span.Write(name);

            this.SendMessage(RequestTypes.RequestDump, span);
            return this.ReadMessage()
                .AsDatabaseDumpResponse();
        }

        public IEnumerable<NodeRecord> EnumerateNodes()
        {
            var span = (Span<byte>) stackalloc byte[8];
            this.SendMessage(RequestTypes.RequestCluster, span);
            return this.ReadMessage()
                .AsNodesResponse();
        }

        internal void SendMessage(RequestTypes type, ReadOnlySpan<byte> data)
        {
            if(data.Length % 8 != 0)
            {
                throw new InvalidOperationException();
            }

            var size = data.Length / 8;
            var header = (Span<byte>)stackalloc byte[8];
            header.Write(size)
                .Write((byte)type)
                .Write(REVISION);
                
            stream.Write(header);
            stream.Write(data);
        }

        internal Message ReadMessage()
        {
            var message = new Message();
            var header = (Span<byte>) stackalloc byte[8];

            var amount = stream.Read(header);
            if (amount != header.Length)
            {
                throw new EndOfStreamException();
            }

            message.Size = header.ReadInt32();
            message.Type = header.ReadByte();
            message.Revision = header.ReadByte();
            message.Data = new byte[message.Size * 8];

            if (stream.Read(message.Data, 0, message.Data.Length) != message.Data.Length)
            {
                throw new EndOfStreamException();
            }

            return message;
        }

        public void Dispose(){
            this.stream?.Dispose();
            this.client?.Dispose();
        }

        public static async Task<DqliteClient> CreateAsync(bool leaderOnly, params string[] nodes)
        {
            for(int i = 0; i < 5;++i)
            {
                foreach(var node in nodes)
                {
                    try
                    {
                        var client = Create(node, leaderOnly);
                        if(client != null)
                        {
                            return client;
                        }
                    }
                    catch
                    {

                    }
                }
                await Task.Delay(250*i+500);
            }
            throw new DqliteException(1, "Failed to connect to node");
        }

        public static DqliteClient Create(string address, bool leaderOnly)
        {
            var client = default(DqliteClient);
            try
            {
                while (true)
                {
                    client = new DqliteClient();
                    client.Open(address);

                    var leader = client.GetLeader();
                    if (leader == null)
                    {
                        return null;
                    }
                    else if (leaderOnly && leader.Address != address)
                    {
                        address = leader.Address;
                        client.Dispose();
                        continue;
                    }

                    return client;                    
                }
            }
            catch
            {
                client?.Dispose();
                throw;
            }
        }
    }
}