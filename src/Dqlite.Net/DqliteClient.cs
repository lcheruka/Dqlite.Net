using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
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

        public void Open(string address, int port)
        {
            var span = (Span<byte>)stackalloc byte[8];
            span.Write(VERSION);

            this.client.Connect(address, port);
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
            this.SendMessage(RequestTypes.RequestLeader, REVISION, span);
            return this.ReadMessage()
                .AsNodeResponse();
        }

        public void RegisterClient(ulong clientId)
        {
            var span = (Span<byte>)stackalloc byte[8];
            span.Write(clientId);
            this.SendMessage(RequestTypes.RequestClient, REVISION, span);
            this.ReadMessage()
                .AsWelcomeResponse();
        }

        public DatabaseRecord OpenDatabase(string name)
        {
            var length = PadWord(name.Length + 1) + 16;
            var span = (Span<byte>)stackalloc byte[length];
            span.Write(name);

            this.SendMessage(RequestTypes.RequestOpen, REVISION, span);
            return this.ReadMessage()
                .AsDatabaseResponse();
        }

        public PreparedStatementRecord PrepareStatement(DatabaseRecord database, string text)
        {
            var length = PadWord(text.Length + 1) + 8;
            var span = (Span<byte>)stackalloc byte[length];
            span.Write((ulong)database.Id)
                .Write(text);

            this.SendMessage(RequestTypes.RequestPrepare, REVISION, span);
            return this.ReadMessage().AsPreparedStatementResponse();
        }

        public StatementResult ExecuteStatement(PreparedStatementRecord preparedStatement, params DqliteParameter[] parameters)
        {
            var length = 8 + GetSize(parameters);
            var span = (Span<byte>)new byte[length];

            span.Write(preparedStatement.DatabaseId)
                .Write(preparedStatement.Id)
                .Write(parameters);

            this.SendMessage(RequestTypes.RequestExec, REVISION, span);
            return this.ReadMessage().AsStatementResultResponse();
        }

        public DqliteDataRecord ExecuteQuery(PreparedStatementRecord preparedStatement, params DqliteParameter[] parameters)
        {
            var length = 8 + GetSize(parameters);
            var span = (Span<byte>)new byte[length];

            span.Write(preparedStatement.DatabaseId)
                .Write(preparedStatement.Id)
                .Write(parameters);

            this.SendMessage(RequestTypes.RequestQuery, REVISION, span);
            return new DqliteDataRecord(this);
        }

        public void FinalizeStatement(PreparedStatementRecord preparedStatement)
        {
            var span = (Span<byte>)stackalloc byte[8];

            span.Write(preparedStatement.DatabaseId)
                .Write(preparedStatement.Id);

            this.SendMessage(RequestTypes.RequestFinalize, REVISION, span);
            ReadMessage().AsAknowledgmentResponse();
        }

        public StatementResult ExecuteNonQuery(DatabaseRecord database, string text, params DqliteParameter[] parameters)
        {
            var length = 8 + PadWord(text.Length+1) + GetSize(parameters);
            var span = (Span<byte>)new byte[length];

            span.Write((ulong)database.Id)
                .Write(text)
                .Write(parameters);

            this.SendMessage(RequestTypes.RequestExecSQL, REVISION, span);
            return this.ReadMessage().AsStatementResultResponse();
        }

        public DqliteDataRecord ExecuteQuery(DatabaseRecord database, string text, params DqliteParameter[] parameters)
        {
            var length = 8 + PadWord(text.Length+1) + GetSize(parameters);
            var span = (Span<byte>)new byte[length];

            span.Write((ulong)database.Id)
                .Write(text)
                .Write(parameters);

            this.SendMessage(RequestTypes.RequestQuerySQL, REVISION, span);
            return new DqliteDataRecord(this);
        }

        public void InterruptStatement(DatabaseRecord database)
        {
            var span = (Span<byte>)new byte[8];
            span.Write((ulong)database.Id);

            this.SendMessage(RequestTypes.RequestInterrupt, REVISION, span);
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

            this.SendMessage(RequestTypes.RequestJoin, REVISION, span);
            this.ReadMessage()
                .AsAknowledgmentResponse();
        }

        public void PromoteNode(ulong nodeId)
        {
            var span = (Span<byte>)stackalloc byte[8];
            span.Write(nodeId);

            this.SendMessage(RequestTypes.RequestPromote, REVISION, span);
            this.ReadMessage()
                .AsAknowledgmentResponse();
        }

        public void RemoveNode(ulong nodeId)
        {
            var span = (Span<byte>)stackalloc byte[8];
            span.Write(nodeId);

            this.SendMessage(RequestTypes.RequestRemove, REVISION, span);
            this.ReadMessage()
                .AsAknowledgmentResponse();
        }

        public DatabaseDump DumpDatabase(string name)
        {
            var length = PadWord(name.Length + 1);
            var span = (Span<byte>)stackalloc byte[length];
            span.Write(name);

            this.SendMessage(RequestTypes.RequestDump, 0, span);
            return this.ReadMessage()
                .AsDatabaseDumpResponse();
        }

        public IEnumerable<NodeRecord> EnumerateNodes()
        {
            var span = (Span<byte>) stackalloc byte[8];
            this.SendMessage(RequestTypes.RequestCluster, 0, span);
            return this.ReadMessage()
                .AsNodesResponse();
        }

        internal void SendMessage(RequestTypes type, byte revision, Span<byte> data)
        {
            if(data.Length % 8 != 0)
            {
                throw new InvalidOperationException();
            }

            var size = data.Length / 8;
            var header = (Span<byte>)stackalloc byte[8];
            header.Write(size)
                .Write((byte)type)
                .Write(revision);
                
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

        public static DqliteClient FindLeader(string address, int port)
        {
            var client = default(DqliteClient);
            try
            {
                while (true)
                {
                    client = new DqliteClient();
                    client.Open(address, port);

                    var leader = client.GetLeader();
                    if (leader == null)
                    {
                        return null;
                    }
                    else if (leader.Address != $"{address}:{port}")
                    {
                        var index = leader.Address.IndexOf(':');
                        address = leader.Address.Substring(0, index);
                        port = int.Parse(leader.Address.Substring(index + 1));
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