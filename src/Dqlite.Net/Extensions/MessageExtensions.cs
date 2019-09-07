using System;
using System.Collections.Generic;
using System.Text;
using Dqlite.Net.Messages;

namespace Dqlite.Net
{
    public static class MessageExtensions
    {
        public static Message ThrowOnFailure(this Message message)
        {
            var response = (ResponseTypes)message.Type;
            if(response == ResponseTypes.ResponseFailure)
            {
                var span = message.Data.AsSpan();
                var errorCode = span.ReadUInt64();
                var errorMessage = span.ReadString();

                throw new DqliteException(errorCode, errorMessage);
            }

            return message;
        }

        public static NodeRecord AsNodeResponse(this Message message)
        {
            message.ThrowOnFailure();
            var response = (ResponseTypes)message.Type;
            if (response != ResponseTypes.ResponseNode)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseNode}, actually {response}");
            }
            else if(message.Data.Length == 0)
            {
                return null;
            }

            var span = message.Data.AsSpan();

            var node = new NodeRecord();
            node.Id = span.ReadUInt64();
            node.Address = span.ReadString();

            return node;
        }

        public static void AsWelcomeResponse(this Message message)
        {
            message.ThrowOnFailure();
            var response = (ResponseTypes)message.Type;
            if (response != ResponseTypes.ResponseWelcome)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseWelcome}, actually {response}");
            }

            var span = message.Data.AsSpan();
            span.ReadUInt64();
        }

        public static IEnumerable<NodeRecord> AsNodesResponse(this Message message)
        {
            message.ThrowOnFailure();
            var response = (ResponseTypes)message.Type;
            if (response != ResponseTypes.ResponseNodes)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseNodes}, actually {response}");
            }

            var span = message.Data.AsSpan();

            var count = span.ReadUInt64();
            var nodes = new NodeRecord[count];
            for (var i = 0UL; i < count; ++i)
            {
                nodes[i] = new NodeRecord();
                nodes[i].Id = span.ReadUInt64();
                nodes[i].Address = span.ReadString();
            }

            return nodes;
        }

        public static DatabaseRecord AsDatabaseResponse(this Message message)
        {
            message.ThrowOnFailure();
            var response = (ResponseTypes)message.Type;
            if (response != ResponseTypes.ResponseDb)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseDb}, actually {response}");
            }

            var span = message.Data.AsSpan();
            var database = new DatabaseRecord();
            database.Id = span.ReadUInt32();

            return database;
        }

        public static PreparedStatementRecord AsPreparedStatementResponse(this Message message)
        {
            message.ThrowOnFailure();
            var response = (ResponseTypes)message.Type;
            if (response != ResponseTypes.ResponseStmt)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseStmt}, actually {response}");
            }

            var span = message.Data.AsSpan();
            var statement = new PreparedStatementRecord();
            statement.DatabaseId = span.ReadUInt32();
            statement.Id = span.ReadUInt32();
            statement.ParameterCount = span.ReadUInt64();

            return statement;
        }

        public static StatementResult AsStatementResultResponse(this Message message)
        {
            message.ThrowOnFailure();
            var response = (ResponseTypes)message.Type;
            if (response != ResponseTypes.ResponseResult)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseResult}, actually {response}");
            }

            var span = message.Data.AsSpan();
            var statement = new StatementResult();
            statement.LastRowId = span.ReadUInt32();
            statement.RowCount = span.ReadUInt32();

            return statement;
        }

        public static bool AsAknowledgmentResponse(this Message message)
        {
            message.ThrowOnFailure();
            var response = (ResponseTypes)message.Type;

            return response == ResponseTypes.ResponseEmpty;
        }

        public static DatabaseDump AsDatabaseDumpResponse(this Message message)
        {
            message.ThrowOnFailure();
            var response = (ResponseTypes)message.Type;
            if (response != ResponseTypes.ResponseEmpty)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseEmpty}, actually {response}");
            }

            var database = new DatabaseDump();
            var span = message.Data.AsSpan();

            database.Main = new DatabaseFile();
            database.Main.Name = span.ReadString();
            database.Main.Size = (int)span.ReadUInt64();
            database.Main.Data = span.ReadBytes(database.Main.Size);

            database.Log = new DatabaseFile();
            database.Log.Name = span.ReadString();
            database.Log.Size = (int)span.ReadUInt64();
            database.Log.Data = span.ReadBytes(database.Log.Size);

            return database;
        }

    }
}
