using System;
using System.Collections.Generic;
using System.IO;

namespace Dqlite.Net
{
    internal static class ResponseParsers
    {
        public static void ThrowOnFailure(ResponseTypes type, int size, Memory<byte> data)
        {
            if(type == ResponseTypes.ResponseFailure)
            {
                var span = data.Span;
                var errorCode = span.ReadUInt64();
                var errorMessage = span.ReadString();

                throw new DqliteException(errorCode, errorMessage);
            }
        }

        public static DqliteNodeInfo ParseNodeResponse(ResponseTypes type, int size, Memory<byte> data)
        {
            ThrowOnFailure(type, size, data);
            if (type != ResponseTypes.ResponseNode)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseNode}, actually {type}");
            }
            else if(data.Length == 0)
            {
                return null;
            }

            var span = data.Span;

            var node = new DqliteNodeInfo();
            node.Id = span.ReadUInt64();
            node.Address = span.ReadString();

            return node;
        }

        public static bool ParseWelcomeResponse(ResponseTypes type, int size, Memory<byte> data)
        {
            ThrowOnFailure(type, size, data);
            if (type != ResponseTypes.ResponseWelcome)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseWelcome}, actually {type}");
            }

            var span = data.Span;
            span.ReadUInt64();
            return true;
        }

        public static DqliteNodeInfo[] ParseNodesResponse(ResponseTypes type, int size, Memory<byte> data)
        {
            ThrowOnFailure(type, size, data);
            if (type != ResponseTypes.ResponseNodes)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseNodes}, actually {type}");
            }

            var span = data.Span;

            var count = span.ReadUInt64();
            var nodes = new DqliteNodeInfo[count];
            for (var i = 0UL; i < count; ++i)
            {
                nodes[i] = new DqliteNodeInfo();
                nodes[i].Id = span.ReadUInt64();
                nodes[i].Address = span.ReadString();
                if(span.Length > 0)
                {
                    nodes[i].Role = (DqliteNodeRoles) span.ReadUInt64();
                }
            }

            return nodes;
        }

        public static DatabaseRecord ParseDatabaseResponse(ResponseTypes type, int size, Memory<byte> data)
        {
            ThrowOnFailure(type, size, data);
            if (type != ResponseTypes.ResponseDb)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseDb}, actually {type}");
            }

            var span = data.Span;
            var database = new DatabaseRecord();
            database.Id = span.ReadUInt32();

            return database;
        }

        public static PreparedStatement ParsePreparedStatementResponse(ResponseTypes type, int size, Memory<byte> data)
        {
            ThrowOnFailure(type, size, data);
            if (type != ResponseTypes.ResponseStmt)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseStmt}, actually {type}");
            }

            var span = data.Span;
            var statement = new PreparedStatement();
            statement.DatabaseId = span.ReadUInt32();
            statement.Id = span.ReadUInt32();
            statement.ParameterCount = span.ReadUInt64();

            return statement;
        }

        public static StatementResult ParseStatementResultResponse(ResponseTypes type, int size, Memory<byte> data)
        {
            ThrowOnFailure(type, size, data);
            if (type != ResponseTypes.ResponseResult)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseResult}, actually {type}");
            }

            var span = data.Span;
            var statement = new StatementResult();
            statement.LastRowId = span.ReadUInt32();
            statement.RowCount = span.ReadUInt32();

            return statement;
        }

        public static bool ParseAknowledgmentResponse(ResponseTypes type, int size, Memory<byte> data)
        {
            ThrowOnFailure(type, size, data);
            return type == ResponseTypes.ResponseEmpty;
        }

        public static DqliteDataRecord ParseDataRecordResponse(ResponseTypes type, int size, Memory<byte> data)
        {
            ThrowOnFailure(type, size, data);
            if (type != ResponseTypes.ResponseRows)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseRows}, actually {type}");
            }

            var span = data.Span;

            var columns = new string[span.ReadUInt64()];
            var types = new List<DqliteTypes[]>();
            var values = new List<object[]>();

            for (var i = 0; i < columns.Length; ++i)
            {
                columns[i] = span.ReadString();
            }

            while (span.Length > 0)
            {
                var rowTypes = new DqliteTypes[columns.Length];
                var rowValues= new object[columns.Length];

                var headerBits = rowTypes.Length * 4;
                var padBits = 0;
                if (headerBits % 64 != 0)
                {
                    padBits = 64 - (headerBits % 64);
                }

                var headerSize = (headerBits + padBits) / 64 * 8;
                for (var i = 0; i < headerSize; ++i)
                {
                    var slot = span.ReadByte();
                    if (slot == 0xee)
                    {
                        // More rows are available.
                        return new DqliteDataRecord(columns, types, values, true);
                    }
                    else if (slot == 0xff)
                    {
                        return new DqliteDataRecord(columns, types, values, false);
                    }

                    var index = i * 2;
                    if (index >= rowTypes.Length)
                    {
                        // This is padding.
                        continue;
                    }
                    rowTypes[index] = (DqliteTypes)(slot & 0x0f);

                    if (++index >= rowTypes.Length)
                    {
                        // This is padding.
                        continue;
                    }
                    rowTypes[index] = (DqliteTypes)(slot >> 4);
                }
                
                for (var i = 0; i < rowTypes.Length; ++i)
                {
                    switch (rowTypes[i])
                    {
                        case DqliteTypes.Integer:
                            rowValues[i] = span.ReadInt64();
                            break;
                        case DqliteTypes.Float:
                            rowValues[i] = span.ReadDouble();
                            break;
                        case DqliteTypes.Blob:
                            rowValues[i] = span.ReadBlob();
                            break;
                        case DqliteTypes.Text:
                            rowValues[i] = span.ReadString();
                            break;
                        case DqliteTypes.Null:
                            span.ReadUInt64();
                            rowValues[i] = null;
                            break;
                        case DqliteTypes.ISO8601:
                            {
                                var value = span.ReadString();
                                if (string.IsNullOrEmpty(value))
                                {
                                    rowValues[i] = DateTime.MinValue;
                                }

                                rowValues[i] = DateTime.Parse(value);
                                break;
                            }
                        case DqliteTypes.Boolean:
                            rowValues[i] = span.ReadUInt64() != 0;
                            break;
                        default:
                            throw new DqliteException(0, "Unknown type");
                    }
                }

                types.Add(rowTypes);
                values.Add(rowValues);
            }
            
            throw new EndOfStreamException();
        }

    }
}
