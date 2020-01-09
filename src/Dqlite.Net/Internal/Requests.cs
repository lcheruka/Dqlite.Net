using System;

namespace Dqlite.Net
{
    internal static class Requests
    {
        public static void Write(Span<byte> span, ulong id)
            => span.Empty()
                    .Write(id);

        public static void Write(Span<byte> span, string name)
            => span.Empty()
                    .Write(name);

        public static void Write(Span<byte> span, DatabaseRecord database, string text)
            => span.Empty()
                    .Write((ulong)database.Id)
                    .Write(text);

        public static void Write(Span<byte> span, PreparedStatement preparedStatement, DqliteParameter[] parameters)
            => span.Empty()
                    .Write(preparedStatement.DatabaseId)
                    .Write(preparedStatement.Id)
                    .Write(parameters);

        public static void Write(Span<byte> span, PreparedStatement preparedStatement)
            => span.Empty()
                    .Write(preparedStatement.DatabaseId)
                    .Write(preparedStatement.Id);
        public static void Write(Span<byte> span, DatabaseRecord database, string text, DqliteParameter[] parameters)
            => span.Empty()
                    .Write((ulong)database.Id)
                    .Write(text)
                    .Write(parameters);

        public static void Write(Span<byte> span, DatabaseRecord database)
            => span.Empty()
                    .Write((ulong)database.Id);

        public static void Write(Span<byte> span, ulong nodeId, string address)
            => span.Empty()
                    .Write(nodeId)
                    .Write(address);
    }
}