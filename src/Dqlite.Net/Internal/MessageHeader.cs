using System;

namespace Dqlite.Net
{
    internal struct MessageHeader
    {
        public int Size { get; }
        public byte Type { get; }
        public byte Revision {get; }
        public ushort Unused { get; }

        public MessageHeader(int size, byte type, byte revision)
        {
            this.Size = size;
            this.Type = type;
            this.Revision = revision;
            this.Unused = 0;
        }

        public static MessageHeader Parse(Span<byte> span)
        {
            var size = span.ReadInt32() * 8;
            var type = span.ReadByte();
            var revision = span.ReadByte();

            return new MessageHeader(size, type, revision);
        }
    }
}