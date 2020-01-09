using System;
using System.Collections.Generic;
using System.Text;
using static Dqlite.Net.Utils;

namespace Dqlite.Net
{
    internal unsafe static class SpanExtensions
    {
        public static byte ReadByte(this ref Span<byte> bytes)
        {
            var value = bytes[0];
            bytes = bytes.Slice(1);
            return value;
        }

        public static byte[] ReadBytes(this ref Span<byte> bytes, int length)
        {
            var value = bytes.Slice(0, length).ToArray();
            bytes = bytes.Slice(length);
            return value;
        }

        public static int ReadInt32(this ref Span<byte> bytes)
        {
            var value = (bytes[3] << 24) | (bytes[2] << 16) | (bytes[1] << 8) | bytes[0];
            bytes = bytes.Slice(4);

            return value;
        }

        public static uint ReadUInt32(this ref Span<byte> bytes)
        {
            return (uint)bytes.ReadInt32();
        }

        public static long ReadInt64(this ref Span<byte> bytes)
        {
            var value1 = (bytes[3] << 24) | (bytes[2] << 16) | (bytes[1] << 8) | bytes[0];
            var value2 = (bytes[7] << 24) | (bytes[6] << 16) | (bytes[5] << 8) | bytes[4];
            bytes = bytes.Slice(8);

            return (uint)value1 | ((long)value2 << 32 );
        }

        public static ulong ReadUInt64(this ref Span<byte> bytes)
        {
            return (ulong)bytes.ReadInt64();
        }

        public static double ReadDouble(this ref Span<byte> bytes)
        {
            var value = bytes.ReadInt64();
            return *(double*)&value;
        }

        public static string ReadString(this ref Span<byte> bytes)
        {
            var index = bytes.IndexOf((byte)0);
            if(index == -1)
            {
                return null;
            }

            var value = Encoding.UTF8.GetString(bytes.Slice(0, index++));

            if(index % 8 != 0)
            {
                index += (8 - (index % 8));
            }

            bytes = bytes.Slice(index);

            return value;
        }

        public static byte[] ReadBlob(this ref Span<byte> bytes)
        {
            var length = (int)bytes.ReadUInt64();
            var data = bytes.Slice(0, length);
            bytes = bytes.Slice(length);
            return data.ToArray();
        }

        public static Span<byte> Empty(this Span<byte> bytes)
        {
            bytes.Clear();
            return bytes;
        }

        public static Span<byte> Write(this Span<byte> bytes, byte value)
        {
            bytes[0] = value;
            return bytes.Slice(1);
        }

        public static Span<byte> Write(this Span<byte> bytes, byte[] value)
        {
            for(int i = 0; i < value.Length; ++i)
            {
                bytes[i] = value[i];
            }
            return bytes.Slice(value.Length);
        }

        public static Span<byte> Write(this Span<byte> bytes, int value)
        {
            fixed (byte* b = bytes)
            {
                *((int*)b) = value;
            }
            return bytes.Slice(4);
        }

        public static Span<byte> Write(this Span<byte> bytes, uint value)
        {
            return bytes.Write((int)value);
        }

        public static Span<byte> Write(this Span<byte> bytes, long value)
        {
            fixed (byte* b = bytes)
            {
                *((long*)b) = value;
            }
            return bytes.Slice(8);
        }

        public static Span<byte> Write(this Span<byte> bytes, ulong value)
        {
            return bytes.Write((long)value);
        }

        public static Span<byte> Write(this Span<byte> bytes, double value)
        {
            return bytes.Write(*(long*)&value);
        }

        public static Span<byte> Write(this Span<byte> bytes, string value)
        {
            var length = PadWord(value.Length+1);
            Encoding.UTF8.GetBytes(value.AsSpan(), bytes);
            return bytes.Slice(length);
        }

        public static Span<byte> Write(this Span<byte> bytes, DqliteParameter[] parameters)
        {
            var headerBits = 8 + parameters.Length * 8;
            var padBits = 0;
            if(headerBits % 64 != 0)
            {
                padBits = 64 - (headerBits % 64);
            }

            var pad = padBits / 8;
            var span = bytes.Write((byte)parameters.Length);
            foreach (var parameter in parameters)
            {
                span = span.Write((byte)parameter.DqliteType);
            }

            if(pad != 0)
            {
                span = span.Slice(pad);
            }

            foreach (var parameter in parameters)
            {
                switch (parameter.DqliteType)
                {
                    case DqliteTypes.Integer:
                        span = span.Write(Convert.ToInt64(parameter.Value));
                        break;
                    case DqliteTypes.Float:
                        span = span.Write(Convert.ToDouble(parameter.Value));
                        break;
                    case DqliteTypes.Boolean:
                        span = span.Write(Convert.ToUInt64(parameter.Value));
                        break;
                    case DqliteTypes.Null:
                        span = span.Write(0UL);
                        break;
                    /*
                    case DqliteTypes.UnixTime:
                        span = span.Write((ulong)parameter.Value);
                        break;
                    */
                    case DqliteTypes.ISO8601:
                    case DqliteTypes.Text:
                        span = span.Write(parameter.Value.ToString());
                        break;
                    case DqliteTypes.Blob:
                        {
                            var value = (byte[])parameter.Value;
                            span = span.Write((ulong)value.Length);
                            span = span.Write(value);
                        }
                        break;
                }
            }
            return span;
        }
    }
}
