using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Dqlite.Net.Messages;

namespace Dqlite.Net
{
    public class DqliteDataRecord
    {
        public object this[string name] => GetValue(GetOrdinal(name));
        public object this[int ordinal] => GetValue(ordinal);
        public int FieldCount => this.Columns.Length;

        internal string[] Columns { get; set; }
        internal List<DqliteTypes[]> Types { get; set; }
        internal List<object[]> Values { get; set; }
        internal bool HasRows { get; set; }

        private int index;

        private readonly DqliteClient client;
        internal DqliteDataRecord(DqliteClient client)
        {
            this.client = client;
            this.Columns = new string[0];
            this.Types = new List<DqliteTypes[]>();
            this.Values = new List<object[]>();
            this.index = 0;
            this.HasRows = true;
        }

        public bool Read()
        {
            if(index + 1 >= this.Values.Count && this.HasRows)
            {
                AdvanceRow();
            }

            this.index += 1;
            return this.index < this.Values.Count;
        }

        public bool IsDBNull(int ordinal)
        {
            if (ordinal < 0 || ordinal >= this.FieldCount)
            {
                throw new ArgumentOutOfRangeException();
            }
            return this.Types[index][ordinal] == DqliteTypes.Null;
        }

        public object GetValue(int ordinal)
        {
            if (ordinal < 0 || ordinal >= this.FieldCount)
            {
                throw new ArgumentOutOfRangeException();
            }
            return this.Values[index][ordinal];
        }

        public double GetDouble(int ordinal)
        {
            if (ordinal < 0 || ordinal >= this.FieldCount)
            {
                throw new ArgumentOutOfRangeException();
            }
            return (double)this.Values[index][ordinal];
        }

        public bool GetBoolean(int ordinal)
        {
            if (ordinal < 0 || ordinal >= this.FieldCount)
            {
                throw new ArgumentOutOfRangeException();
            }
            return (bool)this.Values[index][ordinal];
        }

        public long GetInt64(int ordinal)
        {
            if (ordinal < 0 || ordinal >= this.FieldCount)
            {
                throw new ArgumentOutOfRangeException();
            }
            return (long)this.Values[index][ordinal];
        }

        public string GetString(int ordinal)
        {
            if (ordinal < 0 || ordinal >= this.FieldCount)
            {
                throw new ArgumentOutOfRangeException();
            }
            return (string)this.Values[index][ordinal];
        }

        public byte[] GetBlob(int ordinal)
        {
            if (ordinal < 0 || ordinal >= this.FieldCount)
            {
                throw new ArgumentOutOfRangeException();
            }
            return (byte[])this.Values[index][ordinal];
        }

        public DateTime GetDateTime(int ordinal)
        {
            if (ordinal < 0 || ordinal >= this.FieldCount)
            {
                throw new ArgumentOutOfRangeException();
            }
            return (DateTime)this.Values[index][ordinal];
        }

        public string GetName(int ordinal)
        {
            if (ordinal < 0 || ordinal >= this.FieldCount)
            {
                throw new ArgumentOutOfRangeException();
            }

            return this.Columns[ordinal];
        }

        public int GetOrdinal(string name)
        {
            for (var i = 0; i < this.FieldCount; i++)
            {
                if (GetName(i) == name)
                {
                    return i;
                }
            }
            throw new ArgumentOutOfRangeException(nameof(name), name, message: null);
        }

        private void AdvanceRow()
        {
            var message = this.client.ReadMessage();
            message.ThrowOnFailure();
            var response = (ResponseTypes)message.Type;
            if (response != ResponseTypes.ResponseRows)
            {
                throw new InvalidOperationException($"Invalid response: expected {ResponseTypes.ResponseRows}, actually {response}");
            }

            var span = message.Data.AsSpan();

            this.Columns = new string[span.ReadUInt64()];
            this.Types.Clear();
            this.Values.Clear();
            this.index = -1;
            for (var i = 0; i < this.Columns.Length; ++i)
            {
                this.Columns[i] = span.ReadString();
            }

            while (span.Length > 0)
            {
                var rowTypes = new DqliteTypes[this.Columns.Length];
                var rowValues= new object[this.Columns.Length];

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
                        this.HasRows = true;
                        return;
                    }
                    else if (slot == 0xff)
                    {
                        this.HasRows = false;
                        return;
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
                        case DqliteTypes.UnixTime:
                            //TODO
                            rowValues[i] = span.ReadInt64();
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

                this.Types.Add(rowTypes);
                this.Values.Add(rowValues);
            }
        }
    }
}
