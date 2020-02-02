using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Dqlite.Net
{
    internal class DqliteDataRecord : DqliteValueReader
    {
        public int RowCount => this.Values?.Count ?? 0;
        public string[] Columns { get; }
        public List<DqliteTypes[]> Types { get; }
        public List<object[]> Values { get; }
        public bool HasRows => index + 1 < this.RowCount;
        public bool HasAdditionalRows {get;}
        private int index;

        public DqliteDataRecord(string[] columns, List<DqliteTypes[]> types, List<object[]> values, bool hasRows)
        {
            this.Columns = columns;
            this.Types = types;
            this.Values = values;
            this.HasAdditionalRows = hasRows;
            this.index = -1;
        }

        public virtual object this[string name]
            => GetValue(GetOrdinal(name));
        public virtual object this[int ordinal]
            => GetValue(ordinal);
        public override int FieldCount
            => this.Columns?.Length ?? -1;

        public override bool IsDBNull(int ordinal)
            => this.index >= this.RowCount
                ? throw new InvalidOperationException("No data exists for the row/column.")
                : base.IsDBNull(ordinal);
                
        public override object GetValue(int ordinal)
            => this.index >= this.RowCount
                ? throw new InvalidOperationException("No data exists for the row/column.")
                : base.GetValue(ordinal);

        protected override bool GetBooleanCore(int ordinal)
            => Convert.ToBoolean(this.Values[this.index][ordinal]); 

        protected override double GetDoubleCore(int ordinal)
            => Convert.ToDouble(this.Values[this.index][ordinal]);

        protected override long GetInt64Core(int ordinal)
            => Convert.ToInt64(this.Values[this.index][ordinal]);

        protected override string GetStringCore(int ordinal)
            => Convert.ToString(this.Values[this.index][ordinal]);

        protected override byte[] GetBlobCore(int ordinal)
            => (byte[])(this.Values[this.index][ordinal]);

        protected override DqliteTypes GetDqliteType(int ordinal)
        {
            if (ordinal < 0 || ordinal >= FieldCount)
            {
                throw new ArgumentOutOfRangeException(nameof(ordinal), ordinal, message: null);
            }

            return this.Types[this.index][ordinal];
        }

        protected override T GetNull<T>(int ordinal)
            => typeof(T) == typeof(DBNull) || typeof(T) == typeof(object)
                ? (T)(object)DBNull.Value
                : throw new InvalidOperationException(GetOnNullErrorMsg(ordinal));
                
        public virtual string GetName(int ordinal)
        {
            if (ordinal < 0 || ordinal >= FieldCount)
            {
                throw new ArgumentOutOfRangeException(nameof(ordinal), ordinal, message: null);
            }

            return this.Columns[ordinal];
        }

        public virtual int GetOrdinal(string name)
        {
            for (var i = 0; i < FieldCount; i++)
            {
                if (GetName(i) == name)
                {
                    return i;
                }
            }

            throw new ArgumentOutOfRangeException(nameof(name), name, message: null);
        }

        public virtual string GetDataTypeName(int ordinal)
        {
            var dqliteType = GetDqliteType(ordinal);
            switch (dqliteType)
            {
                case DqliteTypes.Integer:
                    return "INTEGER";

                case DqliteTypes.Float:
                    return "REAL";

                case DqliteTypes.Text:
                    return "TEXT";

                case DqliteTypes.Boolean:
                    return "BOOLEAN";

                case DqliteTypes.ISO8601:
                    return "ISO8601";

                default:
                    Debug.Assert(
                        dqliteType == DqliteTypes.Blob || dqliteType == DqliteTypes.Null,
                        "Unexpected column type: " + dqliteType);
                    return "BLOB";
            }
        }

        public virtual Type GetFieldType(int ordinal)
        {
            var dqliteType = GetDqliteType(ordinal);
            return GetFieldTypeFromSqliteType(dqliteType);
        }

        internal static Type GetFieldTypeFromSqliteType(DqliteTypes dqliteType)
        {
            switch (dqliteType)
            {
                case DqliteTypes.Integer:
                    return typeof(long);

                case DqliteTypes.Float:
                    return typeof(double);

                case DqliteTypes.Text:
                    return typeof(string);

                case DqliteTypes.Boolean:
                    return typeof(bool);

                case DqliteTypes.ISO8601:
                    return typeof(DateTime);

                default:
                    Debug.Assert(
                        dqliteType == DqliteTypes.Blob || dqliteType == DqliteTypes.Null,
                        "Unexpected column type: " + dqliteType);
                    return typeof(byte[]);
            }
        }

        public static Type GetFieldType(string type)
        {
            switch (type)
            {
                case "integer":
                    return typeof(long);

                case "real":
                    return typeof(double);

                case "text":
                    return typeof(string);
                
                case "boolean":
                    return typeof(bool);

                case "iso8601":
                    return typeof(DateTime);

                default:
                    Debug.Assert(type == "blob" || type == null, "Unexpected column type: " + type);
                    return typeof(byte[]);
            }
        }

        public virtual long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            var blob = GetBlob(ordinal);

            long bytesToRead = blob.Length - dataOffset;
            if (buffer != null)
            {
                bytesToRead = Math.Min(bytesToRead, length);
                Array.Copy(blob, dataOffset, buffer, bufferOffset, bytesToRead);
            }

            return bytesToRead;
        }

        public virtual long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            var text = GetString(ordinal);

            int charsToRead = text.Length - (int)dataOffset;
            charsToRead = Math.Min(charsToRead, length);
            text.CopyTo((int)dataOffset, buffer, bufferOffset, charsToRead);
            return charsToRead;
        }

        public bool Read()
        {
            return ++this.index < this.Values?.Count;
        }
    }
}
