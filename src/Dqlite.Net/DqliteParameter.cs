using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Dqlite.Net
{
    public class DqliteParameter : DbParameter
    {
        private object _value;
        private int _size;
        private DqliteTypes _DqliteType;

        public override object Value
        {
            get => _value;
            set
            {
                _value = value;
                _DqliteType = GetDqliteType(value);
                _size = GetSize(_value, _DqliteType);
            }
        }

        public virtual DqliteTypes DqliteType
        {
            get => _DqliteType;
            set => throw new NotSupportedException();
        }

        public override int Size
        {
            get => _size;
            set => throw new NotSupportedException();
        }

        public override DbType DbType { get; set; } = DbType.String;

        public override ParameterDirection Direction
        {
            get => ParameterDirection.Input;
            set
            {
                if (value != ParameterDirection.Input)
                {
                    throw new ArgumentException(nameof(Direction));
                }
            }
        }

        public override bool IsNullable { get; set; }
        public override string ParameterName { get; set; } = string.Empty;
        public override string SourceColumn { get; set; } = string.Empty;
        public override bool SourceColumnNullMapping { get; set; }

        public DqliteParameter()
        {

        }

        public DqliteParameter(string parameterName, object value)
        {
            this.ParameterName = parameterName;
            this.Value = value;
        }

        public DqliteParameter(object value)
        {
            this.Value = value;
        }

        public override void ResetDbType()
            => ResetDqliteType();

        public virtual void ResetDqliteType()
        {
            DbType = DbType.String;
            DqliteType = DqliteTypes.Text;
        }


        public static int GetSize(object value, DqliteTypes type)
        {
            switch(type)
            {
                case DqliteTypes.Integer:
                case DqliteTypes.Float:
                case DqliteTypes.Boolean:
                case DqliteTypes.Null:
                case DqliteTypes.UnixTime:
                    return 8;
                case DqliteTypes.ISO8601:
                case DqliteTypes.Text:
                    return value?.ToString()?.Length ?? 0;
                case DqliteTypes.Blob:
                    return (value as byte[])?.Length ?? 0;
            }
            return 0;
        }

        public static DqliteTypes GetDqliteType(object value)
        {
            if( value == null)
            {
                return DqliteTypes.Null;
            }
            else if(value is byte[])
            {
                return DqliteTypes.Blob;
            }

            var type = value.GetType();
            var code = Type.GetTypeCode(type);

            switch(code)
            {
                case TypeCode.Boolean:
                    return DqliteTypes.Boolean;
                case TypeCode.Double:
                case TypeCode.Single:
                    return DqliteTypes.Float;
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Byte:
                case TypeCode.SByte:
                    return DqliteTypes.Integer;
                case TypeCode.DateTime:
                    return DqliteTypes.ISO8601;
            }

            return DqliteTypes.Text;
        }
    }
}
