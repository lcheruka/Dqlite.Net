using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Dqlite.Net
{
    public class DqliteDataReader : IDataReader
    {
        public int Depth => 0;
        public bool IsClosed { get; private set; }
        public int RecordsAffected { get; private set; }
        public int FieldCount => IsClosed ? throw new NotImplementedException() : record.FieldCount;
        public object this[string name] => IsClosed ? throw new NotImplementedException() : record[name];
        public object this[int i] => IsClosed ? throw new NotImplementedException() : record[i];


        private DqliteDataRecord record;
        private bool closeConnection;
        private readonly DqliteCommand command;
        private readonly PreparedStatementRecord statement;
        private readonly DatabaseRecord database;
        private readonly DqliteParameter[] parameters;
        private readonly string commandText;

        internal DqliteDataReader(DqliteCommand command, PreparedStatementRecord statement, DqliteParameter[] parameters, bool closeConnection)
        {
            this.command = command;
            this.statement = statement;
            this.parameters = parameters;
            this.closeConnection = closeConnection;
        }

        internal DqliteDataReader(DqliteCommand command, DatabaseRecord database, string commandText, DqliteParameter[] parameters, bool closeConnection)
        {
            this.command = command;
            this.database = database;
            this.commandText = commandText;
            this.parameters = parameters;
            this.closeConnection = closeConnection;
        }

        public bool NextResult()
        {
            return false;
        }

        public bool Read()
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            if(this.record == null)
            {
                if(this.statement != null)
                {
                    this.record = this.command.Connection.Client.ExecuteQuery(this.statement, this.parameters);
                }
                else
                {
                    this.record = this.command.Connection.Client.ExecuteQuery(this.database, this.commandText, this.parameters);
                }
            }

            if (this.record.Read())
            {
                RecordsAffected += this.record.Values.Count;
                return true;
            }
            return false;
        }

        public bool GetBoolean(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return this.record.GetBoolean(i);
        }

        public byte GetByte(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return (byte)this.record.GetInt64(i);
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return this.record.GetDateTime(i);
        }

        public decimal GetDecimal(int i)
        {
            var value = this.GetDouble(i);
            return Convert.ToDecimal(value);
        }

        public double GetDouble(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return this.record.GetDouble(i);
        }

        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return (float)this.record.GetDouble(i);
        }

        public Guid GetGuid(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            var value = this.record.GetBlob(i);
            return new Guid(value);
        }

        public short GetInt16(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return (short)this.record.GetInt64(i);
        }

        public int GetInt32(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return (int)this.record.GetInt64(i);
        }

        public long GetInt64(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return this.record.GetInt64(i);
        }

        public string GetName(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return this.record.GetName(i);
        }

        public int GetOrdinal(string name)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return this.record.GetOrdinal(name);
        }

        public string GetString(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return this.record.GetString(i);
        }

        public object GetValue(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return this.record.GetValue(i);
        }

        public int GetValues(object[] values)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            var i = 0;
            for(; i < values.Length && i < this.record.FieldCount; ++i)
            {
                values[i] = this.record[i];
            }
            return i;
        }

        public bool IsDBNull(int i)
        {
            if (IsClosed)
            {
                throw new InvalidOperationException("DataReader is closed");
            }

            return this.record.IsDBNull(i);
        }

        public void Close()
        {
            if (IsClosed)
            {
                return;
            }

            command.Connection.Client.InterruptStatement(command.Connection.CurrentDatabase);
            command.DataReader = null;
            IsClosed = true;

            if (closeConnection)
            {
                command.Connection.Close();
            }
        }

        public void Dispose()
        {
            this.Close();
        }
    }
}
