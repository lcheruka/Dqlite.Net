using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Dqlite.Net.Properties;

namespace Dqlite.Net
{
    public class DqliteDataReader : DbDataReader
    {
        public override object this[string name]
            => this.record == null
                ? throw new InvalidOperationException(Resources.NoData)
                : this.record[name];
        public override object this[int ordinal]
            => this.record == null
                ? throw new InvalidOperationException(Resources.NoData)
                : this.record[ordinal];
        public override int Depth => 0;

        public override int FieldCount 
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(FieldCount)))
                : this.record?.Columns?.Length ?? 0;

        public override bool HasRows => this.record?.HasRows ?? false;

        public override bool IsClosed => this.closed;

        public override int RecordsAffected => this.recordsAffected;


        private DqliteDataRecord record;
        private int recordsAffected;
        private bool closed;

        private readonly DqliteConnection connection;
        private readonly DqliteCommand command;
        private readonly bool closeConnection;

        internal DqliteDataReader(
            DqliteCommand command, 
            DqliteDataRecord record,
            bool closeConnection
        )
        {
            this.command = command;
            this.connection = command.Connection;
            this.closeConnection = closeConnection;
            this.record = record;
            this.recordsAffected = this.record.RowCount;
        }

        public override bool GetBoolean(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetBoolean(ordinal);

        public override byte GetByte(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetByte(ordinal);

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);

        public override char GetChar(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetChar(ordinal);

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);

        public override string GetDataTypeName(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetDataTypeName(ordinal);

        public override DateTime GetDateTime(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetDateTime(ordinal);

        public override decimal GetDecimal(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetDecimal(ordinal);

        public override double GetDouble(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetDouble(ordinal);

        public override Type GetFieldType(int ordinal)
        => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetFieldType(ordinal);

        public override float GetFloat(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetFloat(ordinal);

        public override Guid GetGuid(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetGuid(ordinal);

        public override short GetInt16(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetInt16(ordinal);

        public override int GetInt32(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetInt32(ordinal);

        public override long GetInt64(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetInt64(ordinal);

        public override string GetName(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetName(ordinal);

        public override int GetOrdinal(string name)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetOrdinal(name);

        public override string GetString(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetString(ordinal);

        public override object GetValue(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetValue(ordinal);

        public override int GetValues(object[] values)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.GetValues(values);

        public override bool IsDBNull(int ordinal)
            => this.closed
                ? throw new InvalidOperationException(Resources.DataReaderClosed(nameof(GetFieldType)))
                : this.record == null
                    ? throw new InvalidOperationException(Resources.NoData)
                    : this.record.IsDBNull(ordinal);

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override bool NextResult()
        {
            return false;
        }

        public override bool Read()
        {
            if((this.record?.Read() ?? false)){
                return true;
            }
                
            if(this.record?.HasAdditionalRows ?? true)
            {
                this.record = this.connection.Connector.ReadResponse<DqliteDataRecord>(ResponseParsers.ParseDataRecordResponse);

                if(this.recordsAffected == -1)
                {
                    this.recordsAffected = this.record.RowCount;
                }
                else
                {
                    this.recordsAffected += this.record.RowCount;
                }
                
                return this.record?.Read() ?? false;
            }

            return false;
        }

        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            if((this.record?.Read() ?? false)){
                return true;
            }

            if(this.record?.HasAdditionalRows ?? true)
            {
                this.record = await this.connection.Connector.ReadResponseAsync<DqliteDataRecord>(ResponseParsers.ParseDataRecordResponse, cancellationToken);

                if(this.recordsAffected == -1)
                {
                    this.recordsAffected = this.record.RowCount;
                }
                else
                {
                    this.recordsAffected += this.record.RowCount;
                }
                
                return this.record?.Read() ?? false;
            }

            return false;
        }

        public override T GetFieldValue<T>(int ordinal)
            => this.record.GetFieldValue<T>(ordinal);

        public override void Close()
            => this.Dispose();

        public override Task CloseAsync()
            => this.DisposeAsync().AsTask();

        protected override void Dispose(bool disposing)
        {
            if(!disposing || this.closed){
                return;
            }

            this.command.DataReader = null;
            this.record = null;
            
            this.connection.Connector.InterruptStatement(this.connection.CurrentDatabase);
            this.closed = true;

            if(this.closeConnection)
            {
                this.command.Dispose();
            }
        }

        public override async ValueTask DisposeAsync()
        {
            if (this.closed)
            {
                return;
            }

            this.command.DataReader = null;
            this.record = null;

            await this.connection.Connector.InterruptStatementAsync(this.connection.CurrentDatabase, CancellationToken.None);
            this.closed = true;

            if(this.closeConnection)
            {
                await this.command.DisposeAsync();
            }
        }
    }
}
