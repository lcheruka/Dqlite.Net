using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dqlite.Net.Properties;

namespace Dqlite.Net
{
    public class DqliteCommand : DbCommand
    {
        private readonly Lazy<DqliteParameterCollection> _parameters = new Lazy<DqliteParameterCollection>(
            () => new DqliteParameterCollection());
        private DqliteConnection _connection;
        private PreparedStatement statement;
        private string _commandText;
        private string _internalCommandText;
        private string[] _parameterNames;
        private bool _prepared;

        
        public DqliteCommand()
        {

        }

        public DqliteCommand(string commandText)
        {
            this.CommandText = commandText;
        }

        public DqliteCommand(string commandText, DqliteConnection connection)
            : this(commandText)
        {
            this.Connection = connection;
            this.CommandTimeout = connection.DefaultTimeout;
        }

        public DqliteCommand(string commandText, DqliteConnection connection, DqliteTransaction transaction)
            : this(commandText, connection)
        {
            this.Transaction = transaction;
        }

        public override CommandType CommandType
        {
            get => CommandType.Text;
            set
            {
                if (value != CommandType.Text)
                {
                    throw new ArgumentException(Resources.InvalidCommandType(value));
                }
            }
        }

        public override string CommandText
        {
            get => _commandText;
            set
            {
                if (DataReader != null)
                {
                    throw new InvalidOperationException(Resources.SetRequiresNoOpenReader(nameof(CommandText)));
                }

                if (value != _commandText)
                {
                    DisposePreparedStatement();
                    _commandText = value;
                    _internalCommandText = Utils.ParseSql(value, out _parameterNames);
                }
            }
        }

        public new virtual DqliteConnection Connection
        {
            get => _connection;
            set
            {
                if (DataReader != null)
                {
                    throw new InvalidOperationException(Resources.SetRequiresNoOpenReader(nameof(Connection)));
                }

                if (value != _connection)
                {
                    DisposePreparedStatement();

                    _connection?.RemoveCommand(this);
                    _connection = value;
                    value?.AddCommand(this);
                }
            }
        }

        protected override DbConnection DbConnection
        {
            get => Connection;
            set => Connection = (DqliteConnection)value;
        }

        public new virtual DqliteTransaction Transaction { get; set; }

        protected override DbTransaction DbTransaction
        {
            get => Transaction;
            set => Transaction = (DqliteTransaction)value;
        }

        public new virtual DqliteParameterCollection Parameters
            => _parameters.Value;

        protected override DbParameterCollection DbParameterCollection
            => Parameters;

        public override int CommandTimeout { get; set; } = 30;
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected internal virtual DqliteDataReader DataReader { get; set; }

        protected override void Dispose(bool disposing)
        {
            DisposePreparedStatement(disposing);

            if (disposing)
            {
                _connection?.RemoveCommand(this);
            }

            base.Dispose(disposing);
        }

        public new virtual DqliteParameter CreateParameter()
            => new DqliteParameter();
        protected override DbParameter CreateDbParameter()
            => CreateParameter();

        public override void Prepare()
            => this.PrepareAsync().GetAwaiter().GetResult();

        public override async Task PrepareAsync(CancellationToken cancellationToken = default(System.Threading.CancellationToken))
        {
            if (_connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException(Resources.CallRequiresOpenConnection(nameof(Prepare)));
            }

            if (string.IsNullOrEmpty(_commandText))
            {
                throw new InvalidOperationException(Resources.CallRequiresSetCommandText(nameof(Prepare)));
            }

            if (_prepared)
            {
                return;
            }
            
            if(this.statement == null)
            {
                this.statement = await this.Connection.Connector.PrepareStatementAsync(Connection.CurrentDatabase, CommandText, cancellationToken);
                _prepared = true;
            }
        }

        public new virtual DqliteDataReader ExecuteReader()
            => ExecuteReader(CommandBehavior.Default);

        public new virtual DqliteDataReader ExecuteReader(CommandBehavior behavior)
        {
            CheckState();

            var closeConnection = behavior.HasFlag(CommandBehavior.CloseConnection);
            var parameters = this.Parameters.Bind(_parameterNames);
            var record = (this.statement != null 
                    ? this.Connection.Connector.ExecuteQuery(this.statement, parameters)
                    : this.Connection.Connector.ExecuteQuery(this.Connection.CurrentDatabase, this.CommandText, parameters));

            return DataReader = new DqliteDataReader(this, record, closeConnection);
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
            => ExecuteReader(behavior);

        public new virtual Task<DqliteDataReader> ExecuteReaderAsync()
            => ExecuteReaderAsync(CommandBehavior.Default, CancellationToken.None);
        public new virtual Task<DqliteDataReader> ExecuteReaderAsync(CancellationToken cancellationToken)
            => ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);
        public new virtual Task<DqliteDataReader> ExecuteReaderAsync(CommandBehavior behavior)
            => ExecuteReaderAsync(behavior, CancellationToken.None);
        public new virtual async Task<DqliteDataReader> ExecuteReaderAsync(
            CommandBehavior behavior,
            CancellationToken cancellationToken)
        {
            
            cancellationToken.ThrowIfCancellationRequested();

            CheckState();

            var closeConnection = behavior.HasFlag(CommandBehavior.CloseConnection);
            var parameters = this.Parameters.Bind(_parameterNames);
            var record = await (this.statement != null 
                    ? this.Connection.Connector.ExecuteQueryAsync(this.statement, parameters, cancellationToken)
                    : this.Connection.Connector.ExecuteQueryAsync(this.Connection.CurrentDatabase, this.CommandText, parameters, cancellationToken));

            return DataReader = new DqliteDataReader(this, record, closeConnection);
        }

        public override int ExecuteNonQuery()
        {
            CheckState();
            var parameters = this.Parameters.Bind(_parameterNames);
            var result = this.Connection.Connector.ExecuteNonQuery(this.Connection.CurrentDatabase, this.CommandText, parameters);
            return (int)result.RowCount;
        }

        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            CheckState();
            var parameters = this.Parameters.Bind(_parameterNames);
            var result = await this.Connection.Connector.ExecuteNonQueryAsync(this.Connection.CurrentDatabase, this.CommandText, parameters, cancellationToken);
            return (int)result.RowCount;
        }

        public override object ExecuteScalar()
        {
            CheckState();
            
            using(var reader = ExecuteReader())
            {
                return reader.Read() ? reader.GetValue(0) : null;
            }
        }

        public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            CheckState();

            await using(var reader = await ExecuteReaderAsync())
            {
                return await reader.ReadAsync() ? reader.GetValue(0) : null;
            }
        }

        private void CheckState()
        {
            if (this.DataReader != null)
            {
                throw new InvalidOperationException(Resources.DataReaderOpen);
            }

            if (this.Connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException(Resources.CallRequiresOpenConnection(nameof(ExecuteReader)));
            }

            if (string.IsNullOrEmpty(this.CommandText))
            {
                throw new InvalidOperationException(Resources.CallRequiresSetCommandText(nameof(ExecuteReader)));
            }

            if (this.Transaction != this.Connection.Transaction)
            {
                throw new InvalidOperationException(
                    this.Transaction == null
                        ? Resources.TransactionRequired
                        : Resources.TransactionConnectionMismatch);
            }
        }

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
            CommandBehavior behavior,
            CancellationToken cancellationToken)
            => await ExecuteReaderAsync(behavior, cancellationToken);

        public override void Cancel()
        {
            
        }

        private void DisposePreparedStatement(bool disposing = true)
        {
            if (disposing && this.DataReader != null)
            {
                this.DataReader.Dispose();
                this.DataReader = null;
            }

            if (this.statement != null)
            {
                this.Connection.Connector.FinalizeStatement(statement);
                this.statement = null;
            }

            _prepared = false;
        }

        private async Task DisposePreparedStatementAsync(bool disposing = true)
        {
            if (disposing && this.DataReader != null)
            {
                this.DataReader.Dispose();
                this.DataReader = null;
            }

            if (this.statement != null)
            {
                await this.Connection.Connector.FinalizeStatementAsync(statement, CancellationToken.None);
                this.statement = null;
            }

            _prepared = false;
        }
    }
}
