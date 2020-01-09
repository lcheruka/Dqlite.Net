using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dqlite.Net.Properties;


namespace Dqlite.Net
{
    public class DqliteConnection : DbConnection
    {

        internal const string MainDatabaseName = "main"; 
        internal const int DefaultPort = 6543;

        private readonly List<WeakReference<DqliteCommand>> _commands = new List<WeakReference<DqliteCommand>>();

        private string _connectionString;
        private ConnectionState _state;
        private DqliteConnector connector;
        private DatabaseRecord database;

        /// <summary>
        ///     Initializes a new instance of the <see cref="DqliteConnection" /> class.
        /// </summary>
        public DqliteConnection()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="DqliteConnection" /> class.
        /// </summary>
        /// <param name="connectionString">The string used to open the connection.</param>
        /// <seealso cref="DqliteConnectionStringBuilder" />
        public DqliteConnection(string connectionString)
            => ConnectionString = connectionString;


        /// <summary>
        ///     Gets or sets a string used to open the connection.
        /// </summary>
        /// <value>A string used to open the connection.</value>
        /// <seealso cref="SqliteConnectionStringBuilder" />
        public override string ConnectionString
        {
            get => _connectionString;
            set
            {
                if (State != ConnectionState.Closed)
                {
                    throw new InvalidOperationException(Resources.ConnectionStringRequiresClosedConnection);
                }

                _connectionString = value;
                ConnectionOptions = new DqliteConnectionStringBuilder(value);
            }
        }

        internal DqliteConnector Connector => this.connector;
        internal DatabaseRecord CurrentDatabase => this.database;
        internal DqliteConnectionStringBuilder ConnectionOptions { get; set; }

        /// <summary>
        ///     Gets the name of the current database. Always 'main'.
        /// </summary>
        /// <value>The name of the current database.</value>
        public override string Database
            => MainDatabaseName;

        /// <summary>
        ///     Gets the path to the database file. Will be absolute for open connections.
        /// </summary>
        /// <value>The path to the database file.</value>
        public override string DataSource
        {
            get => ConnectionOptions.DataSource;
        }

        /// <summary>
        ///     Gets or sets the default <see cref="DqliteCommand.CommandTimeout" /> value for commands created using
        ///     this connection. This is also used for internal commands in methods like
        ///     <see cref="BeginTransaction()" />.
        /// </summary>
        /// <value>The default <see cref="DqliteCommand.CommandTimeout" /> value.</value>
        public virtual int DefaultTimeout { get; set; } = 30;

        /// <summary>
        ///     Gets the version of SQLite used by the connection.
        /// </summary>
        /// <value>The version of SQLite used by the connection.</value>
        public override string ServerVersion
            => string.Empty;
        
        /// <summary>
        ///     Gets the current state of the connection.
        /// </summary>
        /// <value>The current state of the connection.</value>
        public override ConnectionState State
            => _state;
        
        /// <summary>
        ///     Gets the <see cref="DbProviderFactory" /> for this connection.
        /// </summary>
        /// <value>The <see cref="DbProviderFactory" />.</value>
        protected override DbProviderFactory DbProviderFactory
            => DqliteFactory.Instance;
        
        /// <summary>
        ///     Gets or sets the transaction currently being used by the connection, or null if none.
        /// </summary>
        /// <value>The transaction currently being used by the connection.</value>
        protected internal virtual DqliteTransaction Transaction { get; set; }

        public override void Open()
        {
            if (State == ConnectionState.Open)
            {
                return;
            }

            if (ConnectionString == null)
            {
                throw new InvalidOperationException(Resources.OpenRequiresSetConnectionString);
            }

            if (this.ConnectionOptions.Nodes.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(ConnectionOptions.Nodes));
            }

            if (string.IsNullOrEmpty(ConnectionOptions.DataSource))
            {
                throw new ArgumentNullException(nameof(ConnectionOptions.DataSource));
            }

            _state = ConnectionState.Connecting;
            OnStateChange(new StateChangeEventArgs(ConnectionState.Closed, ConnectionState.Connecting));
            try
            {
                using(var cts = new CancellationTokenSource())
                {
                    cts.CancelAfter(this.ConnectionTimeout * 1000);
                    this.connector = new DqliteConnector(this.ConnectionOptions, false);
                    this.connector.Connect(cts.Token);
                    this.database = this.connector.OpenDatabase(this.ConnectionOptions.DataSource);
                }

                _state = ConnectionState.Open;
                OnStateChange(new StateChangeEventArgs(ConnectionState.Connecting, ConnectionState.Open));
            }
            catch
            {
                this.connector?.Dispose();
                this.connector = null;
                _state = ConnectionState.Closed;
                OnStateChange(new StateChangeEventArgs(ConnectionState.Connecting, ConnectionState.Closed));
            }
        }

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            if (State == ConnectionState.Open)
            {
                return;
            }

            if (ConnectionString == null)
            {
                throw new InvalidOperationException(Resources.OpenRequiresSetConnectionString);
            }

            if (this.ConnectionOptions.Nodes.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(ConnectionOptions.Nodes));
            }

            if (string.IsNullOrEmpty(ConnectionOptions.DataSource))
            {
                throw new ArgumentNullException(nameof(ConnectionOptions.DataSource));
            }

            _state = ConnectionState.Connecting;
            OnStateChange(new StateChangeEventArgs(ConnectionState.Closed, ConnectionState.Connecting));
            try
            {
                using(var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    cts.CancelAfter(this.ConnectionTimeout * 1000);
                    this.connector = new DqliteConnector(this.ConnectionOptions, false);
                    await this.connector.ConnectAsync(cts.Token);
                    this.database = await this.connector.OpenDatabaseAsync(this.ConnectionOptions.DataSource, cts.Token);
                }
                _state = ConnectionState.Open;
                OnStateChange(new StateChangeEventArgs(ConnectionState.Connecting, ConnectionState.Open));
            }
            catch
            {
                await this.connector.DisposeAsync();
                this.connector = null;
                _state = ConnectionState.Closed;
                OnStateChange(new StateChangeEventArgs(ConnectionState.Connecting, ConnectionState.Closed));
            }            
        }

        public override void Close()
        {
            if (State != ConnectionState.Open)
            {
                return;
            }

            Transaction?.Dispose();

            for (var i = _commands.Count - 1; i >= 0; i--)
            {
                var reference = _commands[i];
                if (reference.TryGetTarget(out var command))
                {
                    // NB: Calls RemoveCommand()
                    command.Dispose();
                }
                else
                {
                    _commands.RemoveAt(i);
                }
            }

            Debug.Assert(_commands.Count == 0);

            this.connector.Dispose();
            this.connector = null;

            _state = ConnectionState.Closed;
            OnStateChange(new StateChangeEventArgs(ConnectionState.Open, ConnectionState.Closed));
        }

        public override async Task CloseAsync()
        {
            if (State != ConnectionState.Open)
            {
                return;
            }

            if(this.Transaction != null)
            {
                await this.Transaction.DisposeAsync();
            }

            for (var i = _commands.Count - 1; i >= 0; i--)
            {
                var reference = _commands[i];
                if (reference.TryGetTarget(out var command))
                {
                    await command.DisposeAsync();
                }
                else
                {
                    _commands.RemoveAt(i);
                }
            }

            Debug.Assert(_commands.Count == 0);

            await this.connector.DisposeAsync();
            this.connector = null;

            _state = ConnectionState.Closed;
            OnStateChange(new StateChangeEventArgs(ConnectionState.Open, ConnectionState.Closed));
        }

        /// <summary>
        ///     Releases any resources used by the connection and closes it.
        /// </summary>
        /// <param name="disposing">
        ///     true to release managed and unmanaged resources; false to release only unmanaged resources.
        /// </param>
        protected override void Dispose(bool disposing)
        {
            if(disposing)
            {
                this.Close();
            }

            base.Dispose(disposing);
        }

        /// <summary>
        ///     Creates a new command associated with the connection.
        /// </summary>
        /// <returns>The new command.</returns>
        /// <remarks>
        ///     The command's <seealso cref="DqliteCommand.Transaction" /> property will also be set to the current
        ///     transaction.
        /// </remarks>
        public new virtual DqliteCommand CreateCommand()
            => new DqliteCommand()
            {
                Connection = this,
                CommandTimeout = DefaultTimeout,
                Transaction = Transaction
            };

        /// <summary>
        ///     Creates a new command associated with the connection.
        /// </summary>
        /// <returns>The new command.</returns>
        protected override DbCommand CreateDbCommand()
            => CreateCommand();

        internal void AddCommand(DqliteCommand command)
            => _commands.Add(new WeakReference<DqliteCommand>(command));
        
        internal void RemoveCommand(DqliteCommand command)
        {
            for (var i = _commands.Count - 1; i >= 0; i--)
            {
                if (_commands[i].TryGetTarget(out var item)
                    && item == command)
                {
                    _commands.RemoveAt(i);
                }
            }
        }

        /// <summary>
        ///     Begins a transaction on the connection.
        /// </summary>
        /// <returns>The transaction.</returns>
        public new virtual DqliteTransaction BeginTransaction()
            => BeginTransaction(IsolationLevel.Unspecified);

        /// <summary>
        ///     Begins a transaction on the connection.
        /// </summary>
        /// <param name="isolationLevel">The isolation level of the transaction.</param>
        /// <returns>The transaction.</returns>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => BeginTransaction(isolationLevel);

        /// <summary>
        ///     Begins a transaction on the connection.
        /// </summary>
        /// <param name="isolationLevel">The isolation level of the transaction.</param>
        /// <returns>The transaction.</returns>
        public new virtual DqliteTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            if (State != ConnectionState.Open)
            {
                throw new InvalidOperationException(Resources.CallRequiresOpenConnection(nameof(BeginTransaction)));
            }

            if (Transaction != null)
            {
                throw new InvalidOperationException(Resources.ParallelTransactionsNotSupported);
            }

            return Transaction = new DqliteTransaction(this, isolationLevel);
        }

        /// <summary>
        ///     Changes the current database. Not supported.
        /// </summary>
        /// <param name="databaseName">The name of the database to use.</param>
        /// <exception cref="NotSupportedException">Always.</exception>
        public override void ChangeDatabase(string databaseName)
            => throw new NotSupportedException();
    }
}