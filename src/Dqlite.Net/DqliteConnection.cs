using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace Dqlite.Net
{
    public class DqliteConnection : IDbConnection
    {
        public string ConnectionString
        {
            get => this.connectionString;
            set
            {
                if (this.State != ConnectionState.Closed)
                {
                    throw new InvalidOperationException();
                }

                this.connectionString = value;
                this.ConnectionOptions = new DqliteConnectionStringBuilder(value);
            }
        }

        public string Database => this.ConnectionOptions.Database;
        public int ConnectionTimeout => this.DefaultTimeout;
        public int DefaultTimeout { get; set; } = 30;

        public ConnectionState State { get; private set; }

        internal DatabaseRecord CurrentDatabase { get; private set; }
        internal DqliteClient Client { get; private set; }
        internal DqliteTransaction Transaction { get; set; }
        internal DqliteConnectionStringBuilder ConnectionOptions { get; set; }


        private string connectionString;
        private readonly IList<WeakReference<DqliteCommand>> commands;

        public DqliteConnection()
        {
            this.commands = new List<WeakReference<DqliteCommand>>();
        }

        public DqliteConnection(string connectionString) 
            : this()
        {
            this.ConnectionString = connectionString;
        }

        public IDbTransaction BeginTransaction()
        {
            return BeginTransaction(IsolationLevel.Unspecified);
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            if (this.State != ConnectionState.Open)
            {
                throw new InvalidOperationException();
            }
            else if (this.Transaction != null)
            {
                throw new InvalidOperationException();
            }

            return (this.Transaction =  new DqliteTransaction(this, il));
        }

        IDbCommand IDbConnection.CreateCommand() => CreateCommand();

        public DqliteCommand CreateCommand()
        {
            return new DqliteCommand() { Connection = this, CommandTimeout = this.DefaultTimeout, Transaction = this.Transaction };
        }

        public void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException();
        }

        public void Open()
        {
            if (this.State == ConnectionState.Open)
            {
                return;
            }
            else if (this.ConnectionString == null)
            {
                throw new InvalidOperationException();
            }
            else if (string.IsNullOrEmpty(ConnectionOptions.Host))
            {
                throw new ArgumentNullException(nameof(ConnectionOptions.Host));
            }
            else if (string.IsNullOrEmpty(ConnectionOptions.Database))
            {
                throw new ArgumentNullException(nameof(ConnectionOptions.Database));
            }

            this.State = ConnectionState.Connecting;

            try
            {
                if (ConnectionOptions.LeaderOnly)
                {
                    this.Client = DqliteClient.FindLeader(this.ConnectionOptions.Host, this.ConnectionOptions.Port);
                }
                else
                {
                    this.Client = new DqliteClient();
                    this.Client.Open(this.ConnectionOptions.Host, this.ConnectionOptions.Port);
                }

                this.CurrentDatabase = this.Client.OpenDatabase(this.ConnectionOptions.Database);
                this.State = ConnectionState.Open;
            }
            catch
            {
                this.State = ConnectionState.Closed;
                this.Client?.Dispose();
                throw;
            }
        }

        public void Close()
        {
            if (this.State != ConnectionState.Open)
            {
                return;
            }

            this.Transaction?.Dispose();

            foreach (var reference in this.commands)
            {
                if (reference.TryGetTarget(out var command))
                {
                    command.Dispose();
                }
            }

            this.commands.Clear();
            this.State = ConnectionState.Closed;

            this.Client?.Dispose();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    this.Close();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
