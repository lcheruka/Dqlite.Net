using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Dqlite.Net.Properties;

namespace Dqlite.Net
{
    public class DqliteTransaction : DbTransaction
    {
        public new virtual DqliteConnection Connection
            => _connection;
         protected override DbConnection DbConnection
            => Connection;
            
        public override IsolationLevel IsolationLevel
            => _completed || _connection.State != ConnectionState.Open
                ? throw new InvalidOperationException(Resources.TransactionCompleted)
                : _isolationLevel != IsolationLevel.Unspecified
                    ? _isolationLevel
                    : IsolationLevel.Serializable;

        private DqliteConnection _connection;
        private readonly IsolationLevel _isolationLevel;
        private bool _completed;

        public DqliteTransaction(DqliteConnection connection, IsolationLevel isolationLevel)
        {
            if (isolationLevel == IsolationLevel.ReadUncommitted
                || isolationLevel == IsolationLevel.ReadCommitted
                || isolationLevel == IsolationLevel.RepeatableRead)
            {
                isolationLevel = IsolationLevel.Serializable;
            }

            _connection = connection;
            _isolationLevel = isolationLevel;

            if (isolationLevel == IsolationLevel.ReadUncommitted)
            {
                _connection.Connector.ExecuteNonQuery(_connection.CurrentDatabase, "PRAGMA read_uncommitted = 1;", new DqliteParameter[]{});
            }
            else if (isolationLevel == IsolationLevel.Serializable)
            {
                _connection.Connector.ExecuteNonQuery(_connection.CurrentDatabase, "PRAGMA read_uncommitted = 0;", new DqliteParameter[]{});
            }
            else if (isolationLevel != IsolationLevel.Unspecified)
            {
                throw new ArgumentException(Resources.InvalidIsolationLevel(isolationLevel));
            }

            _connection.Connector.ExecuteNonQuery(_connection.CurrentDatabase, 
                IsolationLevel == IsolationLevel.Serializable
                    ? "BEGIN IMMEDIATE;"
                    : "BEGIN;", 
                new DqliteParameter[]{});
        }

        public override void Commit()
        {
            if ( _completed
                || _connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException(Resources.TransactionCompleted);
            }

            _connection.Connector.ExecuteNonQuery(_connection.CurrentDatabase, "COMMIT;", new DqliteParameter[]{});
            Complete();
        }

        public override async Task CommitAsync(CancellationToken cancellationToken = default(System.Threading.CancellationToken))
        {
            if (_completed || _connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException(Resources.TransactionCompleted);
            }

            await _connection.Connector.ExecuteNonQueryAsync(_connection.CurrentDatabase, "COMMIT;", new DqliteParameter[]{}, cancellationToken);
            Complete();
        }

        public override void Rollback()
        {
            if (_completed || _connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException(Resources.TransactionCompleted);
            }

            _connection.Connector.ExecuteNonQuery(_connection.CurrentDatabase, "ROLLBACK;", new DqliteParameter[]{});
            Complete();
        }

        public override async Task RollbackAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_completed || _connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException(Resources.TransactionCompleted);
            }

            await _connection.Connector.ExecuteNonQueryAsync(_connection.CurrentDatabase, "ROLLBACK;", new DqliteParameter[]{}, cancellationToken);
            Complete();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing
                && !_completed
                && _connection.State == ConnectionState.Open)
            {
                Rollback();
            }
        }

        public override async ValueTask DisposeAsync()
        {
            if (!_completed
                && _connection.State == ConnectionState.Open)
            {
                await RollbackAsync();
            }
        }

        private void Complete()
        {
            _connection.Transaction = null;
            _connection = null;
            _completed = true;
        }
    }
}
