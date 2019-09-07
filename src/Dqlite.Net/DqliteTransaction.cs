using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Dqlite.Net
{
    public class DqliteTransaction : IDbTransaction
    {
        public IDbConnection Connection => this.connection;
        public IsolationLevel IsolationLevel { get; }

        internal bool ExternalRollback { get; private set; }

        private DqliteConnection connection;
        private bool completed;

        public DqliteTransaction(DqliteConnection connection, IsolationLevel isolationLevel)
        {
            if (isolationLevel == IsolationLevel.ReadUncommitted
                || isolationLevel == IsolationLevel.ReadCommitted
                || isolationLevel == IsolationLevel.RepeatableRead)
            {
                isolationLevel = IsolationLevel.Serializable;
            }

            this.connection = connection;
            this.IsolationLevel = isolationLevel;

            if (isolationLevel == IsolationLevel.ReadUncommitted)
            {
                this.connection.ExecuteNonQuery("PRAGMA read_uncommitted = 1;");
            }
            else if (isolationLevel == IsolationLevel.Serializable)
            {
                this.connection.ExecuteNonQuery("PRAGMA read_uncommitted = 0;");
            }
            else if (isolationLevel != IsolationLevel.Unspecified)
            {
                throw new ArgumentException();
            }

            this.connection.ExecuteNonQuery(
                IsolationLevel == IsolationLevel.Serializable
                    ? "BEGIN IMMEDIATE;"
                    : "BEGIN;");
        }

        public void Commit()
        {
            if (ExternalRollback
                || this.completed
                || this.connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException();
            }

            this.connection.ExecuteNonQuery("COMMIT;");
            Complete();
        }

        public void Rollback()
        {
            if (this.completed || this.connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException();
            }

            RollbackInternal();
        }

        public void Dispose()
        {
            if (!this.completed && this.connection.State == ConnectionState.Open)
            {
                RollbackInternal();
            }
        }

        private void RollbackInternal()
        {
            if (!ExternalRollback)
            {
                this.connection.ExecuteNonQuery("ROLLBACK;");
            }

            Complete();
        }

        private void Complete()
        {
            this.connection.Transaction = null;
            this.connection = null;
            this.completed = true;
        }
    }
}
