using System;
using System.Data;
using System.Linq;

namespace Dqlite.Net
{
    public class DqliteCommand : IDbCommand
    {
        public string CommandText
        {
            get => this.commandText;
            set
            {
                if (this.DataReader != null)
                {
                    throw new InvalidOperationException("DataReader Already Open");
                }

                if (value != this.commandText)
                {
                    DisposePreparedStatement();
                    commandText = value;
                }
            }
        }

        public int CommandTimeout { get; set; }
        public CommandType CommandType
        {
            get => CommandType.Text;
            set
            {
                if (value != CommandType.Text)
                {
                    throw new ArgumentException(nameof(CommandText));
                }
            }
        }

        IDbConnection IDbCommand.Connection { get => this.Connection; set => this.Connection = (DqliteConnection)value; }
        IDataParameterCollection IDbCommand.Parameters { get => this.Parameters; }
        IDbTransaction IDbCommand.Transaction { get => this.Transaction; set => this.Transaction = (DqliteTransaction)value; }
        UpdateRowSource IDbCommand.UpdatedRowSource { get; set; }

        internal DqliteConnection Connection { get; set; }
        internal DqliteTransaction Transaction { get; set; }
        internal DqliteParameterCollection Parameters { get; private set; }
        internal DqliteDataReader DataReader { get; set; }

        private string commandText;
        private PreparedStatementRecord statement;

        public DqliteCommand()
        {
            this.Parameters = new DqliteParameterCollection();
        }

        public void Cancel()
        {
            
        }

        public IDbDataParameter CreateParameter()
        {
            return new DqliteParameter();
        }

        public void Dispose()
        {
            this.DisposePreparedStatement();
            this.Parameters.Clear();
        }

        public int ExecuteNonQuery()
        {
            if (Connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection isn't open");
            }

            if (CommandText == null)
            {
                throw new InvalidOperationException("CommandText is required");
            }

            if (this.DataReader != null)
            {
                throw new InvalidOperationException("DataReader already open");
            }

            if (this.statement != null)
            {
                var result =  Connection.Client.ExecuteStatement(this.statement, this.Parameters.ToArray());
                return (int)result.RowCount;
            }
            else
            {
                var result = Connection.Client.ExecuteNonQuery(this.Connection.CurrentDatabase, this.CommandText, this.Parameters.ToArray());
                return (int)result.RowCount;
            }
        }

        public IDataReader ExecuteReader()
        {
            return ExecuteReader(CommandBehavior.Default);
        }

        public IDataReader ExecuteReader(CommandBehavior behavior)
        {
            if ((behavior & ~(CommandBehavior.Default | CommandBehavior.SequentialAccess | CommandBehavior.SingleResult
                              | CommandBehavior.SingleRow | CommandBehavior.CloseConnection)) != 0)
            {
                throw new ArgumentException("Invalid Command Behavior");
            }
            
            if (this.DataReader != null)
            {
                throw new InvalidOperationException("DataReader already open");
            }
            
            if (this.Connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection isn't open");
            }
            
            if (this.CommandText == null)
            {
                throw new InvalidOperationException("CommandText is required");
            }
            
            if (this.Transaction != Connection.Transaction)
            {
                throw new InvalidOperationException(
                    Transaction == null
                        ? "Transaction Required"
                        : "Mismatch Transaction");
            }
            
            if (this.Connection.Transaction?.ExternalRollback == true)
            {
                throw new InvalidOperationException("Transaction Completed");
            }

            if (this.statement != null)
            {
                if(this.statement.ParameterCount != (ulong)this.Parameters.Count)
                {
                    throw new InvalidOperationException("Invalid number of parameters");
                }

                return new DqliteDataReader(this, statement, this.Parameters.ToArray(), behavior == CommandBehavior.CloseConnection);
            }
            return new DqliteDataReader(this, this.Connection.CurrentDatabase, this.CommandText, this.Parameters.ToArray(), behavior == CommandBehavior.CloseConnection);
        }

        public object ExecuteScalar()
        {
            if (this.Connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection isn't open");
            }

            if (this.CommandText == null)
            {
                throw new InvalidOperationException("CommandText is required");
            }

            using (var reader = ExecuteReader())
            {
                return reader.Read()
                    ? reader.GetValue(0)
                    : null;
            }
        }

        public void Prepare()
        {
            if(this.statement == null)
            {
                this.statement = this.Connection.Client.PrepareStatement(Connection.CurrentDatabase, CommandText);
            }
        }

        private void DisposePreparedStatement()
        {
            if (this.statement != null)
            {
                this.Connection.Client.FinalizeStatement(statement);
                this.statement = null;
            }
        }
    }
}
