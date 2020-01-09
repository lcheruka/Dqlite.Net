using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dqlite.Net
{
    internal static class DqliteConnectorExtensions
    {
        private static readonly DqliteParameter[] PARAMETER_EMPTY = new DqliteParameter[0]; 
        public static Task<StatementResult> ExecuteNonQueryAsync(this DqliteConnector connector, DatabaseRecord database, string commandText, CancellationToken cancellationToken = default(CancellationToken)) 
            => connector.ExecuteNonQueryAsync(database, commandText, PARAMETER_EMPTY, cancellationToken);

        public static Task<StatementResult> ExecuteNonQueryAsync(this DqliteConnector connector, PreparedStatement preparedStatement, CancellationToken cancellationToken = default(CancellationToken)) 
            => connector.ExecuteNonQueryAsync(preparedStatement, PARAMETER_EMPTY, cancellationToken);
        
        public static Task<DqliteDataRecord> ExecuteQueryAsync(this DqliteConnector connector, DatabaseRecord database, string commandText, CancellationToken cancellationToken = default(CancellationToken)) 
            => connector.ExecuteQueryAsync(database, commandText, PARAMETER_EMPTY, cancellationToken);
        
        public static Task<DqliteDataRecord> ExecuteQueryAsync(this DqliteConnector connector, PreparedStatement preparedStatement, CancellationToken cancellationToken = default(CancellationToken)) 
            => connector.ExecuteQueryAsync(preparedStatement, PARAMETER_EMPTY, cancellationToken);

        
        public static StatementResult ExecuteNonQuery(this DqliteConnector connector, DatabaseRecord database, string commandText) 
            => connector.ExecuteNonQuery(database, commandText, PARAMETER_EMPTY);

        public static StatementResult ExecuteNonQuery(this DqliteConnector connector, PreparedStatement preparedStatement) 
            => connector.ExecuteNonQuery( preparedStatement, PARAMETER_EMPTY);
        
        public static DqliteDataRecord ExecuteQuery(this DqliteConnector connector, DatabaseRecord database, string commandText) 
            => connector.ExecuteQuery(database, commandText, PARAMETER_EMPTY);
        
        public static DqliteDataRecord ExecuteQuery(this DqliteConnector connector, PreparedStatement preparedStatement) 
            => connector.ExecuteQuery(preparedStatement, PARAMETER_EMPTY);
        
    }
}