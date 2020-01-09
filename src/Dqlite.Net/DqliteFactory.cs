using System.Data.Common;

namespace Dqlite.Net
{
    /// <summary>
    ///     Creates instances of various Microsoft.Data.Sqlite classes.
    /// </summary>
    public class DqliteFactory : DbProviderFactory
    {
        private DqliteFactory()
        {
        }

        /// <summary>
        ///     The singleton instance.
        /// </summary>
        public static readonly DqliteFactory Instance = new DqliteFactory();

        /// <summary>
        ///     Creates a new command.
        /// </summary>
        /// <returns>The new command.</returns>
        public override DbCommand CreateCommand()
            => new DqliteCommand();

        /// <summary>
        ///     Creates a new connection.
        /// </summary>
        /// <returns>The new connection.</returns>
        public override DbConnection CreateConnection()
            => new DqliteConnection();

        /// <summary>
        ///     Creates a new connection string builder.
        /// </summary>
        /// <returns>The new connection string builder.</returns>
        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
            => new DqliteConnectionStringBuilder();

        /// <summary>
        ///     Creates a new parameter.
        /// </summary>
        /// <returns>The new parameter.</returns>
        public override DbParameter CreateParameter()
            => new DqliteParameter();
    }
}