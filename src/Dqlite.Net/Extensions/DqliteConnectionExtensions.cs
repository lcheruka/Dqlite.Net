using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dqlite.Net
{
    public static class DqliteConnectionExtensions
    {
        public static int ExecuteNonQuery(
            this DqliteConnection connection,
            string commandText,
            params DqliteParameter[] parameters)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = commandText;
                command.Parameters.AddRange(parameters);

                return command.ExecuteNonQuery();
            }
        }

        public static T ExecuteScalar<T>(
            this DqliteConnection connection,
            string commandText,
            params DqliteParameter[] parameters)
            => (T)connection.ExecuteScalar(commandText, parameters);

        private static object ExecuteScalar(
            this DqliteConnection connection,
            string commandText,
            params DqliteParameter[] parameters)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = commandText;
                command.Parameters.AddRange(parameters);

                return command.ExecuteScalar();
            }
        }
    }
}
