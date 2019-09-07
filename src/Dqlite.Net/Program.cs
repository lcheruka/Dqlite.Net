using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Dqlite.Net
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            using (var node = DqliteNode.Create(1, "127.0.0.1:5000", "/data"))
            {
                node.Start();

                using (var connection = new DqliteConnection("Host=127.0.0.1;Port=5000;Database=main"))
                {
                    connection.Open();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = "CREATE TABLE IF NOT EXISTS test (n INT)";
                        command.ExecuteNonQuery();

                        command.CommandText = "DELETE FROM test";
                        command.ExecuteNonQuery();

                        command.CommandText = "INSERT INTO test(n) VALUES(?)";
                        command.Parameters.AddWithValue(0);

                        for (int i = 0; i < 100; i++)
                        {
                            command.Parameters[0].Value = i;
                            var result = command.ExecuteNonQuery();
                        }
                        command.Parameters.Clear();

                        command.CommandText = "SELECT count(0) FROM test";
                        var count = (long)command.ExecuteScalar();
                        Console.WriteLine(count);
                    }
                }
            }
        }
    }
}