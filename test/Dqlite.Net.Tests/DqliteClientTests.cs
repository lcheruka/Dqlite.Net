using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Dqlite.Net
{
    public class DqliteClientTests : IDisposable
    {
        private readonly DqliteNode node;
        private readonly string dataDir;

        public DqliteClientTests(){
            this.dataDir = Directory.CreateDirectory(
                Path.Combine(Path.GetTempPath(), "dqlite_tests_"+Guid.NewGuid())).FullName;
            this.node = DqliteNode.Create(1, "127.0.0.1:6543", this.dataDir);
            this.node.Start();
        }
        
        [Theory]
        [InlineData(0)]
        [InlineData(10)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Test1(int rows)
        {
            var builder = new DqliteConnectionStringBuilder()
            {
                Nodes = new []{"127.0.0.1:6543"},
                DataSource = "main"
            };
            using(var connection = new DqliteConnection(builder.ToString()))
            {
                connection.Open();
                using(var command = new DqliteCommand(){Connection = connection})
                {
                    command.CommandText = "CREATE TABLE IF NOT EXISTS Sample(n INT);";
                    command.ExecuteNonQuery();

                    command.CommandText = "DELETE FROM Sample;";
                    command.ExecuteNonQuery();

                    command.CommandText = "INSERT INTO Sample(n) VALUES(?);";
                    command.Prepare();

                    for(int i = 0; i < rows; ++i)
                    {
                        command.Parameters.Add(new DqliteParameter(i));
                        command.ExecuteNonQuery();
                        command.Parameters.Clear();
                    }

                    command.CommandText = "SELECT count(0) FROM Sample;";

                    using(var reader = command.ExecuteReader())
                    {
                        Assert.True(reader.Read());
                        Assert.Equal(1, reader.FieldCount);
                        Assert.Equal(rows, reader.GetInt64(0));
                        Assert.False(reader.Read());
                    }

                    command.CommandText = "SELECT n FROM Sample;";

                    using(var reader = command.ExecuteReader())
                    {
                        for(int i = 0; i < rows; ++i)
                        {
                            Assert.True(reader.Read());
                            Assert.Equal(i, reader.GetInt64(0));
                        }
                        Assert.False(reader.Read());
                    }
                }
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(10)]
        [InlineData(100)]
        [InlineData(1000)]
        public async Task Test1Async(int rows)
        {
            var builder = new DqliteConnectionStringBuilder()
            {
                Nodes = new []{"127.0.0.1:6543"},
                DataSource = "main"
            };
            using(var connection = new DqliteConnection(builder.ToString()))
            {
                await connection.OpenAsync();
                using(var command = new DqliteCommand(){Connection = connection})
                {
                    command.CommandText = "CREATE TABLE IF NOT EXISTS Sample(n INT);";
                    await command.ExecuteNonQueryAsync();

                    command.CommandText = "DELETE FROM Sample;";
                    await command.ExecuteNonQueryAsync();

                    command.CommandText = "INSERT INTO Sample(n) VALUES(?);";
                    await command.PrepareAsync();

                    for(int i = 0; i < rows; ++i)
                    {
                        command.Parameters.Add(new DqliteParameter(i));
                        await command.ExecuteNonQueryAsync();
                        command.Parameters.Clear();
                    }

                    command.CommandText = "SELECT count(0) FROM Sample;";

                    await using(var reader = await command.ExecuteReaderAsync())
                    {
                        Assert.True(await reader.ReadAsync());
                        Assert.Equal(1, reader.FieldCount);
                        Assert.Equal(rows, reader.GetInt64(0));
                        Assert.False(await reader.ReadAsync());
                    }

                    command.CommandText = "SELECT n FROM Sample;";

                    await using(var reader = await command.ExecuteReaderAsync())
                    {
                        for(int i = 0; i < rows; ++i)
                        {
                            Assert.True(await reader.ReadAsync());
                            Assert.Equal(i, reader.GetInt64(0));
                        }
                        Assert.False(await reader.ReadAsync());
                    }
                }
            }
        }
        
        public void Dispose()
        {
            this.node.Dispose();
            Directory.Delete(this.dataDir, true);
        }
    }
}
