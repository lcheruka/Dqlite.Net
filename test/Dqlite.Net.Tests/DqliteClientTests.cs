using System;
using System.IO;
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
            this.node = DqliteNode.Create(1, "127.0.0.1:5000", this.dataDir);
            this.node.Start();
        }
        
        [Theory]
        [InlineData(0)]
        [InlineData(10)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Test1(int rows)
        {
            using(var client = new DqliteClient())
            {
                client.Open("127.0.0.1", 5000);
                var database = client.OpenDatabase("main");
                
                client.ExecuteNonQuery(database, "CREATE TABLE IF NOT EXISTS Sample(n INT)");
                client.ExecuteNonQuery(database,  "DELETE FROM Sample");

                var statement = client.PrepareStatement(database, "INSERT INTO Sample(n) VALUES(?)");
                
                for(int i = 0; i < rows; ++i)
                {
                    client.ExecuteStatement(statement, new DqliteParameter(i));
                }

                client.FinalizeStatement(statement);

                var countRecord = client.ExecuteQuery(database, "SELECT count(0) FROM Sample");
                Assert.True(countRecord.Read());
                Assert.Equal(1, countRecord.FieldCount);
                Assert.Equal(rows, countRecord.GetInt64(0));
                Assert.False(countRecord.Read());
                var record = client.ExecuteQuery(database, "SELECT n FROM Sample");

                for(int i = 0; i < rows; ++i)
                {
                    Assert.True(record.Read());
                    Assert.Equal(i, record.GetInt64(0));
                }
                Assert.False(record.Read());
            }
        }
        
        public void Dispose()
        {
            this.node.Dispose();
            Directory.Delete(this.dataDir, true);
        }
    }
}
