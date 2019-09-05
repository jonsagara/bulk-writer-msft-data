﻿using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Xunit;

namespace BulkWriter.Tests
{
    public class BulkWriterTests
    {
        private readonly string _connectionString = TestHelpers.ConnectionString;

        public class BulkWriterTestsMyTestClass
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }

        [Fact]
        public async Task CanWriteSync()
        {
            string tableName = DropCreate(nameof(BulkWriterTestsMyTestClass));

            var writer = new BulkWriter<BulkWriterTestsMyTestClass>(_connectionString);

            var items = Enumerable.Range(1, 1000).Select(i => new BulkWriterTestsMyTestClass { Id = i, Name = "Bob" });

            writer.WriteToDatabase(items);

            var count = (int)await TestHelpers.ExecuteScalar(_connectionString, $"SELECT COUNT(1) FROM {tableName}");

            Assert.Equal(1000, count);
        }

        [Fact]
        public async Task CanWriteSyncWithExistingConnection()
        {
            string tableName = DropCreate(nameof(BulkWriterTestsMyTestClass));

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                var writer = new BulkWriter<BulkWriterTestsMyTestClass>(connection);

                var items = Enumerable.Range(1, 1000)
                    .Select(i => new BulkWriterTestsMyTestClass { Id = i, Name = "Bob" });

                writer.WriteToDatabase(items);

                var count = (int)await TestHelpers.ExecuteScalar(connection, $"SELECT COUNT(1) FROM {tableName}");

                Assert.Equal(1000, count);
            }
        }

        [Fact]
        public async Task CanWriteSyncWithExistingConnectionAndTransaction()
        {
            string tableName = DropCreate(nameof(BulkWriterTestsMyTestClass));

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                using (var transaction = connection.BeginTransaction())
                {

                    var writer = new BulkWriter<BulkWriterTestsMyTestClass>(connection, transaction);

                    var items = Enumerable.Range(1, 1000)
                        .Select(i => new BulkWriterTestsMyTestClass { Id = i, Name = "Bob" });

                    writer.WriteToDatabase(items);

                    var count = (int)await TestHelpers.ExecuteScalar(connection, $"SELECT COUNT(1) FROM {tableName}", transaction);

                    Assert.Equal(1000, count);

                    transaction.Rollback();

                    count = (int)await TestHelpers.ExecuteScalar(connection, $"SELECT COUNT(1) FROM {tableName}");

                    Assert.Equal(0, count);
                }
            }
        }

        public class OrdinalAndColumnNameExampleType
        {
            [NotMapped]
            public string Dummy { get; set; }

            [Column(Order = 0)]
            public int Id { get; set; }

            [NotMapped]
            public string Name { get; set; }

            [Column("Name")]
            public string Name2 { get; set; }
        }

        [Fact]
        public async Task Should_Handle_Both_Ordinal_And_ColumnName_For_Destination_Mapping()
        {
            string tableName = DropCreate(nameof(OrdinalAndColumnNameExampleType));

            var writer = new BulkWriter<OrdinalAndColumnNameExampleType>(_connectionString);

            var items = new[] { new OrdinalAndColumnNameExampleType { Id = 1, Name2 = "Bob" } };

            writer.WriteToDatabase(items);

            var count = (int)await TestHelpers.ExecuteScalar(_connectionString, $"SELECT COUNT(1) FROM {tableName}");

            Assert.Equal(1, count);
        }

        public class MyTestClassForNvarCharMax
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public async Task Should_Handle_Column_Nvarchar_With_Length_Max()
        {
            string tableName = nameof(MyTestClassForNvarCharMax);
            TestHelpers.ExecuteNonQuery(_connectionString, $"DROP TABLE IF EXISTS [dbo].[{tableName}]");
            TestHelpers.ExecuteNonQuery(_connectionString,
                "CREATE TABLE [dbo].[" + tableName + "](" +
                "[Id] [int] IDENTITY(1,1) NOT NULL," +
                "[Name] [nvarchar](MAX) NULL," +
                "CONSTRAINT [PK_" + tableName + "] PRIMARY KEY CLUSTERED ([Id] ASC)" +
                ")");

            var writer = new BulkWriter<MyTestClassForNvarCharMax>(_connectionString);

            var items = new[] { new MyTestClassForNvarCharMax { Id = 1, Name = "Bob" } };

            writer.WriteToDatabase(items);

            var count = (int)await TestHelpers.ExecuteScalar(_connectionString, $"SELECT COUNT(1) FROM {tableName}");

            Assert.Equal(1, count);
        }

        public class MyTestClassForVarBinary
        {
            public int Id { get; set; }
            public byte[] Data { get; set; }
        }

        [Fact]
        public async Task Should_Handle_Column_VarBinary_Large()
        {
            string tableName = nameof(MyTestClassForVarBinary);

            TestHelpers.ExecuteNonQuery(_connectionString, $"DROP TABLE IF EXISTS [dbo].[{tableName}]");
            TestHelpers.ExecuteNonQuery(_connectionString,
                "CREATE TABLE [dbo].[" + tableName + "](" +
                "[Id] [int] IDENTITY(1,1) NOT NULL," +
                "[Data] [varbinary](MAX) NULL," +
                "CONSTRAINT [PK_" + tableName + "] PRIMARY KEY CLUSTERED ([Id] ASC)" +
                ")");

            var writer = new BulkWriter<MyTestClassForVarBinary>(_connectionString);
            var items = new[] { new MyTestClassForVarBinary { Id = 1, Data = new byte[1024 * 1024 * 1] } };
            new Random().NextBytes(items.First().Data);

            writer.WriteToDatabase(items);

            var count = (int)await TestHelpers.ExecuteScalar(_connectionString, $"SELECT COUNT(1) FROM {tableName}");
            var data = (byte[])await TestHelpers.ExecuteScalar(_connectionString, $"SELECT TOP 1 Data FROM {tableName}");
            Assert.Equal(items.First().Data, data);
            Assert.Equal(1, count);
        }

        private string DropCreate(string tableName)
        {
            TestHelpers.ExecuteNonQuery(_connectionString, $"DROP TABLE IF EXISTS [dbo].[{tableName}]");

            TestHelpers.ExecuteNonQuery(_connectionString,
                "CREATE TABLE [dbo].[" + tableName + "](" +
                "[Id] [int] IDENTITY(1,1) NOT NULL," +
                "[Name] [nvarchar](50) NULL," +
                "CONSTRAINT [PK_" + tableName + "] PRIMARY KEY CLUSTERED ([Id] ASC)" +
                ")");

            return tableName;
        }
    }
}