﻿using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace BulkWriter.Tests
{
    internal static class TestHelpers
    {
        static TestHelpers()
        {
            ConnectionString = @"Data Source=(localdb)\mssqllocaldb;Database=BulkWriter.Tests;Trusted_Connection=True;";

            string admin = @"Data Source=(localdb)\mssqllocaldb;Trusted_Connection=True;";
            ExecuteNonQuery(admin, @"IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'BulkWriter.Tests')
CREATE DATABASE [BulkWriter.Tests]");
        }


        public static void ExecuteNonQuery(string connectionString, string commandText)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand(commandText, sqlConnection))
                {
                    sqlConnection.Open();
                    command.ExecuteNonQuery();
                }
            }
        }

        public static async Task<object> ExecuteScalar(string connectionString, string commandText)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                await sqlConnection.OpenAsync();
                return await ExecuteScalar(sqlConnection, commandText);
            }
        }

        public static async Task<object> ExecuteScalar(SqlConnection sqlConnection, string commandText, SqlTransaction transaction = null)
        {
            using (var command = new SqlCommand(commandText, sqlConnection, transaction))
            {
                return await command.ExecuteScalarAsync();
            }
        }

        public static string ConnectionString { get; }
    }
}