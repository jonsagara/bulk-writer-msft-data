using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using BulkWriter.Internal;
using Microsoft.Data.SqlClient;

namespace BulkWriter
{
    // 2019-09-04 jonsagara:
    //   * Added ctor(SqlConnection, SqlBulkCopyOptions, SqlTransaction) overload to allow the caller to specify options.
    //   * Changed signature of Initialize to accept SqlBulkCopyOptions from the caller, and to use the caller's options
    //     if non-null instead of computing options.
    public class BulkWriter<TResult> : IBulkWriter<TResult>
    {
        private readonly SqlBulkCopy _sqlBulkCopy;
        private readonly IEnumerable<PropertyMapping> _propertyMappings;

        public BulkWriter(string connectionString)
        {
            _propertyMappings = typeof(TResult).BuildMappings();
            _sqlBulkCopy = Initialize(options => new SqlBulkCopy(connectionString, options), callerOptions: null);
        }

        public BulkWriter(SqlConnection connection, SqlTransaction transaction = null)
        {
            _propertyMappings = typeof(TResult).BuildMappings();
            _sqlBulkCopy = Initialize(options => new SqlBulkCopy(connection, options, transaction), callerOptions: null);
        }

        public BulkWriter(SqlConnection connection, SqlBulkCopyOptions callerOptions, SqlTransaction transaction)
        {
            _propertyMappings = typeof(TResult).BuildMappings();
            _sqlBulkCopy = Initialize(options => new SqlBulkCopy(connection, options, transaction), callerOptions);
        }

        private SqlBulkCopy Initialize(Func<SqlBulkCopyOptions, SqlBulkCopy> createBulkCopy, SqlBulkCopyOptions? callerOptions)
        {
            var hasAnyKeys = _propertyMappings.Any(x => x.Destination.IsKey);

            // If the caller provided options, use those. Otherwise, compute them.
            var sqlBulkCopyOptions = callerOptions != null
                ? callerOptions.Value
                : (hasAnyKeys ? SqlBulkCopyOptions.KeepIdentity : SqlBulkCopyOptions.Default) | SqlBulkCopyOptions.TableLock;

            var tableAttribute = typeof(TResult).GetTypeInfo().GetCustomAttribute<TableAttribute>();
            var schemaName = tableAttribute?.Schema;
            var tableName = tableAttribute?.Name ?? typeof(TResult).Name;
            var destinationTableName = schemaName != null ? $"{schemaName}.{tableName}" : tableName;

            var sqlBulkCopy = createBulkCopy(sqlBulkCopyOptions);

            sqlBulkCopy.DestinationTableName = destinationTableName;
            sqlBulkCopy.EnableStreaming = true;
            sqlBulkCopy.BulkCopyTimeout = 0;

            foreach (var propertyMapping in _propertyMappings.Where(propertyMapping => propertyMapping.ShouldMap))
            {
                sqlBulkCopy.ColumnMappings.Add(propertyMapping.ToColumnMapping());
            }

            return sqlBulkCopy;
        }

        public int BatchSize
        {
            get => _sqlBulkCopy.BatchSize;
            set => _sqlBulkCopy.BatchSize = value;
        }

        public int BulkCopyTimeout
        {
            get => _sqlBulkCopy.BulkCopyTimeout;
            set => _sqlBulkCopy.BulkCopyTimeout = value;
        }

        public Action<SqlBulkCopy> BulkCopySetup { get; set; } = sbc => { };

        public void WriteToDatabase(IEnumerable<TResult> items)
        {
            BulkCopySetup(_sqlBulkCopy);

            using (var dataReader = new EnumerableDataReader<TResult>(items, _propertyMappings))
            {
                _sqlBulkCopy.WriteToServer(dataReader);
            }
        }

        public async Task WriteToDatabaseAsync(IEnumerable<TResult> items)
        {
            BulkCopySetup(_sqlBulkCopy);

            using (var dataReader = new EnumerableDataReader<TResult>(items, _propertyMappings))
            {
                await _sqlBulkCopy.WriteToServerAsync(dataReader);
            }
        }

        public void Dispose() => ((IDisposable)_sqlBulkCopy).Dispose();
    }
}