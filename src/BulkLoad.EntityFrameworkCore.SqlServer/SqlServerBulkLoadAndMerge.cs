using System.Data;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using BulkLoad.EntityFrameworkCore.Abstractions.Implementation;
using BulkLoad.EntityFrameworkCore.Abstractions.Models;
using BulkLoad.EntityFrameworkCore.Abstractions.Options;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace BulkLoad.EntityFrameworkCore.SqlServer;

public class SqlServerBulkLoadAndMerge<TEntity>(DbContext dbContext) 
    : AbstractBulkLoadAndMerge<TEntity>(dbContext)
    where TEntity : class
{
    private readonly string stagingTableName = "##BULK_" + Guid.NewGuid().ToString().Replace('-', '_');
    private readonly string deletionsTableName = "##BULK_" + Guid.NewGuid().ToString().Replace('-', '_') + "_DELETIONS";
    
    private SqlConnection Connection => (SqlConnection)DbContext.Database.GetDbConnection();
    private SqlTransaction? Transaction => DbContext.Database.CurrentTransaction?.GetDbTransaction() as SqlTransaction;

    protected override string FormatIdentifier(string identifier)
    {
        return "[" + identifier + "]";
    }

    protected override string CurrentTimestampExpression => "sysutcdatetime()";
    protected override string FalseValue => "0";
    protected override string TrueValue => "1";

    private string QualifiedTableName
    {
        get
        {
            if (string.IsNullOrWhiteSpace(Metadata.TableSchema))
                return FormatIdentifier(Metadata.TableName);
            return FormatIdentifier(Metadata.TableName) + "." + FormatIdentifier(Metadata.TableSchema);
        }
    }

    protected override async Task<DataTable> CreateStagingTableAsync(CancellationToken cancellationToken)
    {
        DataTable dataTable = new();
        List<string> columns = new();

        foreach (var property in Metadata.Properties.Where(x => !x.IsDbGenerated))
        {
            columns.Add($"{FormatIdentifier(property.ColumnName)} {property.SqlServerType}");
            var propertyInfo = typeof(TEntity).GetProperty(property.PropertyName);
            if (propertyInfo == null)
                continue;

            DataColumn dataColumn = new(property.ColumnName)
            {
                DataType = Nullable.GetUnderlyingType(propertyInfo.PropertyType) ?? propertyInfo.PropertyType,
                AllowDBNull = property.IsNullable,
                ColumnName = property.ColumnName
            };

            dataTable.Columns.Add(dataColumn);
        }

        if (columns.Count == 0)
            throw new InvalidOperationException("Table doesn't contain any columns!");
        
        dataTable.Columns.Add("__idx", typeof(long));
        columns.Add("__idx bigint");
        
        dataTable.Columns.Add("__action", typeof(string)).MaxLength = 1;
        columns.Add("__action char(1)");

        var createStagingTableQuery = $"CREATE TABLE {FormatIdentifier(stagingTableName)} ({string.Join(",", columns)})";
        await DbContext.Database.ExecuteSqlRawAsync(createStagingTableQuery, cancellationToken);
        
        return dataTable;
    }
    
    protected override async ValueTask CopyRecordsIntoStagingTableAsync(DataTable batch, int offset, CancellationToken cancellationToken)
    {
        var sqlBulkCopy = new SqlBulkCopy(Connection, SqlBulkCopyOptions.Default, Transaction)
        {
            DestinationTableName = stagingTableName
        };
        await sqlBulkCopy.WriteToServerAsync(batch, cancellationToken);
    }

    protected override Task ClearTempTableAsync(CancellationToken cancellationToken)
    {
        return DbContext.Database.ExecuteSqlAsync($"TRUNCATE TABLE {FormatIdentifier(stagingTableName)}", cancellationToken);
    }

    private string IdentifierList(IEnumerable<string> values)
        => string.Join(", ", values.Select(FormatIdentifier));

    protected override async Task IdentifyDeletionsAsync(CancellationToken cancellationToken)
    {
        var formattedTempTableName = FormatIdentifier(stagingTableName);

        var softDeletionComparison = string.Empty;
        if (Options.SoftDeleteColumn != null)
            softDeletionComparison = IsNotSoftDeleted("storageTable") + " AND ";
        
        // Step 3: Identify records that will be deleted
        var queryText = $"""
             SELECT storageTable.* INTO {FormatIdentifier(deletionsTableName)}
             FROM {QualifiedTableName} storageTable
             WHERE {softDeletionComparison}
                 NOT EXISTS (SELECT 1 FROM {formattedTempTableName} tempTable WHERE {GetPrimaryKeyComparison("tempTable", "storageTable")})
            """;

        var deletionCount = await DbContext.Database.ExecuteSqlRawAsync(queryText, cancellationToken);
        Debug.WriteLine(deletionCount);
    }
    
    protected override async Task HandleDeletionsAsync(CancellationToken cancellationToken)
    {
        if (Options.SoftDeleteColumn != null)
        {
            var queryText = $"""
                             UPDATE storageTable SET
                               {FormatIdentifier(Options.SoftDeleteColumn)} = {SetSoftDeleted}
                             FROM {QualifiedTableName} storageTable
                             INNER JOIN {FormatIdentifier(deletionsTableName)} deletions ON {GetPrimaryKeyComparison("deletions", "storageTable")}
                             WHERE {IsNotSoftDeleted("storageTable")}
                             """;
            var rowsSoftDeleted = await DbContext.Database.ExecuteSqlRawAsync(queryText, cancellationToken);
            RecordsAffected += rowsSoftDeleted;
        }
        else
        {
            var queryText = $"""
                             DELETE tgt FROM {QualifiedTableName} tgt
                             WHERE EXISTS (SELECT 1 FROM {FormatIdentifier(deletionsTableName)} src WHERE {GetPrimaryKeyComparison("src", "tgt")}
                             """;
            var rowsDeleted = await DbContext.Database.ExecuteSqlRawAsync(queryText, cancellationToken);
            RecordsAffected += rowsDeleted;
        }
    }

    private async Task ReduceStagingTableAsync(CancellationToken cancellationToken)
    {
        var formattedTempTableName = FormatIdentifier(stagingTableName);
        
        var queryText = @$"
            DELETE FROM {formattedTempTableName} tempTable
            INNER JOIN {QualifiedTableName} AS storageTable ON {GetPrimaryKeyComparison("tempTable", "storageTable")}
            WHERE {GetRecordsAreEqualClause("tempTable", "storageTable")}";

        await DbContext.Database.ExecuteSqlRawAsync(queryText, cancellationToken);
    }
    
    protected override async Task MergeTempTableIntoStorageTableAsync(
        int tempTableOffset, 
        CancellationToken cancellationToken)
    {
        var formattedTempTableName = FormatIdentifier(stagingTableName);

        var recordIsNotSoftDeleted = string.Empty;
        if (Options.SoftDeleteColumn != null)
            recordIsNotSoftDeleted = " AND " + IsNotSoftDeleted("storageTable");

        string queryText;
        
        if (Options.ComparisonMethod != ComparisonMethod.NoComparison)
        {
            // Flag no-ops
            queryText = $"""
                                 UPDATE {formattedTempTableName}
                                     SET [__action] = 'N'
                                 FROM {formattedTempTableName}
                                 INNER JOIN {QualifiedTableName} AS storageTable ON {GetPrimaryKeyComparison(formattedTempTableName, "storageTable")}
                                 WHERE {formattedTempTableName}.__idx >= {tempTableOffset} AND
                                       ({GetRecordsAreEqualClause("storageTable", formattedTempTableName)} {recordIsNotSoftDeleted})
                             """;

            var noOpCount = await dbContext.Database.ExecuteSqlRawAsync(queryText, cancellationToken);
        }

        // Flag updates
        queryText = @$"
            UPDATE {formattedTempTableName}
                SET [__action] = 'U'
            FROM {formattedTempTableName}
            INNER JOIN {QualifiedTableName} AS storageTable ON {GetPrimaryKeyComparison(formattedTempTableName, "storageTable")}
            WHERE {formattedTempTableName}.__idx >= {tempTableOffset} AND {formattedTempTableName}.__action <> 'N'";

        var updateCount = await dbContext.Database.ExecuteSqlRawAsync(queryText, cancellationToken);

        var insertValues =
            Metadata.Properties
                .Where(x => !x.IsDbGenerated)
                .Select(x => x.ColumnName)
                .ToDictionary(x => x, x => "src." + FormatIdentifier(x));

        if (Options.SoftDeleteColumn != null)
            insertValues[Options.SoftDeleteColumn] = SetNotSoftDeleted;

        foreach (var kp in Options.OnUpdateExpressions)
            insertValues[kp.Key] = kp.Value;

        var updateValues = insertValues
            .Where(kp => !Options.ColumnToNotUpdate.Contains(kp.Key))
            .ToDictionary(kp => kp.Key, kp => kp.Value);

        var insertClause = "(" + string.Join(", ", insertValues.Keys.Select(FormatIdentifier)) + ") VALUES (" +
                           string.Join(", ", insertValues.Values) + ")";

        var updateClause = string.Join(", ", updateValues.Select(kp => $"{FormatIdentifier(kp.Key)} = {kp.Value}"));
        
        // Step 4: Merge changes into storage table
        queryText = $"""
                                 MERGE INTO {QualifiedTableName} AS tgt
                                 USING (SELECT * FROM {formattedTempTableName} WHERE __idx >= {tempTableOffset} AND __action <> 'N') AS src
                                 ON {GetPrimaryKeyComparison("src", "tgt")}
                                 WHEN NOT MATCHED THEN INSERT {insertClause}
                                 WHEN MATCHED THEN UPDATE SET
                                     {updateClause};
                     """;

        var changeCount = await DbContext.Database.ExecuteSqlRawAsync(queryText, cancellationToken);
        RecordsAffected += changeCount;
    }

    protected override async IAsyncEnumerable<BulkInsertRecord<TEntity>> EnumerateChangesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var formattedTempTableName = FormatIdentifier(stagingTableName);

        var additionsQuery = 
            $"""
                        SELECT src.*
                        FROM {QualifiedTableName} src
                        INNER JOIN {formattedTempTableName} tgt ON {GetPrimaryKeyComparison("src", "tgt")}
                        WHERE tgt.__action = 'A'
                        ORDER BY {string.Join(", ", Metadata.PrimaryKeyColumns.Select(FormatIdentifier))}
             """;

        var additions = DbContext
            .Set<TEntity>()
            .FromSqlRaw(additionsQuery)
            .AsAsyncEnumerable()
            .WithCancellation(cancellationToken);

        await foreach (var addition in additions)
        {
            yield return new BulkInsertRecord<TEntity>(BulkInsertAction.Added, addition);
        }

        var updatesQuery = 
            $"""
                 SELECT src.*
                 FROM {QualifiedTableName} src
                 INNER JOIN {formattedTempTableName} tgt ON {GetPrimaryKeyComparison("src", "tgt")}
                 WHERE tgt.__action = 'U'
                 ORDER BY {string.Join(", ", Metadata.PrimaryKeyColumns.Select(FormatIdentifier))}
             """;

        var updates = DbContext
            .Set<TEntity>()
            .FromSqlRaw(updatesQuery)
            .AsAsyncEnumerable()
            .WithCancellation(cancellationToken);

        await foreach (var update in updates)
        {
            yield return new BulkInsertRecord<TEntity>(BulkInsertAction.Updated, update);
        }
        
        if (Options.Kind == BulkInsertKind.IncrementalChanges)
            yield break;

        var removalsQuery = $"SELECT src.* FROM {FormatIdentifier(deletionsTableName)} src";

        var removals = DbContext
            .Set<TEntity>()
            .FromSqlRaw(removalsQuery)
            .AsAsyncEnumerable()
            .WithCancellation(cancellationToken);

        await foreach (var removed in removals)
        {
            yield return new BulkInsertRecord<TEntity>(BulkInsertAction.Removed, removed);
        }
    }

    protected override async Task DropTempTableAsync(CancellationToken cancellationToken)
    {
        var deleteCommand = $"DROP TABLE IF EXISTS {FormatIdentifier(stagingTableName)}";
        await DbContext.Database.ExecuteSqlRawAsync(deleteCommand, cancellationToken: cancellationToken);
        
        deleteCommand = $"DROP TABLE IF EXISTS {FormatIdentifier(deletionsTableName)}";
        await DbContext.Database.ExecuteSqlRawAsync(deleteCommand, cancellationToken: cancellationToken);
    }
}