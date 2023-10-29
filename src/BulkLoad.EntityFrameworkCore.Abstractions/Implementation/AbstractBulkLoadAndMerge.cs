using System.Data;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using BulkLoad.EntityFrameworkCore.Abstractions.Exceptions;
using BulkLoad.EntityFrameworkCore.Abstractions.Hashing;
using BulkLoad.EntityFrameworkCore.Abstractions.Helpers;
using BulkLoad.EntityFrameworkCore.Abstractions.Models;
using BulkLoad.EntityFrameworkCore.Abstractions.Options;
using Microsoft.EntityFrameworkCore;

namespace BulkLoad.EntityFrameworkCore.Abstractions.Implementation;

public abstract class AbstractBulkLoadAndMerge<TEntity>(DbContext dbContext) : IBulkLoadAndMerge<TEntity>
    where TEntity : class
{
    public readonly BulkInsertOptions<TEntity> Options = new();
    protected DbContext DbContext { get; } = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
    public EntityMetadata Metadata { get; } = dbContext.GetEntityMetadata<TEntity>();

    protected int RecordsAffected = 0;

    private async IAsyncEnumerable<BulkInsertRecord<TEntity>> InternalExecuteAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await OpenDatabaseConnection(cancellationToken);
        await using var transaction = await DbContext.Database.BeginTransactionAsync(cancellationToken);
        
        var batchTable = await CreateStagingTableAsync(cancellationToken);
        try
        {
            PropertyInfo? rowHashProp = null;
            DataColumn? rowHashColumn = null;
            var sysColumns = new[] { "__idx", "__action" };
            var columnsToHash = batchTable.Columns
                .ToArray() // <- Create a copy
                .Where(col => !sysColumns.Contains(col.ColumnName))
                .Take(batchTable.Columns.Count - 2) // Skip __action and __idx
                .ToList();

            if (Options.SoftDeleteColumn != null)
                columnsToHash.Remove(columnsToHash.Single(c => c.ColumnName == Options.SoftDeleteColumn));
        
            if (Options.RowHashProperty != null && Options.RowHashColumn != null)
            {
                rowHashProp = typeof(TEntity).GetProperty(Options.RowHashProperty);
                rowHashColumn = batchTable.Columns[Options.RowHashColumn]!;
                columnsToHash.Remove(rowHashColumn);
            
                var columnsToExclude = Options.PropertiesToExcludeFromComparison
                    .Select(prop => Metadata.GetColumnName(prop))
                    .ToArray();

                columnsToHash.RemoveAll(c => columnsToExclude.Contains(c.ColumnName));
            }
            
            var properties = Metadata.Properties
                .Where(p => !p.IsDbGenerated)
                .Select(p => typeof(TEntity).GetProperty(p.PropertyName)!)
                .ToList();

            var batchOffset = 0;
            var entityOffset = 0;
            
            await foreach (var entity in Options.Source.WithCancellation(cancellationToken))
            {
                try
                {
                    ConvertToDataRow(entity, batchTable, entityOffset++, properties, rowHashColumn, rowHashProp,
                        columnsToHash);
                    
                    if (batchTable.Rows.Count < Options.BatchSize)
                        continue;

                    await CopyRecordsIntoStagingTableAsync(batchTable, batchOffset, cancellationToken);

                    if (Options.Kind == BulkInsertKind.IncrementalChanges)
                    {
                        await MergeTempTableIntoStorageTableAsync(batchOffset, cancellationToken);
                    }

                    batchOffset += batchTable.Rows.Count;
                    batchTable.Clear();
                }
                catch (Exception error)
                {
                    throw new BulkLoadAndMergeException(error.Message, error);
                }
            }

            try
            {
                if (batchTable.Rows.Count > 0)
                {
                    await CopyRecordsIntoStagingTableAsync(batchTable, batchOffset, cancellationToken);
                }

                if (Options.Kind == BulkInsertKind.Snapshot)
                {
                    await IdentifyDeletionsAsync(cancellationToken);
                    await HandleDeletionsAsync(cancellationToken);
                }

                if (Options.Kind == BulkInsertKind.Snapshot || batchTable.Rows.Count > 0)
                {
                    if (Options.Kind == BulkInsertKind.Snapshot)
                        batchOffset = 0;
                    
                    await MergeTempTableIntoStorageTableAsync(batchOffset, cancellationToken);
                }
            }
            catch (Exception error)
            {
                throw new BulkLoadAndMergeException(error.Message, error);
            }

            if (Options.TrackChanges)
            {
                IAsyncEnumerable<BulkInsertRecord<TEntity>> changes;
                try
                {
                    changes = EnumerateChangesAsync(cancellationToken);
                }
                catch (Exception error)
                {
                    throw new BulkLoadAndMergeException(error.Message, error);
                }

                await foreach (var change in changes.WithCancellation(cancellationToken))
                    yield return change;
            }

            try
            {
                await DbContext.Database.CommitTransactionAsync(cancellationToken);
            }
            catch (Exception error)
            {
                throw new BulkLoadAndMergeException(error.Message, error);
            }
        }
        finally
        {
            try
            {
                // Ensure cleanup, so we dont pass cancellation token.
                await DropTempTableAsync(CancellationToken.None);
            }
            catch
            {
                // ignored
            }
        }
    }
    
    private static void ConvertToDataRow(
        TEntity entity, 
        DataTable stagingTable, 
        int offset, 
        IReadOnlyList<PropertyInfo> properties,
        DataColumn? rowHashColumn,
        PropertyInfo? rowHashProp,
        IReadOnlyList<DataColumn> columnsToHash)
    {
        var record = new object?[properties.Count + 2];
        for (var j = 0; j < properties.Count; j++)
        {
            var property = properties[j];
            record[j] = GetPropertyValueOrDbNull(property.GetValue(entity, null));
        }

        record[^2] = offset;
        record[^1] = "A";

        var row = stagingTable.Rows.Add(record);
        if (rowHashColumn != null && rowHashProp != null && columnsToHash.Count > 0)
        {
            if (rowHashProp.PropertyType == typeof(Guid))
            {
                var rowHash = RowHasher.ComputeRowHashAsGuid(row, columnsToHash);
                row[rowHashColumn] = rowHash;
            }
            else if (rowHashProp.PropertyType == typeof(long))
            {
                var rowHash = RowHasher.ComputeRowHashAsBigInt(row, columnsToHash);
                row[rowHashColumn] = rowHash;
            }
        }
    }

    public async Task<int> ExecuteAsync(CancellationToken cancellationToken)
    {
        Options.TrackChanges = false;
        await foreach (var _ in InternalExecuteAsync(cancellationToken))
        {
        }

        return RecordsAffected;
    }

    public IAsyncEnumerable<BulkInsertRecord<TEntity>> ExecuteAndGetChangesAsync(CancellationToken cancellationToken)
    {
        Options.TrackChanges = true;
        return InternalExecuteAsync(cancellationToken);
    }

    protected abstract Task<DataTable> CreateStagingTableAsync(CancellationToken cancellationToken);
    protected abstract ValueTask CopyRecordsIntoStagingTableAsync(DataTable batch, int offset,
        CancellationToken cancellationToken);
    protected abstract Task ClearTempTableAsync(CancellationToken cancellationToken);
    protected abstract Task MergeTempTableIntoStorageTableAsync(int startIndex, CancellationToken cancellationToken);
    protected abstract Task IdentifyDeletionsAsync(CancellationToken cancellationToken);
    protected abstract Task HandleDeletionsAsync(CancellationToken cancellationToken);
    protected abstract IAsyncEnumerable<BulkInsertRecord<TEntity>> EnumerateChangesAsync(
        CancellationToken cancellationToken);
    protected abstract Task DropTempTableAsync(CancellationToken cancellationToken);
    protected abstract string FormatIdentifier(string identifier);

    protected virtual string FormatIdentifier(string? qualifier, string identifier)
    {
        if (string.IsNullOrWhiteSpace(qualifier))
            return FormatIdentifier(identifier);
        return qualifier + "." + FormatIdentifier(identifier);
    }

    protected virtual string GetPrimaryKeyComparison(string? sourceQualifier, string? targetQualifier)
    {
        var clauses = Metadata.PrimaryKeyColumns.Select(col =>
            $"{FormatIdentifier(sourceQualifier, col)} = {FormatIdentifier(targetQualifier, col)}");
        return string.Join(" AND ", clauses);
    }
    
    protected virtual string GetColumnsEqualsClause(string? sourceQualifier, string? targetQualifier)
    {
        var nonKeyProps = Metadata.Properties
            .Where(p => !Metadata.PrimaryKeyColumns.Contains(p.ColumnName))
            .Select(p => p.ColumnName)
            .ToArray();
        
        var clauses = nonKeyProps.Select(col =>
            $"{FormatIdentifier(sourceQualifier, col)} = {FormatIdentifier(targetQualifier, col)}");
        return string.Join(" AND ", clauses);
    }
    
    protected virtual string GetColumnsNotEqualsClause(string? sourceQualifier, string? targetQualifier)
    {
        var nonKeyProps = Metadata.Properties
            .Where(p => !Metadata.PrimaryKeyColumns.Contains(p.ColumnName))
            .Select(p => p.ColumnName)
            .ToArray();
        
        var clauses = nonKeyProps.Select(col =>
            $"{FormatIdentifier(sourceQualifier, col)} <> {FormatIdentifier(targetQualifier, col)}");
        return string.Join(" OR ", clauses);
    }

    protected virtual string GetRowHashColumn()
    {
        if (Options.RowHashProperty == null)
            throw new InvalidOperationException("RowHash column not set!");

        return Metadata.Properties.Single(m => m.PropertyName == Options.RowHashProperty).ColumnName;
    }
    
    protected virtual string GetRowHashEqualsClause(string? sourceQualifier, string? targetQualifier)
    {
        var rowHashColumn = GetRowHashColumn();
        return
            $"{FormatIdentifier(sourceQualifier, rowHashColumn)} = {FormatIdentifier(targetQualifier, rowHashColumn)}";
    }
    
    protected virtual string GetRowHashNotEqualsClause(string? sourceQualifier, string? targetQualifier)
    {
        var rowHashColumn = GetRowHashColumn();
        return
            $"{FormatIdentifier(sourceQualifier, rowHashColumn)} <> {FormatIdentifier(targetQualifier, rowHashColumn)}";
    }

    protected virtual string GetRecordsAreEqualClause(string? sourceQualifier, string? targetQualifier)
        => Options.ComparisonMethod switch
        {
            ComparisonMethod.NoComparison => "1=2",
            ComparisonMethod.RowHash => GetRowHashEqualsClause(sourceQualifier, targetQualifier),
            ComparisonMethod.ColumnByColumn => GetColumnsEqualsClause(sourceQualifier, targetQualifier)
        };
    
    protected virtual string GetRecordsAreNotEqualClause(string? sourceQualifier, string? targetQualifier)
        => Options.RowHashProperty != null
            ? GetRowHashNotEqualsClause(sourceQualifier, targetQualifier)
            : GetColumnsNotEqualsClause(sourceQualifier, targetQualifier);
    
    private async Task OpenDatabaseConnection(CancellationToken cancellationToken)
    {
        try
        {
            await DbContext.Database.OpenConnectionAsync(cancellationToken);
        }
        catch (Exception)
        {
            // ignored	
        }
    }

    private static object GetPropertyValueOrDbNull(object? @object)
    {
        return @object ?? DBNull.Value;
    }
}