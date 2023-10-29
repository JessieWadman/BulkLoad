using System.Linq.Expressions;
using BulkLoad.EntityFrameworkCore.Abstractions.Helpers;
using BulkLoad.EntityFrameworkCore.Abstractions.Implementation;
using BulkLoad.EntityFrameworkCore.Abstractions.Models;
using BulkLoad.EntityFrameworkCore.Abstractions.Options;

namespace BulkLoad.EntityFrameworkCore.Abstractions;

internal sealed class FluentApi<TEntity, TProvider>(TProvider provider) : 
    IMergeStatementConfiguration<TEntity>,
    IBulkLoadAndMergeOptions<TEntity>,
    IBulkLoadSourceConfiguration<TEntity>
    where TProvider : AbstractBulkLoadAndMerge<TEntity>
    where TEntity : class
{
    public IBulkLoadAndMergeOptions<TEntity> WithAllColumnComparison()
    {
        provider.Options.ComparisonMethod = ComparisonMethod.ColumnByColumn;
        provider.Options.RowHashProperty = null;
        provider.Options.RowHashColumn = null;
        return this;
    }

    public IBulkLoadAndMergeOptions<TEntity> WithRowHashComparison(Expression<Func<TEntity, long>> propertySelector)
    {
        var propertyName = ExpressionHelper.GetPropertyName(propertySelector);
        var columnName = provider.Metadata.GetColumnName(propertyName);
        provider.Options.RowHashProperty = propertyName;
        provider.Options.RowHashColumn = columnName;
        provider.Options.ComparisonMethod = ComparisonMethod.RowHash;
        return this;
    }
    
    public IBulkLoadAndMergeOptions<TEntity> WithRowHashComparison(Expression<Func<TEntity, Guid>> propertySelector)
    {
        var propertyName = ExpressionHelper.GetPropertyName(propertySelector);
        var columnName = provider.Metadata.GetColumnName(propertyName);
        provider.Options.RowHashProperty = propertyName;
        provider.Options.RowHashColumn = columnName;
        provider.Options.ComparisonMethod = ComparisonMethod.RowHash;
        return this;
    }
    
    public IBulkLoadAndMergeOptions<TEntity> WithNoComparison()
    {
        provider.Options.RowHashProperty = null;
        provider.Options.RowHashColumn = null;
        provider.Options.ComparisonMethod = ComparisonMethod.NoComparison;
        return this;
    }

    public IBulkLoadAndMergeOptions<TEntity> ExcludePropertyFromComparison<TProperty>(Expression<Func<TEntity, TProperty>> propertySelector)
    {
        var propertyName = ExpressionHelper.GetPropertyName(propertySelector);
        provider.Options.PropertiesToExcludeFromComparison.Add(propertyName);
        return this;
    }

    public IBulkLoadAndMergeOptions<TEntity> ExcludeColumnFromUpdate<TProperty>(Expression<Func<TEntity, TProperty>> propertySelector)
    {
        var propertyName = ExpressionHelper.GetPropertyName(propertySelector);
        var columnName = provider.Metadata.GetColumnName(propertyName);
        provider.Options.ColumnToNotUpdate.Add(columnName);
        return this;
    }

    public IBulkLoadAndMergeOptions<TEntity> OnUpdateRecordSetExpression<TProperty>(Expression<Func<TEntity, TProperty>> propertySelector, string expression)
    {
        var propertyName = ExpressionHelper.GetPropertyName(propertySelector);
        var columnName = provider.Metadata.GetColumnName(propertyName);
        provider.Options.OnUpdateExpressions[columnName] = expression;
        return this;
    }

    public IBulkLoadAndMergeOptions<TEntity> WithSoftDeletion(Expression<Func<TEntity, bool>> propertySelector)
    {
        var propertyName = ExpressionHelper.GetPropertyName(propertySelector);
        var columnName = provider.Metadata.GetColumnName(propertyName);
        provider.Options.SoftDeleteColumn = columnName;
        return this;
    }

    public IBulkLoadAndMergeOptions<TEntity> WithBatchSize(int batchSize)
    {
        provider.Options.BatchSize = batchSize;
        return this;
    }
    
    public IMergeStatementConfiguration<TEntity> FromSnapshot(IAsyncEnumerable<TEntity> source)
    {
        provider.Options.Kind = BulkInsertKind.Snapshot;
        provider.Options.Source = source;
        return this;
    }

    public IMergeStatementConfiguration<TEntity> FromIncrementalChanges(IAsyncEnumerable<TEntity> source)
    {
        provider.Options.Kind = BulkInsertKind.IncrementalChanges;
        provider.Options.Source = source;
        return this;
    }

    public IAsyncEnumerable<BulkInsertRecord<TEntity>> ExecuteAndGetChangesAsync(CancellationToken cancellationToken)
    {
        return provider.ExecuteAndGetChangesAsync(cancellationToken);
    }
    
    public Task<int> ExecuteAsync(CancellationToken cancellationToken)
        => provider.ExecuteAsync(cancellationToken);
}

public interface IBulkLoadSourceConfiguration<TEntity> where TEntity : class
{
    /// <summary>
    /// Replace all existing records with the incoming ones. Honors soft deletes, if specified.
    /// </summary>
    /// <param name="source">Records to load into the table</param>
    IMergeStatementConfiguration<TEntity> FromSnapshot(IAsyncEnumerable<TEntity> source);
    
    /// <summary>
    /// Updates a subset of existing records, but does not remove/soft delete any records.
    /// </summary>
    /// <param name="source">Records to load into the table</param>
    /// <returns></returns>
    IMergeStatementConfiguration<TEntity> FromIncrementalChanges(IAsyncEnumerable<TEntity> source);
}

public interface IMergeStatementConfiguration<TEntity> where TEntity : class
{
    /// <summary>
    /// (Slow) Uses column-by-column comparison between incoming records and existing records to identify changes
    /// during the MERGE statement.
    /// </summary>
    /// <remarks>
    /// Required when underlying schema cannot be changed. Do consider using RowHash strategy instead, if possible. 
    /// </remarks>
    IBulkLoadAndMergeOptions<TEntity> WithAllColumnComparison();

    /// <summary>
    /// Will not compare non-primary key fields of existing records to reduce number of records merged, but will
    /// instead always update existing records. Useful if you know you will always be updating records that already
    /// exists on each merge.
    /// </summary>
    IBulkLoadAndMergeOptions<TEntity> WithNoComparison();
    
    /// <summary>
    /// (Faster) Hashes each record using XxHash64 before loading it, and compares the hashes of incoming records against hashes on
    /// existing records to identify changes during the MERGE statement.
    /// </summary>
    /// <param name="propertySelector">The 64 bit int column that will hold the row hash.</param>
    IBulkLoadAndMergeOptions<TEntity> WithRowHashComparison(Expression<Func<TEntity, long>> propertySelector);
    
    /// <summary>
    /// (Faster) Hashes each record using XxHash128 before loading it, and compares the hashes of incoming records against hashes on
    /// existing records to identify changes during the MERGE statement.
    /// </summary>
    /// <param name="propertySelector">The 128 bit Guid column that will hold the hash.</param>
    IBulkLoadAndMergeOptions<TEntity> WithRowHashComparison(Expression<Func<TEntity, Guid>> propertySelector);
}

public interface IBulkLoadAndMergeOptions<TEntity> : IBulkLoadAndMerge<TEntity> where TEntity : class
{
    /// <summary>
    /// When comparing incoming records and existing records, do not include this column when checking if the two
    /// records are identical, or have changed.
    /// </summary>
    /// <remarks>
    /// If using row hashes, this column will not be included in the hashing. Therefore, changes to this column will
    /// not trigger a merge. This is useful if the property is not part of the incoming data (incoming data will have
    /// a default value for it), but rather set afterwards, by some other mechanism. If an update happens on the record
    /// anyway, because of other differences, this column will be updated also. The difference between
    /// ExcludePropertyFromComparison and ExcludeColumnFromUpdate, is that ExcludeColumnFromUpdate will not update the
    /// field, even if an update does happen, while ExcludePropertyFromComparison will.
    /// </remarks>
    /// <param name="propertySelector">The property to exclude from record comparison</param>
    /// <typeparam name="TProperty">The property type</typeparam>
    /// <returns></returns>
    IBulkLoadAndMergeOptions<TEntity> ExcludePropertyFromComparison<TProperty>(Expression<Func<TEntity, TProperty>> propertySelector);
    
    /// <summary>
    /// When updating an existing record, don't change this column in the target table. If the record does not exist,
    /// then the value will be inserted.
    /// </summary>
    /// <remarks>
    /// This is useful for columns such as "LoadedDate", which will only be updated if the record is actually merged.
    /// Existing records without changes to them will not be affected. The difference between
    /// ExcludePropertyFromComparison and ExcludeColumnFromUpdate, is that ExcludeColumnFromUpdate will not update the
    /// field, even if an update does happen, while ExcludePropertyFromComparison will.
    /// </remarks>
    /// <param name="propertySelector">Property to not change when updating an existing record</param>
    /// <typeparam name="TProperty">Property type</typeparam>
    /// <returns></returns>
    IBulkLoadAndMergeOptions<TEntity> ExcludeColumnFromUpdate<TProperty>(Expression<Func<TEntity, TProperty>> propertySelector);
    
    /// <summary>
    /// When updating or inserting a record, set the specified property to the given expression.
    /// </summary>
    /// <param name="propertySelector">Property to set value for</param>
    /// <param name="expression">Database expression to evaluate</param>
    /// <typeparam name="TProperty">Property type</typeparam>
    /// <returns></returns>
    IBulkLoadAndMergeOptions<TEntity> OnUpdateRecordSetExpression<TProperty>(Expression<Func<TEntity, TProperty>> propertySelector, string expression);

    /// <summary>
    /// Indicates that when doing snapshot merges, the bulk insertion should soft delete records instead of hard deleting them.
    /// </summary>
    /// <param name="propertySelector">Property that indicates that the record is soft deleted</param>
    /// <returns></returns>
    IBulkLoadAndMergeOptions<TEntity> WithSoftDeletion(Expression<Func<TEntity, bool>> propertySelector);
    
    /// <summary>
    /// Indicates batch size when loading records into database
    /// </summary>
    /// <param name="batchSize">Number of records in each batch during loading</param>
    /// <returns></returns>
    IBulkLoadAndMergeOptions<TEntity> WithBatchSize(int batchSize);
}