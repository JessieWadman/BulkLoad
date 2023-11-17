using BulkLoad.EntityFrameworkCore.SqlServer;
using BulkLoad.EntityFrameworkCore.Abstractions;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// Bulk load extension
/// </summary>
public static class DbContextExtensions
{
    /// <summary>
    /// Bulk load data from local source into a temporary database table, and then merge it into a physical database table.
    /// </summary>
    /// <param name="dbContext">EF context to use</param>
    /// <typeparam name="TEntity">Type of entity to load</typeparam>
    /// <returns>A fluent API to configure the operation</returns>
    public static IBulkLoadSourceConfiguration<TEntity> SqlServerLoadAndMerge<TEntity>(this DbContext dbContext)
        where TEntity : class
        => new FluentApi<TEntity, SqlServerBulkLoadAndMerge<TEntity>>(new SqlServerBulkLoadAndMerge<TEntity>(dbContext));
}