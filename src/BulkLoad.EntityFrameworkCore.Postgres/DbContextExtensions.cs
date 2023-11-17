using BulkLoad.EntityFrameworkCore.Abstractions;
using BulkLoad.EntityFrameworkCore.Postgres;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// Extension methods for bulk loading and merging data into Postgres
/// </summary>
public static class DbContextExtensions
{
    /// <summary>
    /// Load data from a local source into a temporary database table, and then merge it into a physical database table
    /// </summary>
    /// <param name="dbContext">EF Core context</param>
    /// <typeparam name="TEntity">Type of entity to load</typeparam>
    /// <returns>Returns a fluent API for configuring the operation</returns>
    public static IBulkLoadSourceConfiguration<TEntity> PostgresLoadAndMerge<TEntity>(this DbContext dbContext)
        where TEntity : class
        => new FluentApi<TEntity, PostgresBulkLoadAndMerge<TEntity>>(new PostgresBulkLoadAndMerge<TEntity>(dbContext));
}