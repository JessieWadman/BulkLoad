using BulkInserter.SqlServer;
using BulkLoad.EntityFrameworkCore.Abstractions;

namespace Microsoft.EntityFrameworkCore;

public static class DbContextExtensions
{
    public static IBulkLoadSourceConfiguration<TEntity> PostgresLoadAndMerge<TEntity>(this DbContext dbContext)
        where TEntity : class
        => new FluentApi<TEntity, PostgresBulkLoadAndMerge<TEntity>>(new PostgresBulkLoadAndMerge<TEntity>(dbContext));
}