using BulkLoad.EntityFrameworkCore.SqlServer;
using BulkLoad.EntityFrameworkCore.Abstractions;

namespace Microsoft.EntityFrameworkCore;

public static class DbContextExtensions
{
    public static IBulkLoadSourceConfiguration<TEntity> SqlServerLoadAndMerge<TEntity>(this DbContext dbContext)
        where TEntity : class
        => new FluentApi<TEntity, SqlServerBulkLoadAndMerge<TEntity>>(new SqlServerBulkLoadAndMerge<TEntity>(dbContext));
}