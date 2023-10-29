using BulkLoad.EntityFrameworkCore.Abstractions.Models;

namespace BulkLoad.EntityFrameworkCore.Abstractions.Implementation;

public interface IBulkLoadAndMerge<TEntity> where TEntity : class
{
    Task ExecuteAsync(CancellationToken cancellationToken);
    IAsyncEnumerable<BulkInsertRecord<TEntity>> ExecuteAndGetChangesAsync(CancellationToken cancellationToken);
}