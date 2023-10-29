using BulkLoad.EntityFrameworkCore.Abstractions.Models;

namespace BulkLoad.EntityFrameworkCore.Abstractions.Implementation;

public interface IBulkLoadAndMerge<TEntity> where TEntity : class
{
    /// <summary>
    /// Executes a merge
    /// </summary>
    /// <param name="cancellationToken">Optional cancellation token</param>
    /// <returns>Number of records that had changes and were merged</returns>
    Task<int> ExecuteAsync(CancellationToken cancellationToken);
    
    /// <summary>
    /// Executes the merge
    /// </summary>
    /// <param name="cancellationToken">Optional cancellation token</param>
    /// <returns>An enumeration of individual changes made</returns>
    IAsyncEnumerable<BulkInsertRecord<TEntity>> ExecuteAndGetChangesAsync(CancellationToken cancellationToken);
}