# Bulk Load and Merge (BLAM) extensions for Microsoft Entity Framework Core
These libraries allow the user to merge a large set of entities into a database, with change detection.

# Usage
```csharp

// Some means of getting entities that should be loaded into the table
private IAsyncEnumerable<Employee> GetEmployeesFromWebApi(...)
{
    ...
}

await using var db = new MyDbContext(dbOptions);

db
    // Bulk load Employee entities into its database table
    .PostgresLoadAndMerge<Employee>()
    
    // Load a snapshot of entities (will remove/soft delete entities in table that
    // are not included in the local list of entities)
    .FromSnapshot(GetEmployeesFromWebAp())
    
    // If you do not want to load an entire snapshot, but instead a subset of entities, where
    // entities in the target table are not removed/soft deleted, then instead use:
    // .FromIncrementalChanges(source)
    
    // Use a specific column to compare local entities to stored entities for changes
    // The library will automatically compute a hash for each entity, and compare against stored
    // hashes, to figure out which entities contain changes.
    .WithRowHashComparison(e => e.RowHash)
    
    // Some properties should not be included in the hash (change detection), but instead only
    // be set if an update occurs.
    .ExcludePropertyFromComparison(e => e.LastUpdatedUtc)
    
    // If an update occurs, use a server side expression to fill a column
    .OnUpdateRecordSetExpression(e => e.LastUpdatedUtc, "current_timestamp")
    
    // Use soft deletes, by flipping a boolean flag, rather than removing those entities
    // from the target table that are not included in the source
    .WithSoftDeletion(e => e.Deleted)
    
    // Merge the entities, but also capture the changes
    .ExecuteAndGetChangesAsync(CancellationToken.None);
```

