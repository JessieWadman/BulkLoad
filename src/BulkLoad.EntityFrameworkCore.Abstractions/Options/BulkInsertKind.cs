namespace BulkLoad.EntityFrameworkCore.Abstractions.Options;

public enum BulkInsertKind
{
    /// <summary>
    /// Add or update the incoming records that are provided, leave the remaining records as is.
    /// </summary>
    IncrementalChanges,
    
    /// <summary>
    /// Add or update the incoming records that are provided, and then delete the remaining records that were not included.
    /// </summary>
    Snapshot
}