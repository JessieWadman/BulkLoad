namespace BulkLoad.EntityFrameworkCore.Abstractions.Options;

/// <summary>
/// Indicates how to compare incoming records and existing records for changes during the merge statement.
/// </summary>
public enum ComparisonMethod
{
    /// <summary>
    /// Comparison in the merge statement between incoming records and existing records is done by hashing the source record and comparing
    /// the hash against the stored record's hash.
    /// </summary>
    RowHash,
    
    /// <summary>
    /// Comparison in the merge statement between incoming records and existing records is done by comparing each field for difference.
    /// </summary>
    ColumnByColumn,
    
    /// <summary>
    /// Do not compare records, always update existing records. Fastest approach, if you know you will always update all
    /// records (for example if you have a "LastSeen" column that should always be updated. 
    /// </summary>
    NoComparison
}

/// <summary>
/// Indicates how the soft delete (if any) is configured for a table
/// </summary>
public enum SoftDeleteType
{
    /// <summary>
    /// Entity doesn't support soft deletion. Remove records that do not exist in incoming snapshot.
    /// </summary>
    None,
    
    /// <summary>
    /// The table has a boolean "deleted" column of type boolean, that indicates if the record is soft deleted or not.
    /// </summary>
    Bool,
    
    /// <summary>
    /// The table has a datetime column, that indicates when the record was soft deleted, or null if it hasn't been
    /// soft deleted.
    /// </summary>
    DateTime
}

internal sealed class BulkInsertOptions<T>
{
    public ComparisonMethod ComparisonMethod = ComparisonMethod.ColumnByColumn;
    public readonly List<string> PropertiesToExcludeFromComparison = new();
    public readonly List<string> ColumnToNotUpdate = new();
    public string? RowHashProperty;
    public string? RowHashColumn;
    public string? SoftDeleteColumn = null;
    public SoftDeleteType SoftDeleteType = SoftDeleteType.None;
    public readonly Dictionary<string, string> OnUpdateExpressions = new();
    public BulkInsertKind Kind = BulkInsertKind.IncrementalChanges;
    public IAsyncEnumerable<T>? Source;
    public int BatchSize = 5000;
    public bool TrackChanges = false;
}