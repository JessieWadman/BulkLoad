namespace BulkLoad.EntityFrameworkCore.Abstractions.Options;

public enum ComparisonMethod
{
    RowHash,
    ColumnByColumn,
    NoComparison
}

public class BulkInsertOptions<T>
{
    public ComparisonMethod ComparisonMethod = ComparisonMethod.ColumnByColumn;
    public readonly List<string> PropertiesToExcludeFromComparison = new();
    public readonly List<string> ColumnToNotUpdate = new();
    public string? RowHashProperty;
    public string? RowHashColumn;
    public string? SoftDeleteColumn = null;
    public readonly Dictionary<string, string> OnUpdateExpressions = new();
    public BulkInsertKind Kind = BulkInsertKind.IncrementalChanges;
    public IAsyncEnumerable<T> Source;
    public int BatchSize = 5000;
    public bool TrackChanges = false;
}