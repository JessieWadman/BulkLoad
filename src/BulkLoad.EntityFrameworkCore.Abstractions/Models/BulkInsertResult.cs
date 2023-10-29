namespace BulkLoad.EntityFrameworkCore.Abstractions.Models;

public enum BulkInsertAction
{
    Added,
    Updated,
    Removed
}

public record BulkInsertRecord<T>(BulkInsertAction Action, T Record);