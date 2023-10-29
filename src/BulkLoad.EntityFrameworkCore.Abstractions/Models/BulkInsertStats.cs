namespace BulkLoad.EntityFrameworkCore.Abstractions.Models;

public record BulkInsertStats(int RowsInsertedOrUpdated, int RowsDeletedOrFlaggedAsDeleted);