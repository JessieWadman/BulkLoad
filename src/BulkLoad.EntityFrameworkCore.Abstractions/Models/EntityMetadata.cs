using System.Diagnostics.CodeAnalysis;

namespace BulkLoad.EntityFrameworkCore.Abstractions.Models;

public record EntityMetadata(
    IReadOnlyList<EntityProperty> Properties,
    IReadOnlyList<string> PrimaryKeyColumns,
    Type Type,
    string TableName,
    string? TableSchema)
{
    public bool TryGetColumnName(string propertyName, [MaybeNullWhen(false)] out string? columnName)
    {
        columnName = Properties.FirstOrDefault(p => p.PropertyName == propertyName)?.ColumnName;
        return columnName != null;
    }

    public string GetColumnName(string propertyName)
    {
        if (!TryGetColumnName(propertyName, out var columnName) || columnName == null)
            throw new InvalidOperationException($"The property '{propertyName}' is not mapped to a column!");
        return columnName;
    }
}

public record EntityProperty(
    string ColumnName,
    bool IsDbGenerated,
    string SqlServerType,
    bool IsNullable,
    string PropertyName
);