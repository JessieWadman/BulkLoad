using System.Data;

namespace BulkLoad.EntityFrameworkCore.Abstractions.Helpers;

internal static class DataColumnCollectionHelper
{
    public static DataColumn[] ToArray(this DataColumnCollection columns)
    {
        var result = new DataColumn[columns.Count];
        columns.CopyTo(result, 0);
        return result;
    }
    
    public static IEnumerable<DataColumn> Exclude(this DataColumnCollection columns, params string[] columnNames)
    {
        for (var i = 0; i < columns.Count; i++)
        {
            var column = columns[i];
            if (!columnNames.Contains(column.ColumnName, StringComparer.OrdinalIgnoreCase))
                yield return column;
        }
    }
    
    public static IEnumerable<DataColumn> Exclude(this IEnumerable<DataColumn> columns, params string[] columnNames)
    {
        var arr = columns.ToArray();
        foreach (var column in arr)
        {
            if (!columnNames.Contains(column.ColumnName, StringComparer.OrdinalIgnoreCase))
                yield return column;
        }
    }

    public static bool TryGet(this DataColumnCollection columns, string columnName, out DataColumn? column)
    {
        foreach (DataColumn col in columns)
        {
            if (string.Equals(col.ColumnName, columnName, StringComparison.OrdinalIgnoreCase))
            {
                column = col;
                return true;
            }
        }

        column = null;
        return false;
    }
}