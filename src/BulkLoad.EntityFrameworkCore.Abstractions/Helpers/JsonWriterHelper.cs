using System.Data;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace BulkLoad.EntityFrameworkCore.Abstractions.Helpers;

public static class JsonWriterHelper
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WritePropertyWithValue(this Utf8JsonWriter writer, string propertyName, object? value)
    {
        writer.WritePropertyName(propertyName);
        if (value == null || value == DBNull.Value)
        {
            writer.WriteNullValue();
            return;
        }

        switch (value)
        {
            case byte b:
                writer.WriteNumberValue(b);
                break;
            case int i32:
                writer.WriteNumberValue(i32);
                break;
            case long i64:
                writer.WriteNumberValue(i64);
                break;
            case double dbl:
                writer.WriteNumberValue(dbl);
                break;
            case decimal dec:
                writer.WriteNumberValue(dec);
                break;
            case float flt:
                writer.WriteNumberValue(flt);
                break;
            case short i16:
                writer.WriteNumberValue(i16);
                break;
            case ushort u16:
                writer.WriteNumberValue(u16);
                break;
            case uint u32:
                writer.WriteNumberValue(u32);
                break;
            case ulong u64:
                writer.WriteNumberValue(u64);
                break;
            case bool boolean:
                writer.WriteBooleanValue(boolean);
                break;
            case byte[] bytes:
                writer.WriteBase64StringValue(bytes);
                break;
            case DateTime dt:
                writer.WriteStringValue(dt.ToString("O"));
                break;
            case DateTimeOffset dto:
                writer.WriteStringValue(dto.ToString("O"));
                break;
            case DateOnly date: 
                writer.WriteStringValue(date.ToString("yyyy\\-MM\\-dd"));
                break;
            case TimeOnly time:
                writer.WriteStringValue(time.ToString("T"));
                break;
            default:
                writer.WriteStringValue(Convert.ToString(value, CultureInfo.InvariantCulture));
                break;
        }
    }
    
    public static void WriteDataRow(
        this Utf8JsonWriter writer, 
        DataRow row,
        IReadOnlyList<DataColumn> columns,
        Dictionary<string, object?>? additionalProperties = null)
    {
        writer.WriteStartObject();

        var values = row.ItemArray;
        var columnCount = columns.Count;
        
        for (var i = 0; i < columnCount; i++)
        {
            var column = columns[i];
            writer.WritePropertyWithValue(column.ColumnName, values[column.Ordinal]);
        }

        if (additionalProperties != null)
        {
            foreach (var kp in additionalProperties)
            {
                writer.WritePropertyWithValue(kp.Key, kp.Value);
            }
        }

        writer.WriteEndObject();
    }
}