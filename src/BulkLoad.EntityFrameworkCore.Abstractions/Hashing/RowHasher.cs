using System.Buffers;
using System.Data;
using System.Text;
using System.Text.Json;
using BulkLoad.EntityFrameworkCore.Abstractions.Helpers;

namespace BulkLoad.EntityFrameworkCore.Abstractions.Hashing;

/// <summary>
/// Computes a hash value from a data row, regardless of source format (JSONL, Parquet, etc)
/// that can be used later in a merge statement to detect actual changes.
/// </summary>
internal static class RowHasher
{
    private static readonly JsonWriterOptions JsonOpts = new()
    {
        Indented = false
    };

    public static Guid ComputeRowHashAsGuid(DataRow row, IReadOnlyList<DataColumn> columns)
    {
        var bufferWriter = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(bufferWriter, JsonOpts);
        
        writer.WriteDataRow(row, columns);
        writer.Flush();
        
        var rowHash = Hasher.ComputeHash128(bufferWriter.WrittenMemory);
        
        return rowHash;
    }
    
    public static long ComputeRowHashAsBigInt(DataRow row, IReadOnlyList<DataColumn> columns)
    {
        var bufferWriter = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(bufferWriter, JsonOpts);
        
        writer.WriteDataRow(row, columns);
        writer.Flush();
        
        var rowHash = Hasher.ComputeHash64(Encoding.UTF8.GetString(bufferWriter.WrittenSpan));
        
        return rowHash;
    }

    public static string ComputeRowHashAsString(DataRow row, DataColumn[] columns)
        => ComputeRowHashAsGuid(row, columns).ToString("D");
}