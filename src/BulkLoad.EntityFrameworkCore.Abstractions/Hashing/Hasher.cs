using System.IO.Hashing;
using System.Runtime.InteropServices;
using System.Text;

namespace BulkLoad.EntityFrameworkCore.Abstractions.Hashing;

internal static class Hasher
{
    public static ulong ComputeHashU64(ReadOnlySpan<byte> bytes)
        => XxHash64.HashToUInt64(bytes);
    
    public static Guid ComputeHash128(ReadOnlySpan<byte> bytes)
    {
        var hash = XxHash128.HashToUInt128(bytes);
        var span = MemoryMarshal.Cast<UInt128, byte>(MemoryMarshal.CreateReadOnlySpan(ref hash, 1));
        return new Guid(span);
    }
    
    private static long ULongToLong(ulong ulongValue)
        => unchecked((long)ulongValue + long.MinValue);

    public static long ComputeHash64(ReadOnlySpan<byte> bytes)
        => ULongToLong(ComputeHashU64(bytes));
    
    public static ulong ComputeHashU64(ReadOnlyMemory<byte> bytes)
        => ComputeHashU64(bytes.Span);
    
    public static long ComputeHash64(ReadOnlyMemory<byte> bytes)
        => ComputeHash64(bytes.Span);

    public static Guid ComputeHash128(ReadOnlyMemory<byte> bytes)
        => ComputeHash128(bytes.Span);
    
    public static Guid ComputeHash128(ReadOnlySpan<char> chars)
    {
        var dest = new byte[Encoding.UTF8.GetByteCount(chars)].AsSpan();
        var length = Encoding.UTF8.GetBytes(chars, dest);
        return ComputeHash128(dest[..length]);
    }
    
    public static ulong ComputeHashU64(ReadOnlySpan<char> chars)
    {
        var dest = new byte[Encoding.UTF8.GetByteCount(chars)].AsSpan();
        var length = Encoding.UTF8.GetBytes(chars, dest);
        return ComputeHashU64(dest[..length]);
    }

    public static long ComputeHash64(ReadOnlySpan<char> chars)
        => ULongToLong(ComputeHashU64(chars));
}