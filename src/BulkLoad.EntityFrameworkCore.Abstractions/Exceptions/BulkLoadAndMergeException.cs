namespace BulkLoad.EntityFrameworkCore.Abstractions.Exceptions;

public class BulkLoadAndMergeException : Exception
{
    public BulkLoadAndMergeException()
    {
    }

    public BulkLoadAndMergeException(string? message) : base(message)
    {
    }

    public BulkLoadAndMergeException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}