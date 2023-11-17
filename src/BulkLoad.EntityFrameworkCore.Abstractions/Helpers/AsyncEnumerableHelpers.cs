namespace System.Linq;

/// <summary>
/// Helper extensions for IAsyncEnumerable
/// </summary>
public static class AsyncEnumerableHelpers
{
    /// <summary>
    /// Allows passing a static IEnumerable as an IAsyncEnumerable, to support passing static data
    /// into methods that expect an IAsyncEnumerable.
    /// </summary>
    /// <param name="source">Source to enumerate</param>
    /// <typeparam name="T">Type of items in the source</typeparam>
    /// <returns>An IAsyncEnumerable that simply enumerates the given IEnumerable</returns>
    public static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IEnumerable<T> source)
    {
        foreach (var item in source)
            yield return item;

        await Task.CompletedTask;
    }
}