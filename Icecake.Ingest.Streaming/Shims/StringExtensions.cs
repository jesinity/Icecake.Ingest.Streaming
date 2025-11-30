namespace Icecake.Ingest.Streaming.Shims;

/// <summary>
/// Provides extension methods for string operations.
/// </summary>
internal static class StringExtensions
{
    public static bool ContainsIgnoreCase(this string? s, string value)
    {
        if (s is null) return false;

#if NETSTANDARD2_0
        return s.IndexOf(value, StringComparison.OrdinalIgnoreCase) >= 0;
#else
        return s.Contains(value, StringComparison.OrdinalIgnoreCase);
#endif
    }
}