namespace Icecake.Ingest.Streaming.Core;

/// <summary>
/// Represents a policy that dictates conditions under which data should be flushed
/// from a buffer to its intended destination. The policy defines parameters
/// like the maximum number of rows, maximum buffer size, flush interval, and
/// a minimum hold duration after appending data.
/// </summary>
public sealed class FlushPolicy
{
    public int MaxRows { get; init; } = 50_000;
    public int MaxBytes { get; init; } = 4_000_000;
    public TimeSpan Interval { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan MinHoldAfterAppend { get; init; } = TimeSpan.FromSeconds(10);
}