namespace Icecake.Ingest.Streaming.Core;

/// <summary>
/// Represents metadata about a data chunk, including its unique identifier, size,
/// number of rows, checksum, and an optional offset token. Chunk metadata provides
/// descriptive details necessary for managing and verifying individual chunks of data
/// within a streaming or ingestion pipeline.
/// </summary>
public sealed class ChunkMetadata 
{
    public required string ChunkId { get; init; }
    public int RowCount { get; init; }
    public long SizeBytes { get; init; }
    public string? Checksum { get; init; }
    public string? OffsetToken { get; init; }
}