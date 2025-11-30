namespace Icecake.Ingest.Streaming.Core;

/// <summary>
/// Represents a discrete chunk of blob data, containing its identifier, payload,
/// and associated metadata. This class encapsulates a unit of data that can be managed
/// independently within a streaming or ingestion pipeline.
/// </summary>
public sealed class Payload
{
    /// <summary>
    /// Gets the unique identifier for the data chunk.
    /// </summary>
    /// <remarks>
    /// The <c>ChunkId</c> is a required property that uniquely identifies the chunk of data.
    /// </remarks>
    public required string ChunkId { get; init; }

    /// <summary>
    /// Gets the binary data payload of the chunk.
    /// </summary>
    /// <remarks>
    /// The <c>Data</c> property represents the serialized binary content of the payload.
    /// </remarks>
    public required byte[] Data { get; init; }

    /// <summary>
    /// Gets the metadata associated with the data chunk.
    /// </summary>
    public required ChunkMetadata Metadata { get; init; }
}