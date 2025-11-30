using System.Text.Json.Serialization;

namespace Icecake.Ingest.Streaming.Models;

/// <summary>
/// Represents a base class for all response types used in streaming ingestion operations.
/// </summary>
/// <remarks>
/// This abstract base class provides common properties such as status code and message,
/// which can be used by derived classes for unified response handling.
/// </remarks>
public abstract class StreamingIngestResponse
{
    [JsonPropertyName("status_code")] public long? StatusCode { get; init; }
    [JsonPropertyName("message")] public string? Message { get; init; }
}