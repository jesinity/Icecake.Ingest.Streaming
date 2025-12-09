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
    
    public override string ToString()
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("StreamingIngestResponse { ");

        if (StatusCode != null)
            sb.Append(nameof(StatusCode)).Append(" = ").Append(StatusCode).Append(", ");

        if (Message != null)
            sb.Append(nameof(Message)).Append(" = ").Append(Message).Append(", ");

        // Trim trailing comma
        if (sb.Length >= 2 && sb[sb.Length - 2] == ',')
            sb.Length -= 2;

        sb.Append(" }");
        return sb.ToString();
    }
}