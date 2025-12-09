using System.Text.Json.Serialization;

namespace Icecake.Ingest.Streaming.Models;

/// <summary>
/// Represents the response for the operation of appending rows to a specific channel in a streaming ingestion process.
/// </summary>
/// <remarks>
/// This response contains a token to manage continuation states for ensuring data consistency
/// during successive append operations.
/// </remarks>
public sealed class AppendRowsResponse : StreamingIngestResponse
{
    [JsonPropertyName("next_continuation_token")]
    public required string NextContinuationToken { get; init; }
    
    public override string ToString()
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("AppendRowsResponse { ");

        // Helper to skip null values
        void Add(string name, object value)
        {
            if (value != null)
            {
                sb.Append(name).Append(" = ").Append(value).Append(", ");
            }
        }

        Add(nameof(NextContinuationToken), NextContinuationToken);

        if (sb.Length >= 2 && sb[sb.Length - 2] == ',')
            sb.Length -= 2;

        sb.Append(" }");
        return sb.ToString();
    }
}


