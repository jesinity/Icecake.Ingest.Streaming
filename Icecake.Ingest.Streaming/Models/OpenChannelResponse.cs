using System.Text.Json.Serialization;

namespace Icecake.Ingest.Streaming.Models;

/// <summary>
/// Represents the response received when opening a streaming ingest channel
/// for data ingestion in the Snowpipe Ingest system.
/// </summary>
/// <remarks>
/// The response contains information about the status of the channel and a
/// continuation token for next operations.
/// </remarks>
public sealed class OpenChannelResponse : StreamingIngestResponse
{
    [JsonPropertyName("next_continuation_token")]
    public string? NextContinuationToken { get; init; }

    [JsonPropertyName("channel_status")] public required ChannelStatus ChannelStatus { get; init; }
    
    public override string ToString()
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("OpenChannelResponse { ");

        // Include base class ToString() if meaningful
        var baseText = base.ToString();
        if (!string.IsNullOrEmpty(baseText) &&
            !string.Equals(baseText, nameof(StreamingIngestResponse), System.StringComparison.Ordinal))
        {
            sb.Append(baseText).Append(", ");
        }

        if (NextContinuationToken != null)
            sb.Append(nameof(NextContinuationToken))
                .Append(" = ")
                .Append(NextContinuationToken)
                .Append(", ");

        if (ChannelStatus != null)
            sb.Append(nameof(ChannelStatus))
                .Append(" = ")
                .Append(ChannelStatus)
                .Append(", ");

        // Remove trailing ", "
        if (sb.Length >= 2 && sb[sb.Length - 2] == ',')
            sb.Length -= 2;

        sb.Append(" }");
        return sb.ToString();
    }
}