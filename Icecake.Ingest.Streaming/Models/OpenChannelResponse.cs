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

}