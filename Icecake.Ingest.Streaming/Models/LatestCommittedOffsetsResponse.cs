using System.Text.Json.Serialization;

namespace Icecake.Ingest.Streaming.Models;

/// <summary>
/// Represents the response received when querying for the latest committed offset tokens
/// for specified channels in a Snowflake schema and pipe.
/// </summary>
public sealed class LatestCommittedOffsetsResponse
{
    public sealed class Item
    {
        [JsonPropertyName("channel_name")] public string? ChannelName { get; init; }
        [JsonPropertyName("offset_token")] public string? OffsetToken { get; init; }
    }

    [JsonPropertyName("channels")] public List<Item> Channels { get; init; } = [];
}