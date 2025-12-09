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
        
        public override string ToString()
        {
            var sb = new System.Text.StringBuilder();
            sb.Append("Item { ");

            if (ChannelName != null)
                sb.Append(nameof(ChannelName)).Append(" = ").Append(ChannelName).Append(", ");

            if (OffsetToken != null)
                sb.Append(nameof(OffsetToken)).Append(" = ").Append(OffsetToken).Append(", ");

            if (sb.Length >= 2 && sb[sb.Length - 2] == ',')
                sb.Length -= 2;

            sb.Append(" }");
            return sb.ToString();
        }
    }

    [JsonPropertyName("channels")] public List<Item> Channels { get; init; } = [];
    
    public override string ToString()
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("LatestCommittedOffsetsResponse { ");

        if (Channels != null && Channels.Count > 0)
        {
            sb.Append("Channels = [ ");

            for (int i = 0; i < Channels.Count; i++)
            {
                sb.Append(Channels[i]);
                if (i < Channels.Count - 1)
                    sb.Append(", ");
            }

            sb.Append(" ]");
        }

        if (sb.Length >= 2 && sb[sb.Length - 2] == ',')
            sb.Length -= 2;

        sb.Append(" }");
        return sb.ToString();
    }
}