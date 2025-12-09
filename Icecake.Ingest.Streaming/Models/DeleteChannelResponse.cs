using System.Text.Json.Serialization;

namespace Icecake.Ingest.Streaming.Models;

/// <summary>
/// Represents the response returned after attempting to delete a streaming ingest channel.
/// </summary>
/// <remarks>
/// This response includes optional information about the status of the deleted channel or relevant
/// metadata related to the operation. It inherits from <see cref="StreamingIngestResponse"/> to
/// provide common response properties, such as status code and message.
/// </remarks>
public sealed class DeleteChannelResponse : StreamingIngestResponse
{
    // Optional on some deployments
    [JsonPropertyName("channel_status")]
    public ChannelStatus? ChannelStatus { get; init; }
    
    public override string ToString()
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("DeleteChannelResponse { ");
        
        var baseText = base.ToString();
        if (!string.IsNullOrEmpty(baseText) &&
            !string.Equals(baseText, nameof(StreamingIngestResponse), StringComparison.Ordinal))
        {
            sb.Append(baseText).Append(", ");
        }

        if (ChannelStatus != null)
        {
            sb.Append(nameof(ChannelStatus)).Append(" = ").Append(ChannelStatus).Append(", ");
        }

        // Trim trailing ", "
        if (sb.Length >= 2 && sb[sb.Length - 2] == ',')
            sb.Length -= 2;

        sb.Append(" }");
        return sb.ToString();
    }
}