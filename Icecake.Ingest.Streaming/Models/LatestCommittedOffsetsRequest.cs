using System.Text.Json.Serialization;

namespace Icecake.Ingest.Streaming.Models;

/// <summary>
/// Represents a request to fetch the latest committed offsets for specific channels in a specified database, schema, and pipe.
/// </summary>
/// <remarks>
/// This request is used in scenarios where an application needs to retrieve the latest offsets for tracking data processing progress.
/// </remarks>
public sealed class LatestCommittedOffsetsRequest
{
    [JsonPropertyName("database_name")] public required string DatabaseName { get; init; }
    [JsonPropertyName("schema_name")] public required string SchemaName { get; init; }
    [JsonPropertyName("pipe_name")] public required string PipeName { get; init; }
    [JsonPropertyName("channels")] public required List<string> Channels { get; init; }

    public override string ToString()
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("LatestCommittedOffsetsRequest { ");

        void Add(string name, object value)
        {
            sb.Append(name).Append(" = ").Append(value).Append(", ");
        }

        Add(nameof(DatabaseName), DatabaseName);
        Add(nameof(SchemaName), SchemaName);
        Add(nameof(PipeName), PipeName);

        if (Channels.Count > 0)
            sb.Append(nameof(Channels)).Append(" = [")
                .Append(string.Join(", ", Channels))
                .Append("], ");

        // Trim trailing comma (netstandard 2.0 safe)
        if (sb.Length >= 2 && sb[sb.Length - 2] == ',')
            sb.Length -= 2;

        sb.Append(" }");
        return sb.ToString();
    }
}