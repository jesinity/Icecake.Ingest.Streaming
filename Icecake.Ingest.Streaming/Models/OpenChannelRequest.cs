namespace Icecake.Ingest.Streaming.Models;

/// <summary>
/// Represents a request to open a Snowpipe ingest channel. This class contains
/// the necessary information required to identify and establish the channel.
/// </summary>
public sealed class OpenChannelRequest
{
    public required string Database { get; init; }
    public required string Schema { get; init; }
    public required string Pipe { get; init; }
    public required string Channel { get; init; }

    public override string ToString()
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("OpenChannelRequest { ");

        void Add(string name, object value)
        {
            sb.Append(name).Append(" = ").Append(value).Append(", ");
        }

        Add(nameof(Database), Database);
        Add(nameof(Schema), Schema);
        Add(nameof(Pipe), Pipe);
        Add(nameof(Channel), Channel);

        // Trim trailing comma
        if (sb.Length >= 2 && sb[sb.Length - 2] == ',')
            sb.Length -= 2;

        sb.Append(" }");
        return sb.ToString();
    }
}