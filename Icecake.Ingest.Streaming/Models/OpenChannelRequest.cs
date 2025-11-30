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
}