namespace Icecake.Ingest.Streaming.Core;

public enum ChannelState
{
    /// <summary>Channel object created but not yet opened.</summary>
    Created,

    /// <summary>Channel is currently opening (awaiting confirmation from Snowflake).</summary>
    Opening,

    /// <summary>Channel is ready to ingest data.</summary>
    Open,

    /// <summary>An error occurred during open, flush, or network operations.</summary>
    Error,

    /// <summary>Channel was explicitly closed (but not dropped).</summary>
    Closed,

    /// <summary>Channel was dropped from the Snowflake table.</summary>
    Dropped
}