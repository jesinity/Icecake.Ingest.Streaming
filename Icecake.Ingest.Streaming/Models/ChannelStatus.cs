using System.Text.Json.Serialization;

namespace Icecake.Ingest.Streaming.Models;

/// <summary>
/// Represents the status of a channel in the streaming ingest process.
/// </summary>
/// <remarks>
/// The ChannelStatus class encapsulates details about the state and related metadata of a streaming ingestion channel.
/// This includes information such as the status code, offsets, timestamps, processing statistics, and error details if any.
/// </remarks>
public sealed class ChannelStatus
{
    [JsonPropertyName("channel_status_code")]
    public required string ChannelStatusCode { get; init; }

    [JsonPropertyName("last_committed_offset_token")]
    public string? LastCommittedOffsetToken { get; init; }

    [JsonPropertyName("created_on_ms")] public long? CreatedOnMs { get; init; }
    [JsonPropertyName("database_name")] public string? DatabaseName { get; init; }
    [JsonPropertyName("schema_name")] public string? SchemaName { get; init; }
    [JsonPropertyName("pipe_name")] public string? PipeName { get; init; }
    [JsonPropertyName("channel_name")] public string? ChannelName { get; init; }
    [JsonPropertyName("rows_inserted")] public int? RowsInserted { get; init; }
    [JsonPropertyName("rows_parsed")] public int? RowsParsed { get; init; }
    [JsonPropertyName("rows_error_count")] public int? RowsErrorCount { get; init; }

    [JsonPropertyName("last_error_offset_upper_bound")]
    public string? LastErrorOffsetUpperBound { get; init; }

    [JsonPropertyName("last_error_message")]
    public string? LastErrorMessage { get; init; }

    [JsonPropertyName("last_error_timestamp_ms")]
    public long? LastErrorTimestampMs { get; init; }

    [JsonPropertyName("snowflake_avg_processing_latency_ms")]
    public long? SnowflakeAvgProcessingLatencyMs { get; init; }
}