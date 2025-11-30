using System.Net;
using System.IO.Compression;

namespace Icecake.Ingest.Streaming.Core;

/// <summary>
/// Represents configuration options for the Snowflake client.
/// </summary>
public sealed class SnowflakeClientOptions
{
    /// <summary>
    /// Specifies the User-Agent string sent with each HTTP request to the Snowflake service.
    /// </summary>
    public string UserAgent { get; init; } = "snowflake-ingest-dotnet/0.1";

    /// <summary>
    /// Specifies the timeout duration for HTTP requests made by the Snowflake client.
    /// </summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(100);

    /// <summary>
    /// Gets the base duration for calculating retry backoff.
    /// </summary>
    public TimeSpan RetryBackoffBase { get; init; } = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// Gets the maximum number of retry attempts for failed HTTP requests.
    /// </summary>
    public int MaxRetries { get; init; } = 5;

    /// <summary>
    /// Gets or sets the proxy information used for HTTP communication with the Snowflake service.
    /// </summary>
    public IWebProxy? Proxy { get; init; }

    /// <summary>
    /// Indicates whether the Snowflake internal client should validate server certificates during TLS/SSL communication.
    /// </summary>
    /// <remarks>
    /// If set to <c>true</c>, the client will enforce server certificate validation.
    /// If set to <c>false</c>, server certificates will not be validated.
    /// </remarks>
    public bool ValidateCertificates { get; init; } = true;

    /// <summary>
    /// Determines whether gzip compression is enabled for append operations.
    /// If set to <see langword="true"/>, data will be compressed using gzip when appending, provided
    /// the payload size meets the specified threshold for compression.
    /// This can optimize the transfer of data and reduce network overhead during ingestion.
    /// </summary>
    public bool EnableGzipOnAppend { get; init; } = true;

    /// <summary>
    /// Gets the minimum payload size, in bytes, that will trigger gzip compression when appending data.
    /// Payloads smaller than this value will not be compressed.
    /// </summary>
    public int GzipMinBytes { get; init; } = 4 * 1024;
    public CompressionLevel GzipLevel { get; init; } =
        CompressionLevel.Fastest;
}