using System.Net;
using System.Text.Json;

namespace Icecake.Ingest.Streaming.Core;

/// <summary>
/// Represents an exception that occurs during the ingestion process in a Snowpipe environment.
/// </summary>
public sealed class SnowpipeIngestException : Exception
{
    public HttpStatusCode StatusCode { get; }
    public string? ErrorCode { get; }
    public string? ErrorMessage { get; }
    public string RawBody { get; }

    /// <summary>
    /// Represents an exception that occurs during the ingestion process within a Snowpipe environment.
    /// </summary>
    private SnowpipeIngestException(HttpStatusCode code, string raw, string? errCode, string? msg) : base($"HTTP {(int)code} {code}: {msg ?? errCode ?? raw}")
    {
        StatusCode = code;
        RawBody = raw;
        ErrorCode = errCode;
        ErrorMessage = msg;
    }

    /// <summary>
    /// Creates a new instance of <see cref="SnowpipeIngestException"/> based on the provided HTTP status code and response body.
    /// </summary>
    /// <param name="code">The HTTP status code received from the Snowpipe ingestion service.</param>
    /// <param name="body">The raw body of the HTTP response containing error details.</param>
    /// <returns>A <see cref="SnowpipeIngestException"/> populated with information parsed from the response or a generic exception if parsing fails.</returns>
    public static SnowpipeIngestException FromHttp(HttpStatusCode code, string body)
    {
        try
        {
            using var doc = JsonDocument.Parse(string.IsNullOrWhiteSpace(body) ? "{}" : body);
            var root = doc.RootElement;
            var ecode = root.TryGetProperty("code", out var c) ? c.GetString() : root.TryGetProperty("errorCode", out var ec) ? ec.GetString() : null;
            var msg = root.TryGetProperty("message", out var m) ? m.GetString() : root.TryGetProperty("error", out var em) ? em.GetString() : null;
            return new SnowpipeIngestException(code, body, ecode, msg);
        }
        catch
        {
            return new SnowpipeIngestException(code, body, null, null);
        }
    }
}