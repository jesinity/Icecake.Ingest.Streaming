using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Icecake.Ingest.Streaming.Auth;
using Icecake.Ingest.Streaming.Core;
using Icecake.Ingest.Streaming.Models;
using Icecake.Ingest.Streaming.Shims;
using Microsoft.Extensions.Logging;

namespace Icecake.Ingest.Streaming;

/// <summary>
/// Represents a client for performing streaming data ingestion with Snowflake.
/// Provides methods to manage channels, send data, and retrieve the status of operations related to streaming ingestion.
/// It uses the V2 version of the Snowpipe streaming API.
/// </summary>
public sealed class SnowpipeIngestClient(
    IAuthProvider auth,
    SnowflakeClientOptions options,
    HttpClient httpClient,
    ILogger<SnowpipeIngestClient> logger)
{

    private static readonly JsonSerializerOptions SerializerOptions = JsonHelper.DefaultCamelCase;
    
    /// <summary>
    /// Opens a streaming ingest channel for data ingestion to the specified database, schema, pipe, and channel.
    /// It uses the v2 path: /v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}
    /// </summary>
    /// <param name="req">
    /// An instance of <see cref="OpenChannelRequest"/> containing the database, schema, pipe, and channel information required to open the channel.
    /// </param>
    /// <param name="ct">
    /// A <see cref="CancellationToken"/> used to propagate notifications that the operation should be canceled.
    /// </param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains an <see cref="OpenChannelResponse"/> instance with the details of the response from the server.
    /// </returns>
    public Task<OpenChannelResponse> OpenChannelAsync(OpenChannelRequest req, CancellationToken ct = default)
    {
        var path =
            $"/v2/streaming/databases/{Uri.EscapeDataString(req.Database)}/" +
            $"schemas/{Uri.EscapeDataString(req.Schema)}/" +
            $"pipes/{Uri.EscapeDataString(req.Pipe)}/" +
            $"channels/{Uri.EscapeDataString(req.Channel)}";

        // PUT with {} body
        return SendJsonAsync<object, OpenChannelResponse>(HttpMethod.Put, path, payload: null, headers: null, ct);
    }

    /// <summary>
    /// Deletes a streaming ingest channel for the specified database, schema, pipe, and channel.
    /// It uses the v2 path: /v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}.
    /// </summary>
    /// <param name="coords">the pipe coordinates</param>
    /// <param name="channel">
    /// The name of the channel to be deleted.
    /// </param>
    /// <param name="ct">
    /// A <see cref="CancellationToken"/> used to propagate notifications that the operation should be canceled.
    /// </param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains a <see cref="DeleteChannelResponse"/> instance with the details of the response from the server, or null if the response is empty.
    /// </returns>
    public Task<DeleteChannelResponse?> DeleteChannelAsync(
        SchemaObjectCoords coords,
        string channel, CancellationToken ct = default)
    {
        var path =
            $"/v2/streaming/databases/{Uri.EscapeDataString(coords.Database)}/" +
            $"schemas/{Uri.EscapeDataString(coords.Schema)}/" +
            $"pipes/{Uri.EscapeDataString(coords.Name)}/" +
            $"channels/{Uri.EscapeDataString(channel)}";

        return SendPossiblyEmptyAsync<DeleteChannelResponse>(HttpMethod.Delete, path, ct);
    }

    /// <summary>
    /// Appends rows to a specified channel within a database, schema, and pipe using newline-delimited JSON (NDJSON) payload.
    /// </summary>
    /// <param name="coords">the coordinates of the pipe</param>
    /// <param name="channel">
    /// The target channel within the specified pipe for appending rows.
    /// </param>
    /// <param name="ndjson">
    /// A read-only memory containing the payload data in NDJSON (newline-delimited JSON) format to be ingested.
    /// </param>
    /// <param name="continuationToken">
    /// A token used to maintain the state of the streaming operation, ensuring data consistency across retries or continuation calls.
    /// </param>
    /// <param name="offsetToken">
    /// An optional token representing the offset within the channel to which rows should be appended.
    /// </param>
    /// <param name="ct">
    /// A <see cref="CancellationToken"/> to propagate notification that the operation should be canceled.
    /// </param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains an <see cref="AppendRowsResponse"/> instance
    /// with the details of the server response after appending rows.
    /// </returns>
    public Task<AppendRowsResponse> AppendRowsAsync(
        SchemaObjectCoords coords,
        string channel,
        ReadOnlyMemory<byte> ndjson,
        string continuationToken,
        string? offsetToken,
        CancellationToken ct = default)
    {
        var basePath =
            $"/v2/streaming/data/databases/{Uri.EscapeDataString(coords.Database)}/" +
            $"schemas/{Uri.EscapeDataString(coords.Schema)}/" +
            $"pipes/{Uri.EscapeDataString(coords.Name)}/" +
            $"channels/{Uri.EscapeDataString(channel)}/rows";

        var qp = new List<string> { $"continuationToken={Uri.EscapeDataString(continuationToken)}" };
        if (!string.IsNullOrEmpty(offsetToken))
            qp.Add($"offsetToken={Uri.EscapeDataString(offsetToken)}");

        var path = qp.Count > 0 ? $"{basePath}?{string.Join("&", qp)}" : basePath;

        return PostNdjsonAsync<AppendRowsResponse>(path, ndjson, ct);
    }

    
    private async Task<TRes> PostNdjsonAsync<TRes>(string path, ReadOnlyMemory<byte> ndjson, CancellationToken ct)
    {
        path = NormalizePath(path);

        if (!IsBootstrapPath(path) && auth is IIngestHostProvider ihp)
            await ihp.EnsureReadyAsync(ct).ConfigureAwait(false);

        var baseUri = ResolveBaseFor(path);
        var url = new Uri(baseUri, path);

        var attempt = 0;
        HttpResponseMessage? res = null;

        // Decide compression up-front
        var tryGzip = options.EnableGzipOnAppend && ndjson.Length >= options.GzipMinBytes;
        byte[]? gzPayload = null;
        if (tryGzip)
            gzPayload = CompressionHelper.Gzip(ndjson, options.GzipLevel);

        while (true)
        {
            using var msg = new HttpRequestMessage(HttpMethod.Post, url);

            if (tryGzip && gzPayload is not null)
            {
                msg.Content = new ByteArrayContent(gzPayload);
                msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/x-ndjson");
                msg.Content.Headers.ContentEncoding.Add("gzip");
            }
            else
            {
                msg.Content = new ByteArrayContent(ndjson.ToArray());
                msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/x-ndjson");
            }

            msg.Headers.Accept.Clear();
            msg.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            await auth.AttachAsync(msg, ct).ConfigureAwait(false);

            try
            {
                res = await httpClient.SendAsync(msg, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);

                // If server doesn’t like gzip, retry once without it
                if (tryGzip && res.StatusCode is HttpStatusCode.UnsupportedMediaType or HttpStatusCode.BadRequest)
                {
                    var body = await SafeReadAsStringAsync(res, ct).ConfigureAwait(false);
                    if (body.ContainsIgnoreCase("Content-Encoding") ||
                        body.ContainsIgnoreCase("gzip"))
                    {
                        logger.LogInformation("Server rejected gzip, retrying without compression once…");
                        res.Dispose();
                        tryGzip = false;
                        continue;
                    }
                }

                if (ShouldRetry(res.StatusCode) && attempt < options.MaxRetries)
                {
                    attempt++;
                    var delay = ComputeDelay(attempt, res);
                    res.Dispose();
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                    continue;
                }

                if (!res.IsSuccessStatusCode)
                {
                    var errText = await SafeReadAsStringAsync(res, ct).ConfigureAwait(false);
                    throw SnowpipeIngestException.FromHttp(res.StatusCode, errText);
                }
                
#if NETSTANDARD2_0
                var jsonText = await res.Content.ReadAsStringAsync().ConfigureAwait(false);
#else   
var jsonText = await res.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
#endif
                
                if (string.IsNullOrWhiteSpace(jsonText))
                    throw new InvalidOperationException("Empty JSON response");
                
                // Pretty-print for diagnostics
                try
                {
                    using var doc = JsonDocument.Parse(jsonText);
                    var pretty = JsonSerializer.Serialize(doc, new JsonSerializerOptions { WriteIndented = true });
                    Console.WriteLine("RESPONSE JSON:\n" + pretty);
                }
                catch (JsonException)
                {
                    Console.WriteLine("RESPONSE BODY (non-JSON):\n" + jsonText);
                }

                return JsonSerializer.Deserialize<TRes>(jsonText, SerializerOptions)
                       ?? throw new InvalidOperationException("Empty JSON response");
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested && attempt < options.MaxRetries)
            {
                attempt++;
                await Task.Delay(ComputeDelay(attempt, null), ct).ConfigureAwait(false);
            }
            catch (HttpRequestException) when (attempt < options.MaxRetries)
            {
                attempt++;
                await Task.Delay(ComputeDelay(attempt, null), ct).ConfigureAwait(false);
            }
            finally
            {
                res?.Dispose();
            }
        }
    }

    private async Task<TRes> SendJsonAsync<TReq, TRes>(
        HttpMethod method,
        string path,
        TReq? payload,
        IDictionary<string, string>? headers,
        CancellationToken ct)
    {
        path = NormalizePath(path);

        if (!IsBootstrapPath(path) && auth is IIngestHostProvider ihp)
            await ihp.EnsureReadyAsync(ct).ConfigureAwait(false);

        var baseUri = ResolveBaseFor(path);
        var url = new Uri(baseUri, path);

        var attempt = 0;
        HttpResponseMessage? res = null;

        while (true)
        {
            var json = payload is null ? "{}" : JsonSerializer.Serialize(payload, SerializerOptions);

            using var msg = new HttpRequestMessage(method, url);
            msg.Content = new StringContent(json, Encoding.UTF8, "application/json");

            msg.Headers.Accept.Clear();
            msg.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            if (headers != null)
                foreach (var kv in headers)
                    msg.Headers.TryAddWithoutValidation(kv.Key, kv.Value);

            await auth.AttachAsync(msg, ct).ConfigureAwait(false);
            Console.WriteLine($"HTTP {method} {msg.RequestUri}");

            try
            {
                res = await httpClient.SendAsync(msg, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);

                if (ShouldRetry(res.StatusCode) && attempt < options.MaxRetries)
                {
                    attempt++;
                    var delay = ComputeDelay(attempt, res);
                    res.Dispose();
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                    continue;
                }

                if (!res.IsSuccessStatusCode)
                {
                    var errText = await SafeReadAsStringAsync(res, ct).ConfigureAwait(false);
                    throw SnowpipeIngestException.FromHttp(res.StatusCode, errText);
                }

#if NETSTANDARD2_0
                var jsonText = await res.Content.ReadAsStringAsync().ConfigureAwait(false);
#else
var jsonText = await res.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
#endif
                if (string.IsNullOrWhiteSpace(jsonText))
                    throw new InvalidOperationException("Empty JSON response");

                try
                {
                    using var doc = JsonDocument.Parse(jsonText);
                    var pretty = JsonSerializer.Serialize(doc, options: new JsonSerializerOptions() { WriteIndented = true });
                    Console.WriteLine("RESPONSE JSON:\n" + pretty);
                }
                catch (JsonException)
                {
                    Console.WriteLine("RESPONSE BODY (non-JSON):\n" + jsonText);
                }

                return JsonSerializer.Deserialize<TRes>(jsonText, SerializerOptions)
                       ?? throw new InvalidOperationException("Empty JSON response");
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested && attempt < options.MaxRetries)
            {
                attempt++;
                await Task.Delay(ComputeDelay(attempt, null), ct).ConfigureAwait(false);
            }
            catch (HttpRequestException) when (attempt < options.MaxRetries)
            {
                attempt++;
                await Task.Delay(ComputeDelay(attempt, null), ct).ConfigureAwait(false);
            }
            finally
            {
                res?.Dispose();
            }
        }
    }

    public Task<ChannelStatus> GetChannelStatusAsync(
        string database, string schema, string pipe, string channel, CancellationToken ct = default)
    {
        var path =
            $"/v2/streaming/databases/{Uri.EscapeDataString(database)}/" +
            $"schemas/{Uri.EscapeDataString(schema)}/" +
            $"pipes/{Uri.EscapeDataString(pipe)}/" +
            $"channels/{Uri.EscapeDataString(channel)}";

        // GET returns the status fields at the top level, which match ChannelStatusV2
        return SendJsonAsync<object, ChannelStatus>(
            HttpMethod.Get, path, payload: null, headers: null, ct);
    }

    /// <summary>
    /// Retrieves the latest committed offset tokens for the specified channels in a Snowflake database, schema, and pipe.
    /// This method uses the v2 Snowpipe streaming API path: /v2/streaming/channels/status/
    /// </summary>
    /// <param name="database">
    /// The name of the database containing the targeted schema and pipe.
    /// </param>
    /// <param name="schema">
    /// The name of the schema containing the targeted pipe.
    /// </param>
    /// <param name="pipe">
    /// The name of the pipe for which the latest committed offset tokens are requested.
    /// </param>
    /// <param name="channels">
    /// A collection of channel names for which the latest committed offset tokens are to be retrieved.
    /// </param>
    /// <param name="ct">
    /// A <see cref="CancellationToken"/> used to propagate notifications that the operation should be canceled.
    /// </param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains an instance of <see cref="LatestCommittedOffsetsResponse"/>
    /// with the details of the latest committed offsets for the specified channels.
    /// </returns>
    public Task<LatestCommittedOffsetsResponse> GetLatestCommittedOffsetTokenAsync(
        string database, string schema, string pipe, IEnumerable<string> channels, CancellationToken ct = default)
    {
        var body = new LatestCommittedOffsetsRequest
        {
            DatabaseName = database,
            SchemaName = schema,
            PipeName = pipe,
            Channels = channels.ToList()
        };
        return SendJsonAsync<LatestCommittedOffsetsRequest, LatestCommittedOffsetsResponse>(
            HttpMethod.Post,
            "/v2/streaming/channels/status/", body, new Dictionary<string, string>(), ct);
    }


    // Tolerant to empty bodies (e.g., 200/204 on DELETE)
    private async Task<TRes?> SendPossiblyEmptyAsync<TRes>(HttpMethod method, string path, CancellationToken ct)
    {
        path = NormalizePath(path);

        if (!IsBootstrapPath(path) && auth is IIngestHostProvider ihp)
            await ihp.EnsureReadyAsync(ct).ConfigureAwait(false);

        var baseUri = ResolveBaseFor(path);
        var url = new Uri(baseUri, path);

        var attempt = 0;
        HttpResponseMessage? res = null;

        while (true)
        {
            using var msg = new HttpRequestMessage(method, url);
            msg.Content = new StringContent("{}", Encoding.UTF8, "application/json");
            msg.Headers.Accept.Clear();
            msg.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            await auth.AttachAsync(msg, ct).ConfigureAwait(false);
            Console.WriteLine($"HTTP {method} {msg.RequestUri}");

            try
            {
                res = await httpClient.SendAsync(msg, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);

                if (ShouldRetry(res.StatusCode) && attempt < options.MaxRetries)
                {
                    attempt++;
                    var delay = ComputeDelay(attempt, res);
                    res.Dispose();
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                    continue;
                }

                if (!res.IsSuccessStatusCode)
                {
                    var errText = await SafeReadAsStringAsync(res, ct).ConfigureAwait(false);
                    throw SnowpipeIngestException.FromHttp(res.StatusCode, errText);
                }

                if (res.Content.Headers.ContentLength == 0)
                    return default;

                var s = await SafeReadAsStringAsync(res, ct).ConfigureAwait(false);
                if (string.IsNullOrWhiteSpace(s))
                    return default;

                return JsonSerializer.Deserialize<TRes>(s, SerializerOptions);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested && attempt < options.MaxRetries)
            {
                attempt++;
                await Task.Delay(ComputeDelay(attempt, null), ct).ConfigureAwait(false);
            }
            catch (HttpRequestException) when (attempt < options.MaxRetries)
            {
                attempt++;
                await Task.Delay(ComputeDelay(attempt, null), ct).ConfigureAwait(false);
            }
            finally
            {
                res?.Dispose();
            }
        }
    }

    // -------------------- Routing helpers --------------------


    // ... rest of your methods unchanged EXCEPT:
    // - Use _http.BaseAddress instead of _baseUri (remove the field)
    // - ResolveBaseFor() can fall back to _http.BaseAddress
    private Uri ResolveBaseFor(string path)
    {
        if (IsBootstrapPath(path))
            return httpClient.BaseAddress!;

        if (auth is IIngestHostProvider ihp)
            return ihp.IngestBaseUri;

        return httpClient.BaseAddress!;
    }
    
    private static bool IsBootstrapPath(string path)
        => path.Equals("/v2/streaming/hostname", StringComparison.OrdinalIgnoreCase)
           || path.Equals("/oauth/token", StringComparison.OrdinalIgnoreCase);

    private static string NormalizePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path)) return "/";
        var p = path.Trim();
        if (!p.StartsWith("/")) p = "/" + p;
        if (p.Length > 1 && p.EndsWith("/")) p = p.TrimEnd('/');
        return p;
    }

    // -------------------- Retry helpers --------------------
    private static async Task<string> SafeReadAsStringAsync(HttpResponseMessage res, CancellationToken ct)
    {
        try
        {
#if NETSTANDARD2_0
            return await res.Content.ReadAsStringAsync().ConfigureAwait(false);
            #else
            return await res.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
#endif
        }
        catch
        {
            return string.Empty;
        }
    }

    private static bool ShouldRetry(HttpStatusCode code)
        => (int)code == 429 || (int)code == 425 || (int)code == 408 || (int)code >= 500;

    private TimeSpan ComputeDelay(int attempt, HttpResponseMessage? res)
    {
        if (res?.Headers.RetryAfter is { } ra)
        {
            if (ra.Delta is TimeSpan delta) return CapDelay(delta);
            if (ra.Date is DateTimeOffset when)
            {
                var dt = when - DateTimeOffset.UtcNow;
                if (dt > TimeSpan.Zero) return CapDelay(dt);
            }
        }

        var baseMs = options.RetryBackoffBase.TotalMilliseconds;
        var factor = Math.Pow(2, Math.Min(6, attempt - 1));
        var jitter = 0.85 + RandomHelper.NextDouble() * 0.30;
        return CapDelay(TimeSpan.FromMilliseconds(baseMs * factor * jitter));
    }

    private static TimeSpan CapDelay(TimeSpan t) => t > TimeSpan.FromSeconds(30) ? TimeSpan.FromSeconds(30) : t;
   
}