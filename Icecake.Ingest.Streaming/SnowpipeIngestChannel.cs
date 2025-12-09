using System.Text;
using System.Text.Json;
using Icecake.Ingest.Streaming.Core;
using Icecake.Ingest.Streaming.Models;
using Icecake.Ingest.Streaming.Shims;
using Microsoft.Extensions.Logging;

namespace Icecake.Ingest.Streaming;

/// <summary>
/// Represents a channel for interacting with Snowpipe for streaming data ingestion.
/// This class manages the lifecycle and operations of a Snowpipe channel, including data insertion,
/// offset management, and flushing data to the associated Snowflake table.
/// It works against the V2 high-performance api.
/// </summary>
public sealed class SnowpipeIngestChannel : IAsyncDisposable, IDisposable
{
    private static readonly JsonSerializerOptions RowJson = JsonHelper.DefaultIgnore;

    private readonly string _channelName;
    private readonly string _pipeName;
    private readonly TableSchema _tableSchema;
    private readonly SnowpipeIngestClient _client;
    private readonly FlushPolicy _policy;

    // Ensures only one FlushAsync runs at a time, even if multiple callers
    // (timers, InsertRow size trigger, Dispose/Drop) invoke it concurrently.
    // This protects the continuation token / offset handling and avoids
    // overlapping network calls for the same Snowpipe channel.
    private readonly SemaphoreSlim _flushGate = new(1, 1);
    private readonly Timer _healthTimer;
    private readonly TimeSpan _statusInterval = TimeSpan.FromMinutes(5);
    private readonly TimeSpan _reopenBackoff = TimeSpan.FromSeconds(2);
    private string? _pendingOffsetToken;

    private readonly object _lock = new();

    private List<Dictionary<string, object?>> _buffer = [];
    private List<Dictionary<string, object?>>? _spareBuffer;

    private readonly Timer _flushTimer;
    
    private const int DefaultFetchTimeoutSeconds = 10;
    private const int DefaultFetchPollMilliseconds = 500;

    private int _estimatedBytes;

    // Server-managed token, that is seeded when the channel gets opened
    // and advanced on appending records.
    // Used to track offsets ingested records, and required by the API 
    private string? _continuationToken;

    // Optional client side offset, used to track ingested record client side.
    // This is used to resume ingestion from a specific offset token in stateful ingestion 
    // scenarios, where the client tracks the offset token on the go.
    private string? _latestCommittedOffsetToken;
    public string? LatestCommittedOffsetToken => _latestCommittedOffsetToken;

    private ChannelState _state = ChannelState.Created;
    public ChannelState State => _state;
    
    private DateTimeOffset _lastAppendUtc = DateTimeOffset.MinValue;
    
    private readonly ILogger<SnowpipeIngestChannel> _logger;
    private readonly SchemaObjectCoords _pipeCoords;
    private bool _disposed;
    private readonly PayloadBuilder _builder;

    /// <summary>
    /// Represents a channel used for streaming ingestion of data into a system.
    /// </summary>
    public SnowpipeIngestChannel(
        string channelName,
        string pipeName,
        TableSchema tableSchema,
        SnowpipeIngestClient client,
        FlushPolicy policy,
        ILogger<SnowpipeIngestChannel> logger
    )
    {
        _channelName = channelName;
        _pipeName = pipeName;
        _client = client;
        _tableSchema = tableSchema;
        _policy = policy;
        _logger = logger;
        _builder = new PayloadBuilder(_tableSchema, _policy.MaxBytes);
        
        _pipeCoords = new SchemaObjectCoords
        {
            Database = _tableSchema.SchemaObject.Database,
            Schema = _tableSchema.SchemaObject.Schema,
            Name = _pipeName
        };
        // periodically checks that the channel is healthy
        _healthTimer = new Timer(async void (_) =>
        {
            try
            {
                await CheckHealthAsync().ConfigureAwait(false);
            }
            catch
            {
                /* swallow exception, next tick will retry */
            }
        }, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

        // periodic flush
        _flushTimer = new Timer(async void (_) =>
        {
            try
            {
                await FlushAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _state = ChannelState.Error;
                _logger.LogError(ex, "Periodic flush failed.");
            }
        }, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    /// <summary>
    /// Opens the streaming ingestion channel.
    /// </summary>
    /// <param name="ct">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the channel cannot be opened due to an error,
    /// such as invalid channel status or missing required tokens in the response.
    /// </exception>
    public async Task OpenAsync(CancellationToken ct = default)
    {
        if (_state == ChannelState.Open) return;
        _state = ChannelState.Opening;
        _disposed = false;

        var resp = await _client.OpenChannelAsync(new Models.OpenChannelRequest
        {
            Database = _tableSchema.SchemaObject.Database,
            Schema = _tableSchema.SchemaObject.Schema,
            Pipe = _pipeName,
            Channel = _channelName
        }, ct).ConfigureAwait(false);

        var cs = resp.ChannelStatus;
        if (!string.Equals(cs.ChannelStatusCode, "SUCCESS", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"Channel open failed: code={cs.ChannelStatusCode}, err={cs.LastErrorMessage}");

        _latestCommittedOffsetToken = cs.LastCommittedOffsetToken ?? _latestCommittedOffsetToken;

        // v2 API feature, the continuation token must be seeded when opening the channel
        // and advanced on each append.
        _continuationToken = resp.NextContinuationToken
                             ?? throw new InvalidOperationException("Missing next_continuation_token in open response.");

        _state = ChannelState.Open;
        // starting the timers
        _flushTimer.Change(_policy.Interval, _policy.Interval);
        _healthTimer.Change(_statusInterval, _statusInterval);
    }

    /// <summary>
    /// Asynchronously drops the ingestion channel, releasing any resources and transitioning
    /// the channel state to Dropped. Ensures any remaining uncommitted data batches are processed
    /// before completing the operation.
    /// </summary>
    /// <param name="ct">A cancellation token to observe while waiting for the operation to complete.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">If the drop operation fails due to an error returned by the system.</exception>
    public async Task DropAsync(CancellationToken ct = default)
    {
        if (_state is ChannelState.Dropped or ChannelState.Closed) return;

        // Give Snowflake time to commit the last micro-batch
        var since = DateTimeOffset.UtcNow - _lastAppendUtc;
        var wait = _policy.MinHoldAfterAppend - since;
        if (wait > TimeSpan.Zero)
            await Task.Delay(wait, ct).ConfigureAwait(false);
        
        var resp = await _client.DeleteChannelAsync(_pipeCoords, _channelName, ct)
            .ConfigureAwait(false);

        if (resp?.ChannelStatus is { } cs &&
            !string.Equals(cs.ChannelStatusCode, "SUCCESS", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"Drop channel failed: code={cs.ChannelStatusCode}, err={cs.LastErrorMessage}");

        _state = ChannelState.Dropped;
        _flushTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        _healthTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    /// <summary>
    /// Inserts a single row of data into the channel's buffer for ingestion.
    /// </summary>
    /// <param name="row">A dictionary representing the row data, where the key is the column name and the value is the corresponding data.</param>
    public void InsertRow(Dictionary<string, object?> row)
    {
        lock (_lock)
        {
            EnsureOpen();
            _buffer.Add(row);
            _estimatedBytes += EstimateRowBytes(row);
            if (_buffer.Count >= _policy.MaxRows || _estimatedBytes >= _policy.MaxBytes)
                _ = FlushAsync();
        }
    }

    /// <summary>
    /// Adds multiple rows of data to the ingestion buffer for streaming into a Snowflake table.
    /// If the buffer exceeds the maximum row count or size defined by the flush policy,
    /// the buffered data will be automatically flushed.
    /// </summary>
    /// <param name="rows">A collection of rows to insert, where each row is represented as a dictionary of column names and their corresponding values.</param>
    public void InsertRows(IEnumerable<Dictionary<string, object?>> rows)
    {
        lock (_lock)
        {
            EnsureOpen();
            foreach (var r in rows)
            {
                _buffer.Add(r);
                _estimatedBytes += EstimateRowBytes(r);
            }

            if (_buffer.Count >= _policy.MaxRows || _estimatedBytes >= _policy.MaxBytes)
                _ = FlushAsync();
        }
    }

    /// <summary>
    /// Sets the offset token for the next flush operation.
    /// </summary>
    /// <param name="token">
    /// The offset token to be assigned. This token is associated with the next flush operation
    /// and used to track the progress of data ingestion on the client side.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Thrown when the provided token is null, empty, or consists only of whitespace.
    /// </exception>
    public void SetOffsetTokenForNextFlush(string token)
    {
        if (string.IsNullOrWhiteSpace(token))
            throw new ArgumentException("offset token cannot be empty", nameof(token));
        _pendingOffsetToken = token;
    }


/// <summary>
    /// Fetches the latest committed offset for the channel, with a configurable timeout and polling interval.
    /// It may take some time before the offset token gets committed and visible.
    /// Uses Snowflake's average processing latency to back off polling when the system is under load.
    /// </summary>
    /// <param name="timeOutSeconds">
    /// The maximum time to wait in seconds before the operation times out if no offset is fetched.
    /// </param>
    /// <param name="pollMilliseconds">
    /// The base polling interval in milliseconds. When Snowflake reports high processing latency,
    /// the effective polling interval is increased to avoid hammering the status endpoint.
    /// </param>
    /// <param name="ct">A cancellation token to observe while waiting for the operation to complete.</param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains the latest committed
    /// offset if available; otherwise, returns null if the operation times out.
    /// </returns>
    public async Task<string?> FetchLatestCommittedOffsetAsync(
        int timeOutSeconds = DefaultFetchTimeoutSeconds,
        int pollMilliseconds = DefaultFetchPollMilliseconds,
        CancellationToken ct = default)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        string? last = null;

        var timeout = TimeSpan.FromSeconds(timeOutSeconds);
        var basePollInterval = TimeSpan.FromMilliseconds(pollMilliseconds);

        while (sw.Elapsed < timeout)
        {
            var status = await GetStatusAsync(ct).ConfigureAwait(false);

            last = status.LastCommittedOffsetToken;
            if (!string.IsNullOrEmpty(last))
                return last;

            // --- Adaptive polling based on SnowflakeAvgProcessingLatencyMs --------------------
            //
            // Idea:
            //   • Never poll faster than basePollInterval (e.g. 250 ms).
            //   • If Snowflake reports a high average processing latency, we back off.
            //   • We use roughly latency / 4 as an upper bound for how often we re-check,
            //     so an 8s processing latency becomes ~2s polling instead of 250ms spam.
            //
            TimeSpan nextDelay = basePollInterval;

            var avgLatencyMs = status.SnowflakeAvgProcessingLatencyMs;
            if (avgLatencyMs.HasValue && avgLatencyMs.Value > 0)
            {
                // start from a fraction of the processing latency
                var adaptiveMs = avgLatencyMs.Value / 4.0;

                // never go below the base poll interval
                if (adaptiveMs < basePollInterval.TotalMilliseconds)
                    adaptiveMs = basePollInterval.TotalMilliseconds;

                // also don't make a single delay so big that we obviously overshoot the timeout
                // (cap at, say, 1/2 of remaining time)
                var remaining = timeout - sw.Elapsed;
                var maxMs = remaining.TotalMilliseconds / 2.0;
                if (adaptiveMs > maxMs && maxMs > 0)
                    adaptiveMs = maxMs;

                nextDelay = TimeSpan.FromMilliseconds(adaptiveMs);
            }

            // Final safety: if we’re basically out of time, break instead of negative/zero delays
            var remainingTime = timeout - sw.Elapsed;
            if (remainingTime <= TimeSpan.Zero)
                break;

            if (nextDelay > remainingTime)
                nextDelay = remainingTime;

            await Task.Delay(nextDelay, ct).ConfigureAwait(false);
        }

        // may still be null if it timed out
        return last;
    }

    /// <summary>
    /// Retrieves the current status of the ingestion channel, including any relevant state information.
    /// </summary>
    /// <param name="ct">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the current <see cref="ChannelStatus"/> of the channel.</returns>
    public async Task<ChannelStatus> GetStatusAsync(CancellationToken ct = default)
    {
        return await _client.GetChannelStatusAsync(
            database: _tableSchema.SchemaObject.Database,
            schema: _tableSchema.SchemaObject.Schema,
            pipe: _pipeName,
            channel: _channelName,
            ct: ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Flushes the current batch of data to the ingestion channel asynchronously.
    /// Ensures the channel is open and processes the data to be sent, handling any applicable exceptions.
    /// </summary>
    /// <param name="offsetToken">An optional offset token that specifies the position of the data in the stream to be used.</param>
    /// <param name="ct">A <see cref="CancellationToken"/> used to observe cancellation requests.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the continuation token is not initialized or the channel is in an invalid state.</exception>
    public async Task FlushAsync(string? offsetToken = null, CancellationToken ct = default)
    {
        using var _ = WithLoggingScope();
        EnsureOpen();

        List<Dictionary<string, object?>> batch;
        
        lock (_lock)
        {
            if (_buffer.Count == 0) return;
            
            // ┌────────────────────────────────────────────────────────────┐
            // │ Double-buffer trick:                                       │
            // │                                                             │
            // │ 1. We  swap the producer buffer (_buffer) with a fresh    │
            // │    list (or a recycled one).                               │
            // │ 2. The old list becomes "batch", a private snapshot for    │
            // │    this flush operation.                                   │
            // │                                                             │
            // │ Why do we do this?                                         │
            // │ -------------------                                         │
            // │ • Flushing involves slow operations (JSON serialization,   │
            // │   network I/O). We must not hold the lock during that.     │
            // │                                                             │
            // │ • If we flushed the same list producers are writing to,    │
            // │   we would have concurrent read/write on List<T>, which    │
            // │   is NOT thread-safe and would corrupt the batch.          │
            // │                                                             │
            // │ • By swapping lists, producers immediately get an empty    │
            // │   list to continue writing into, while we safely flush the │
            // │   old one without holding the lock.                        │
            // │                                                             │
            // │ • _spareBuffer lets us reuse a cleared list to reduce GC   │
            // │   pressure ( micro-optimization).                          │
            // └────────────────────────────────────────────────────────────┘
            batch = _buffer; 
            _buffer = _spareBuffer ?? new List<Dictionary<string, object?>>(batch.Count);
            _spareBuffer = null;
            _estimatedBytes = 0;
        }

        // Serialize flushes: avoid overlapping appends for this channel.
        await _flushGate.WaitAsync(ct).ConfigureAwait(false);

        var chunk = _builder.Build(_channelName, batch, offsetToken);

        var token = _continuationToken
                    ?? throw new InvalidOperationException("Continuation token not initialized. Did OpenAsync succeed?");

        // Resolve which offset token to send for THIS append.
        // It may still be null after the assignment.
        var effectiveOffset = offsetToken ?? _pendingOffsetToken;

        try
        {
            var resp = await _client.AppendRowsAsync(
                _pipeCoords,
                channel: _channelName,
                ndjson: chunk.Data,
                continuationToken: token,
                offsetToken: effectiveOffset,
                ct: ct).ConfigureAwait(false);

            _continuationToken = resp.NextContinuationToken;
            _lastAppendUtc = DateTimeOffset.UtcNow;

            // clear the pending token only if we used it successfully
            if (effectiveOffset is not null && offsetToken is null)
                _pendingOffsetToken = null;
        }
        catch (SnowpipeIngestException ex) when ((int)ex.StatusCode == 400 || (int)ex.StatusCode == 409)
        {
            // One-time reopen and retry if continuation token drifted
            await Task.Delay(TimeSpan.FromSeconds(3), ct).ConfigureAwait(false);
            await OpenAsync(ct).ConfigureAwait(false);

            var retryToken = _continuationToken
                             ?? throw new InvalidOperationException("Continuation token missing after reopen.");

            var retryResponse = await _client.AppendRowsAsync(
                _pipeCoords,
                _channelName,
                chunk.Data,
                retryToken,
                effectiveOffset,
                ct).ConfigureAwait(false);

            _continuationToken = retryResponse.NextContinuationToken;
            _lastAppendUtc = DateTimeOffset.UtcNow;

            if (effectiveOffset is not null && offsetToken is null)
                _pendingOffsetToken = null;
        }
        finally
        {
            // ┌──────────────────────────────────────────────────┐
            // │ After flushing, we clear the batch and store it  │
            // │ in _spareBuffer so it can be reused on the next  │
            // │ swap — avoids allocating a new large List<T>.    │
            // │                                                  │
            // │ NOTE: This does NOT leak rows because 'batch' is │
            // │ never touched by producers; it's a private copy. │
            // └──────────────────────────────────────────────────┘
            batch.Clear();
            _spareBuffer ??= batch;
            _flushGate.Release();
        }
    }

    private void EnsureOpen()
    {
        if (_state != ChannelState.Open)
            throw new InvalidOperationException($"Channel '{_channelName}' not open (state={_state})");
    }

    /// <inheritdoc cref="IAsyncDisposable"/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        using var _ = WithLoggingScope();
        _logger.LogInformation("Disposing channel (async): flushing…");

        _flushTimer.Dispose();
        _healthTimer.Dispose();

        try
        {
            await FlushAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "FlushAsync failed during DisposeAsync.");
        }

        _state = ChannelState.Closed;
    }

    /// <inheritdoc cref="IDisposable"/>
    public void Dispose()
    {
        if (_disposed) return;
        // Run the async cleanup synchronously:
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }


    /// <summary>
    /// Checks the health of the streaming ingest channel and updates its state as necessary.
    /// Ensures the channel is healthy and operational by retrieving its current status
    /// and attempting to recover if it is in an error state or closed server-side.
    /// </summary>
    /// <param name="ct">
    /// The cancellation token that can be used to signal the cancellation of this asynchronous operation.
    /// </param>
    /// <returns>
    /// A task that represents the asynchronous operation of checking the channel's health.
    /// The task completes once the health check is performed and the channel state is updated accordingly.
    /// </returns>
    /// <exception cref="OperationCanceledException">
    /// Thrown if the operation is canceled through the provided cancellation token.
    /// </exception>
    private async Task CheckHealthAsync(CancellationToken ct = default)
    {
        using var _ = WithLoggingScope();

        if (_state != ChannelState.Open)
        {
            return;
        }

        var status = await GetStatusAsync(ct).ConfigureAwait(false);

        // refresh the latest committed offset for external resume
        if (!string.IsNullOrEmpty(status.LastCommittedOffsetToken))
        {
            _latestCommittedOffsetToken = status.LastCommittedOffsetToken;
            _logger.LogInformation("Latest committed offset token: {token}", _latestCommittedOffsetToken);
        }

        // if a channel went unhealthy/closed server-side, attempt a transparent reopen
        if (!string.Equals(status.ChannelStatusCode, "SUCCESS", StringComparison.OrdinalIgnoreCase))
        {
            _state = ChannelState.Error;
            await Task.Delay(_reopenBackoff, ct).ConfigureAwait(false);
            await OpenAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Estimates the size, in bytes, of a serialized row represented as a dictionary.
    /// </summary>
    /// <param name="r">The row to estimate the size for, represented as a dictionary of column names and their respective values.</param>
    /// <returns>The estimated size in bytes of the serialized row.</returns>
    private int EstimateRowBytes(Dictionary<string, object?> r)
    {
        var normalized = new Dictionary<string, object?>();
        foreach (var kv in r)
        {
            if (_tableSchema.ColumnsByName.TryGetValue(kv.Key, out var spec))
                normalized[kv.Key] = ColumnValueConverter.ConvertInternal(spec, kv.Value);
            else
                normalized[kv.Key] = kv.Value;
        }

        var json = JsonSerializer.Serialize(normalized, RowJson);
        return Encoding.UTF8.GetByteCount(json) + 1; // + newline
    }

    private IDisposable? WithLoggingScope() =>
        _logger.BeginScope(new Dictionary<string, object>
        {
            ["database"] = _tableSchema.SchemaObject.Database,
            ["schema"] = _tableSchema.SchemaObject.Schema,
            ["pipe"] = _pipeName,
            ["channel"] = _channelName
        });
}