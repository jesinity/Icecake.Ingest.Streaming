namespace Icecake.Ingest.Streaming.Auth;

/// <summary>
/// Defines the methods and properties required to provide the host information
/// for an ingestion service and ensure it is in a ready state for use.
/// </summary>
public interface IIngestHostProvider
{
    /// <summary>
    /// Gets the base URI for the ingest service. This property provides the resolved
    /// hostname required to interact with the ingest service's endpoints. It throws
    /// an <see cref="InvalidOperationException"/> if the hostname has not been
    /// initialized or discovered. The value is intended to be used by components
    /// that require a URL base to construct service-specific routes.
    /// </summary>
    Uri IngestBaseUri { get; }

    /// <summary>
    /// Indicates whether the ingest service is ready to be used. A value of <c>true</c> signifies
    /// that the required host information and authentication token have been initialized and are valid.
    /// The property returns <c>false</c> if the host information is unavailable, the authentication
    /// token has not been generated, or the token has expired beyond the allowed time skew.
    /// </summary>
    bool IsReady { get; }

    /// <summary>
    /// Ensures that the ingest host provider is in a ready state for use.
    /// </summary>
    /// <param name="ct">A cancellation token to observe while waiting for readiness.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task EnsureReadyAsync(CancellationToken ct);
}