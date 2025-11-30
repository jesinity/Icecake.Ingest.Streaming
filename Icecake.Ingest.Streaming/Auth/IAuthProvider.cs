namespace Icecake.Ingest.Streaming.Auth;

/// <summary>
/// Represents an abstraction for providing authentication support for HTTP requests.
/// </summary>
public interface IAuthProvider
{
    /// <summary>
    /// Attaches authentication data to the specified HTTP request.
    /// </summary>
    /// <param name="request">The HTTP request message to which authentication information will be added.</param>
    /// <param name="ct">The cancellation token used to cancel the asynchronous operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task AttachAsync(HttpRequestMessage request, CancellationToken ct);
}