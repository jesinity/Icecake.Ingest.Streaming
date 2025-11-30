namespace Icecake.Ingest.Streaming.Auth;

/// <summary>
/// Represents the claims data used for creating a JWT (JSON Web Token).
/// </summary>
/// <remarks>
/// This immutable record structure encapsulates necessary information for describing
/// a JWT including mandatory claims such as Subject, Issuer, Audience, IssuedAt, and ExpiresAt.
/// Additionally, it allows for inclusion of extra custom claims through the Extra dictionary.
/// </remarks>
/// <param name="Subject">The principal that is the subject of the JWT.</param>
/// <param name="Issuer">The entity that issued the JWT.</param>
/// <param name="Audience">The audience or recipient for whom the JWT is intended.</param>
/// <param name="IssuedAt">The timestamp indicating the time the JWT was issued.</param>
/// <param name="ExpiresAt">The timestamp indicating the time at which the JWT will expire.</param>
/// <param name="Extra">An optional dictionary containing additional custom claims.</param>
public sealed record JwtClaims(
    string Subject,
    string Issuer,
    string Audience,
    DateTimeOffset IssuedAt,
    DateTimeOffset ExpiresAt,
    IReadOnlyDictionary<string, object>? Extra = null);