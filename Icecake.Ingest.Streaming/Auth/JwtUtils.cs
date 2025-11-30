using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Icecake.Ingest.Streaming.Shims;

namespace Icecake.Ingest.Streaming.Auth;

/// <summary>
/// Provides utility methods for creating JSON Web Tokens (JWT) and handling related operations.
/// </summary>
public static class JwtUtils
{
    /// <summary>
    /// Creates a signed JSON Web Token (JWT) using the provided RSA key material and claims.
    /// </summary>
    /// <param name="key">The RSA key material used to sign the JWT.</param>
    /// <param name="claims">The claims to include in the token payload, such as subject, issuer, audience, and extra fields.</param>
    /// <returns>A signed JWT as a string.</returns>
    public static string CreateSignedJwt(RsaKeyMaterial key, JwtClaims claims)
    {
        var header = new Dictionary<string, object> { ["alg"] = "RS256", ["typ"] = "JWT", ["kid"] = key.Fingerprint };
        var payload = new Dictionary<string, object> { ["sub"] = claims.Subject, ["iss"] = claims.Issuer, ["aud"] = claims.Audience, ["iat"] = claims.IssuedAt.ToUnixTimeSeconds(), ["exp"] = claims.ExpiresAt.ToUnixTimeSeconds() };
        if (claims.Extra is not null) foreach (var kv in claims.Extra) payload[kv.Key] = kv.Value;
        var b64Header = SanitizedBase64Url(JsonSerializer.SerializeToUtf8Bytes(header, JsonHelper.DefaultCamelCase));
        var b64Payload = SanitizedBase64Url(JsonSerializer.SerializeToUtf8Bytes(payload, JsonHelper.DefaultCamelCase));
        var signingInput = Encoding.ASCII.GetBytes(b64Header + "." + b64Payload);
        var sig = key.Sign(signingInput);
        return string.Concat(b64Header, ".", b64Payload, ".", SanitizedBase64Url(sig));
    }
    private static string SanitizedBase64Url(byte[] bytes) => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
}