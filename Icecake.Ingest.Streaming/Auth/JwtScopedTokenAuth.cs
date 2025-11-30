using Icecake.Ingest.Streaming.Connect;
using Icecake.Ingest.Streaming.Core;
using Icecake.Ingest.Streaming.Shims;
using Microsoft.Extensions.Logging;

namespace Icecake.Ingest.Streaming.Auth;

using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

/// <summary>
/// Represents an implementation of authentication and ingest host provider using JWT scoped tokens.
/// </summary>
/// <remarks>
/// This class is designed to handle authentication for streaming ingest services. It supports token-based authentication
/// and provides mechanisms to attach tokens to outgoing HTTP requests as well as managing the lifecycle of tokens.
/// </remarks>
public sealed class JwtScopedTokenAuth : IAuthProvider, IIngestHostProvider
{
    private readonly RsaKeyMaterial _key;
    private readonly string _accountIdentifier;
    private readonly string _user;
    private readonly Uri _accountBaseUri;

    private readonly HttpClient _http;
    private readonly string _userAgent;

    private string? _accessToken;
    private DateTimeOffset _expiresAt = DateTimeOffset.MinValue;
    private Uri? _ingestBase;
    private readonly ILogger<JwtScopedTokenAuth> _logger;

    private static readonly TimeSpan Skew = TimeSpan.FromMinutes(1);

    public JwtScopedTokenAuth(
        AccountOptions accountOptions,
        CredentialsOptions credentials,
        SnowflakeClientOptions clientOptions,
        RsaKeyMaterial key,
        HttpClient httpClient,
        ILogger<JwtScopedTokenAuth> logger)
    {
        _key = key;
        _accountIdentifier = accountOptions.AccountId;
        _user = credentials.User;
        _accountBaseUri = accountOptions.BaseUri;
        _userAgent = clientOptions.UserAgent;

        _http = httpClient;
        _http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        _http.DefaultRequestHeaders.UserAgent.ParseAdd(_userAgent);
        _http.Timeout = TimeSpan.FromSeconds(100);
        _logger = logger;
    }

    public Uri IngestBaseUri
        => _ingestBase ?? throw new InvalidOperationException("Ingest hostname not discovered yet. AttachAsync must run at least once.");

    public async Task AttachAsync(HttpRequestMessage request, CancellationToken ct)
    {
        await EnsureTokenAsync(ct).ConfigureAwait(false);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
    }
    
    public async Task EnsureReadyAsync(CancellationToken ct)
    {
        // This just funnels EnsureTokenAsync
        await EnsureTokenAsync(ct).ConfigureAwait(false);
    }

    private async Task EnsureTokenAsync(CancellationToken ct)
    {
        var needToken = _accessToken is null || DateTimeOffset.UtcNow >= _expiresAt - Skew || _ingestBase is null;
        if (!needToken) return;

        // ---- Build Snowflake-compliant iss/sub ----
        // Account identifier used in URLs can contain dots; Snowflake requires dots => dashes and UPPER in iss/sub.
        var acctDashedUpper = _accountIdentifier.Replace('.', '-').ToUpperInvariant();
        var userUpper = _user.ToUpperInvariant();

        // Fingerprint must match DESC USER RSA_PUBLIC_KEY_FP exactly (including '=' padding if shown there).
        var fp = _key.Fingerprint; // e.g., "SHA256:...="

        var iss = $"{acctDashedUpper}.{userUpper}.{fp}";
        var sub = $"{acctDashedUpper}.{userUpper}";
        var aud = _accountBaseUri.GetLeftPart(UriPartial.Authority);

        // add some skew in the date time offset issued at
        var now = DateTimeOffset.UtcNow;
        var iat = now.AddSeconds(-30);
        var exp = now.AddMinutes(9);

        var claims = new JwtClaims(
            Subject: sub,
            Issuer: iss,
            Audience: aud,
            IssuedAt: iat,
            ExpiresAt: exp
        );

        var jwt = JwtUtils.CreateSignedJwt(_key, claims);

        // Diagnostics
        _logger.LogInformation("JWT => sub={sub}, iss={iss}, aud={aud}, iat={iat}, exp={exp}", sub, iss, aud, iat, exp);
        _logger.LogInformation("JWT header kid: {fp}", fp);

        // ---- 1) Discover ingest hostname (v2) ----
        var hostUrl = new Uri(_accountBaseUri, "/v2/streaming/hostname");
        using (var hostReq = new HttpRequestMessage(HttpMethod.Get, hostUrl))
        {
            hostReq.Headers.UserAgent.ParseAdd(_userAgent);
            hostReq.Headers.Accept.Clear();
            hostReq.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            // Required: declare the Bearer value is a keypair JWT
            hostReq.Headers.TryAddWithoutValidation("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT");

            // Set Authorization explicitly (some handlers/proxies are picky)
            hostReq.Headers.Remove("Authorization");
            hostReq.Headers.TryAddWithoutValidation("Authorization", $"Bearer {jwt}");

            using var hostRes = await _http.SendAsync(hostReq, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
#if NETSTANDARD2_0
            var hostText = await hostRes.Content.ReadAsStringAsync().ConfigureAwait(false);
#else
            var hostText = await hostRes.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
#endif
            var contentType = hostRes.Content.Headers.ContentType?.MediaType ?? "";

            if (!hostRes.IsSuccessStatusCode)
                throw new InvalidOperationException($"Ingest hostname discovery failed: {(int)hostRes.StatusCode} {hostRes.StatusCode}: {hostText}");

            var ingestHost = TryParseIngestHostname(hostText, contentType);
            if (string.IsNullOrWhiteSpace(ingestHost))
                throw new InvalidOperationException(
                    $"Could not determine ingest hostname. Content-Type='{contentType}', Body='{(hostText.Length > 200 ? hostText.Substring(0,200) + "..." : hostText)}'");

            _ingestBase = ingestHost!.StartsWith("http", StringComparison.OrdinalIgnoreCase)
                ? new Uri(ingestHost)
                : new Uri($"https://{ingestHost}");
        }

        // ---- 2) OAuth token exchange (JWT-bearer grant) ----
        var oauthUrl = new Uri(_accountBaseUri, "/oauth/token");
        var form = $"grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&scope={Uri.EscapeDataString(_ingestBase.Host)}";

        using var tokReq = new HttpRequestMessage(HttpMethod.Post, oauthUrl);
        tokReq.Content = new StringContent(form, Encoding.UTF8, "application/x-www-form-urlencoded");
        tokReq.Headers.Authorization = new AuthenticationHeaderValue("Bearer", jwt);

        tokReq.Headers.Accept.Clear();
        tokReq.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        tokReq.Headers.TryAddWithoutValidation("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT");
        tokReq.Headers.UserAgent.ParseAdd(_userAgent);

        using var tokRes = await _http.SendAsync(tokReq, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
#if NETSTANDARD2_0
        var tokText = await tokRes.Content.ReadAsStringAsync().ConfigureAwait(false);
#else
        var tokText = await tokRes.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
#endif
        var tokCt = tokRes.Content.Headers.ContentType?.MediaType ?? "";

        if (!tokRes.IsSuccessStatusCode)
            throw new InvalidOperationException($"OAuth token exchange failed: {(int)tokRes.StatusCode} {tokRes.StatusCode}: {tokText}");

        if (!TryParseAccessToken(tokText, tokCt, out var accessToken, out var expiresIn))
        {
            var output = tokText.Length > 200 ? tokText.Substring(0,200) + "..." : tokText;
            throw new InvalidOperationException($"OAuth token parse failed. Content-Type='{tokCt}', Body starts='{output}'");
        }
            

        _accessToken = accessToken!;
        _expiresAt = DateTimeOffset.UtcNow.AddSeconds(expiresIn ?? 3600);
    }

    private static string? TryParseIngestHostname(string body, string contentType)
    {
        // 1) JSON object/string
        if (!string.IsNullOrWhiteSpace(contentType) && contentType.ContainsIgnoreCase("json"))
        {
            try
            {
                using var doc = JsonDocument.Parse(string.IsNullOrWhiteSpace(body) ? "{}" : body);
                var root = doc.RootElement;
                if (root.ValueKind == JsonValueKind.Object)
                {
                    if (root.TryGetProperty("ingestHostname", out var h1)) return h1.GetString();
                    if (root.TryGetProperty("hostname", out var h2)) return h2.GetString();
                    if (root.TryGetProperty("data", out var d) && d.ValueKind == JsonValueKind.Object &&
                        d.TryGetProperty("ingestHostname", out var h3)) return h3.GetString();
                }

                if (root.ValueKind == JsonValueKind.String)
                {
                    var s = root.GetString();
                    if (!string.IsNullOrWhiteSpace(s)) return s;
                }
            }
            catch
            {
                /* falls thorugh to text!!! */
            }
        }

        // 2) JSON string without object wrapper (content-type wrong)
        var trimmed = (body).Trim();
        if (trimmed.StartsWith("\"") && trimmed.EndsWith("\""))
        {
            try
            {
                return JsonSerializer.Deserialize<string>(trimmed);
            }
            catch
            {
                /* ignore */
            }
        }

        // 3) Plain text: pick the first token that looks like a Snowflake hostname
        if (trimmed.ContainsIgnoreCase("snowflakecomputing.com"))
        {
            var parts = trimmed.Split(new[] { ' ', '\r', '\n', '\t', ',', ';' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var p in parts)
            {
                var q = p.Trim('\"', '\'', '.', '(', ')');
                if (q.ContainsIgnoreCase("snowflakecomputing.com"))
                    return q;
            }
        }

        // 4) Last resort: if it looks hostname-like, return as-is
        if (trimmed.Length > 0 && trimmed.IndexOf(' ') < 0 && trimmed.Contains('.'))
            return trimmed;

        return null;
    }

    private static bool TryParseAccessToken(string? body, string contentType, out string? accessToken, out int? expiresIn)
    {
        accessToken = null;
        expiresIn = null;

        var b = body?.Trim() ?? string.Empty;
        if (b.Length == 0) return false;

        if (LooksLikeJwt(b))
        {
            accessToken = b;
            // If we want, we can read exp from the JWT to set expiresIn more precisely; default to 3600s for now.
            expiresIn = 3600;
            return true;
        }

        // 1) Try JSON if content-type hints or body looks JSON
        if (contentType.ContainsIgnoreCase("json") || b.StartsWith("{") || b.StartsWith("\""))
        {
            try
            {
                using var doc = JsonDocument.Parse(string.IsNullOrWhiteSpace(b) ? "{}" : b);
                var root = doc.RootElement;
                accessToken = root.TryGetProperty("access_token", out var at) ? at.GetString() : null;
                if (root.TryGetProperty("expires_in", out var ei) && ei.TryGetInt32(out var secs))
                    expiresIn = secs;
                if (!string.IsNullOrEmpty(accessToken)) return true;
            }
            catch
            {
                /* fall through */
            }
        }

        // 2) Fallback: form-urlencoded key=value&...
        if (b.Contains('='))
        {
            string? at = null;
            int? ei = null;
            foreach (var pair in b.Split(new[] { '&' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var kv = pair.Split(new[] { '=' }, 2, StringSplitOptions.None);
                if (kv.Length != 2) continue;

                var key = Uri.UnescapeDataString(kv[0]);
                var val = Uri.UnescapeDataString(kv[1]);

                if (key.Equals("access_token", StringComparison.OrdinalIgnoreCase))
                    at = val;
                else if (key.Equals("expires_in", StringComparison.OrdinalIgnoreCase) &&
                         int.TryParse(val, out var secs))
                    ei = secs;
            }
            
            if (!string.IsNullOrEmpty(at))
            {
                accessToken = at;
                expiresIn = ei ?? 3600;
                return true;
            }
        }

        return false;
    }

    // 0) If the body itself looks like a JWT (compact JWS: header.payload.signature), accept it directly.
    // Some Snowflake deployments return the access token as a raw JWT string (even with application/json CT).
    // We accept both 2-part and 3-part tokens (some proxies can strip signature in rare cases).
    private static bool LooksLikeJwt(string s)
    {
        // quick-and-safe check for base64url segments separated by dots
        var parts = s.Split('.');
        if (parts.Length is < 2 or > 3) return false;
        foreach (var p in parts)
        {
            if (p.Length == 0) return false;
            foreach (var c in p)
            {
                var ok = (c >= 'A' && c <= 'Z') ||
                         (c >= 'a' && c <= 'z') ||
                         (c >= '0' && c <= '9') ||
                         c == '-' || c == '_';
                if (!ok) return false;
            }
        }

        return true;
    }

    public bool IsReady => _ingestBase is not null && _accessToken is not null && DateTimeOffset.UtcNow < _expiresAt - Skew;

}