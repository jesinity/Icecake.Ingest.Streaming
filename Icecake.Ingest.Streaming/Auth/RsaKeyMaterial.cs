using System.Security.Cryptography;
using Icecake.Ingest.Streaming.Shims;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.X509;

namespace Icecake.Ingest.Streaming.Auth;

/// <summary>
/// Holds an RSA keypair for Snowflake key-pair auth.
/// - Keeps the RSA instance alive for the app lifetime (no IDisposable).
/// - Supports unencrypted PKCS#8 / PKCS#1 PEMs and encrypted PKCS#8 PEMs.
/// - Fingerprint is "SHA256:Base64" (with padding), matching Snowflake's KID format.
/// 
/// This implementation uses BouncyCastle to:
///  - Parse PEM (encrypted or not)
///  - Build the SubjectPublicKeyInfo (SPKI) for fingerprinting
/// </summary>
public sealed class RsaKeyMaterial
{
    private readonly RSA _rsa;

    /// <summary>
    /// Snowflake public key fingerprint (KID), format: "SHA256:&lt;base64 digest&gt;".
    /// Uses SHA-256 over the SubjectPublicKeyInfo (SPKI) bytes and keeps '=' padding.
    /// </summary>
    public string Fingerprint { get; }

    private RsaKeyMaterial(RSA rsa)
    {
        _rsa = rsa ?? throw new ArgumentNullException(nameof(rsa));
        Fingerprint = ComputeFingerprint(rsa);
    }

    /// <summary>
    /// Load from a PEM file on disk. Supports encrypted PKCS#8 (requires passphrase),
    /// unencrypted PKCS#8, and unencrypted PKCS#1.
    /// </summary>
    public static RsaKeyMaterial FromPemFile(string path, string? passphrase = null)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Path must not be null or empty.", nameof(path));

        var pem = File.ReadAllText(path);
        return FromPem(pem, passphrase);
    }

    /// <summary>
    /// Load from a PEM string. Supports encrypted PKCS#8 (requires passphrase),
    /// unencrypted PKCS#8, and unencrypted PKCS#1.
    /// </summary>
    public static RsaKeyMaterial FromPem(string pem, string? passphrase = null)
    {
        if (string.IsNullOrWhiteSpace(pem))
            throw new ArgumentException("PEM content must not be null or empty.", nameof(pem));

        pem = pem.Trim();

        // Use BouncyCastle PemReader for all variants. It handles:
        // - "BEGIN PRIVATE KEY"          (PKCS#8)
        // - "BEGIN RSA PRIVATE KEY"      (PKCS#1)
        // - "BEGIN ENCRYPTED PRIVATE KEY" (PKCS#8 encrypted)
        object? obj;
        using (var reader = new StringReader(pem))
        {
            PemReader pemReader;

            if (pem.ContainsIgnoreCase("ENCRYPTED PRIVATE KEY"))
            {
                if (string.IsNullOrEmpty(passphrase))
                    throw new ArgumentException("Passphrase is required for encrypted PKCS#8 keys.", nameof(passphrase));

                pemReader = new PemReader(reader, new SimplePasswordFinder(passphrase!));
            }
            else
            {
                // Unencrypted key: password finder is not needed
                pemReader = new PemReader(reader);
            }

            obj = pemReader.ReadObject();
        }

        if (obj == null)
            throw new InvalidOperationException("Failed to read an RSA private key from PEM.");

        AsymmetricKeyParameter privateKey;

        if (obj is AsymmetricCipherKeyPair pair)
        {
            privateKey = pair.Private;
        }
        else if (obj is AsymmetricKeyParameter akp && akp.IsPrivate)
        {
            privateKey = akp;
        }
        else
        {
            throw new InvalidOperationException("PEM does not contain a valid RSA private key.");
        }

        // Convert BouncyCastle RSA key to .NET RSA
        if (privateKey is not RsaPrivateCrtKeyParameters rsaParams)
            throw new InvalidOperationException("Expected RSA private key parameters.");

        var rsa = CreateRsaFromPrivateCrt(rsaParams);
        return new RsaKeyMaterial(rsa);
    }

    /// <summary>
    /// Sign arbitrary bytes with RSASSA-PKCS1-v1_5 using SHA-256 (RS256).
    /// </summary>
    public byte[] Sign(ReadOnlySpan<byte> data)
    {
        return _rsa.SignData(data.ToArray(), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
    }

    /// <summary>
    /// Compute Snowflake fingerprint: SHA-256 over the SubjectPublicKeyInfo (SPKI) DER bytes.
    /// Format: "SHA256:&lt;base64(spkiDigest)&gt;" (with padding).
    /// </summary>
    private static string ComputeFingerprint(RSA rsa)
    {
        // Convert .NET RSA -> BouncyCastle public key
        var bcPublicKey = DotNetUtilities.GetRsaPublicKey(rsa);

        // Build SPKI (SubjectPublicKeyInfo) ASN.1 structure
        SubjectPublicKeyInfo spki = SubjectPublicKeyInfoFactory.CreateSubjectPublicKeyInfo(bcPublicKey);
        byte[] spkiBytes = spki.GetEncoded(); // DER

        using var sha = SHA256.Create();
        byte[] digest = sha.ComputeHash(spkiBytes);

        // Snowflake expects "SHA256:" + base64 (WITH padding).
        return "SHA256:" + Convert.ToBase64String(digest);
    }

    /// <summary>
    /// BouncyCastle password finder for encrypted PEMs.
    /// </summary>
    private sealed class SimplePasswordFinder : IPasswordFinder
    {
        private readonly char[] _password;

        public SimplePasswordFinder(string password)
        {
            _password = password?.ToCharArray() ?? Array.Empty<char>();
        }

        public char[] GetPassword() => _password;
    }
    
    private static RSA CreateRsaFromPrivateCrt(RsaPrivateCrtKeyParameters p)
    {
        var parameters = new RSAParameters
        {
            Modulus  = p.Modulus.ToByteArrayUnsigned(),
            Exponent = p.PublicExponent.ToByteArrayUnsigned(),
            D        = p.Exponent.ToByteArrayUnsigned(),
            P        = p.P.ToByteArrayUnsigned(),
            Q        = p.Q.ToByteArrayUnsigned(),
            DP       = p.DP.ToByteArrayUnsigned(),
            DQ       = p.DQ.ToByteArrayUnsigned(),
            InverseQ = p.QInv.ToByteArrayUnsigned()
        };

        var rsa = RSA.Create();
        rsa.ImportParameters(parameters);
        return rsa;
    }
}