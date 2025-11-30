namespace Icecake.Ingest.Streaming.Auth;

/// <summary>
/// Represents the credentials options required for authentication.
/// Contains user details, private key path for signing, and an optional private key passphrase.
/// </summary>
public class CredentialsOptions
{
    /// <summary>
    /// Gets or sets the Snowflake user identifier required for establishing a connection.
    /// This property defines the username used to authenticate with the Snowflake instance.
    /// </summary>
    public required string User { get; init; }

    /// <summary>
    /// Gets or sets the file path to the private key used for authentication with the Snowflake service.
    /// This property specifies the location of the private key file essential for establishing a secure connection.
    /// </summary>
    public required string PrivateKeyPath { get; init; }

    /// <summary>
    /// Gets or sets the passphrase associated with the private key used for authentication.
    /// This property is used to decrypt the private key specified in the PrivateKeyPath property,
    /// enabling secure access to the Snowflake instance.
    /// </summary>
    public string? PrivateKeyPassphrase { get; set; } = "";
}