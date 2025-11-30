using Icecake.Ingest.Streaming.Auth;
using Icecake.Ingest.Streaming.Core;

namespace Icecake.Ingest.Streaming.Connect;

/// <summary>
/// Represents the options required to establish a connection for interacting
/// with Snowflake's data services via an ADO.NET connection.
/// </summary>
public class ConnectOptions
{
    /// <summary>
    /// Gets or sets the name of the Snowflake database to connect to.
    /// This property is mandatory and specifies the target database
    /// used for querying and data operations within the Snowflake instance.
    /// </summary>
    public required string Database { get; init; }
    public string Schema { get; set; } = "PUBLIC";

    /// <summary>
    /// Gets or sets the role to use when connecting to the Snowflake instance.
    /// That's required by the management plane to perform admin operations, such as
    /// creating missing resources.
    /// </summary>
    public required string Role { get; set; }

    /// <summary>
    /// Gets or sets the Snowflake warehouse name.
    /// This is used by the management plane, not by the Snowpipe ingestion layer itself that is
    /// serverless.
    /// </summary>
    public required string Warehouse { get; init; }

    /// <summary>
    /// Gets or sets the authentication mechanism used to connect to the Snowflake instance.
    /// This property specifies the method of authentication, with the default value
    /// being "snowflake_jwt". It determines how credentials are verified during the
    /// connection process.
    /// </summary>
    public string Authenticator { get; set; } = "snowflake_jwt";

    /// <summary>
    /// Gets or sets the name of the application to be included in the connection string.
    /// This property is typically used to identify the application connecting to
    /// the Snowflake instance and can assist in logging and performance monitoring.
    /// </summary>
    public string Application { get; set; } = "Icecake.Ingest.Streaming";

    public string AdoConnectionString(AccountOptions account, CredentialsOptions credentials) => string.Join(";",
        $"account={account.AccountId}",
        $"user={credentials.User}",
        $"db={Database}",
        $"schema={Schema}",
        $"role={Role}",
        $"warehouse={Warehouse}",
        $"authenticator={Authenticator}",
        $"private_key_file={credentials.PrivateKeyPath}",
        (credentials.PrivateKeyPassphrase is not null ? $"private_key_passphrase={credentials.PrivateKeyPassphrase}" : ""),
        $"application={Application}"
    );
}