namespace Icecake.Ingest.Streaming.Core;

public class AccountOptions
{
    /// <summary>
    /// Gets or sets the name of the organization associated with the account.
    /// This property is used in constructing the account identifier and URI details
    /// for the Snowflake instance.
    /// </summary>
    public required string OrganizationName { get; set; }

    /// <summary>
    /// Gets or sets the name of the specific account within the organization.
    /// This property is used to uniquely identify and format the account's URI
    /// for integration with the Snowflake service.
    /// </summary>
    public required string AccountName { get; set; }
    
    public string AccountId  => $"{OrganizationName}-{AccountName}";
    

    /// <summary>
    /// Gets the base URI for the Snowflake instance. The URI is constructed using the
    /// account identifier and follows the format "https://{Account}.snowflakecomputing.com".
    /// </summary>
    public Uri BaseUri => new($"https://{AccountId}.snowflakecomputing.com");
}