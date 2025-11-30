namespace Icecake.Ingest.Streaming.Core;

/// <summary>
/// Represents the specification of a database schema object, may be for example either a table, a view or a pipe.
/// </summary>
public class SchemaObjectCoords
{
    /// <summary>
    /// Represents the name of the database associated with the table specification.
    /// </summary>
    public required string Database { get; init; }

    /// <summary>
    /// Represents the schema name within the database associated with the object specification.
    /// </summary>
    public required string Schema { get; init; }

    /// <summary>
    /// Represents the name of the object in the associated database and schema.
    /// </summary>
    public required string Name { get; init; }
}