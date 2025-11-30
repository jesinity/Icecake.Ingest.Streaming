namespace Icecake.Ingest.Streaming.Core;

/// <summary>
/// Represents the schema definition of a table. This includes the table specification and a collection of column specifications.
/// </summary>
public sealed class TableSchema
{
    /// <summary>
    /// Represents the table specification associated with the table schema.
    /// Provides details such as the database, schema, and name of the table.
    /// </summary>
    public required SchemaObjectCoords SchemaObject { get; init; }

    /// <summary>
    /// A dictionary containing column specifications mapped by their names.
    /// This property defines the schema of a table by associating column names
    /// with their respective configurations and attributes.
    /// </summary>
    public required Dictionary<string, ColumnSpec> ColumnsByName { get; init; }
}