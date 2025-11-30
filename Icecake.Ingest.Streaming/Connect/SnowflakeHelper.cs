using Icecake.Ingest.Streaming.Core;
using Microsoft.Extensions.Logging;

namespace Icecake.Ingest.Streaming.Connect;

/// <summary>
/// Provides helper methods for managing Snowflake object.
/// </summary>
public class SnowflakeHelper(ISnowflakeSqlExecutor sql, ILogger<SnowflakeHelper> logger)
{
    /// <summary>
    /// Ensures that a streaming pipe exists for the specified table in a Snowflake database.
    /// If the streaming pipe does not exist, it will create a new one based on the table's schema and structure.
    /// </summary>
    /// <param name="schemaObjectDef">A <see cref="SchemaObjectCoords"/> object representing the database, schema, and table details.</param>
    /// <param name="pipe">The name of the pipe to ensure or create.</param>
    /// <param name="ct">An optional <see cref="CancellationToken"/> that can be used to cancel the operation.</param>
    /// <returns>A <see cref="Task"/> that represents the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the specified table does not exist in the database schema.</exception>
    public async Task EnsureStreamingPipeForTableAsync(
        SchemaObjectCoords schemaObjectDef,
        string pipe,
        CancellationToken ct = default)
    {
        var database = schemaObjectDef.Database;
        var schema = schemaObjectDef.Schema;
        var table = schemaObjectDef.Name;
        
        // 1) Does pipe exist?
        var pipeLike = EscapeSql(pipe);
        var check = await sql.QueryAsync($@"
            SHOW PIPES LIKE '{pipeLike}' IN SCHEMA {Id(database)}.{Id(schema)};", ct);

        if (check.Count > 0) return; // already exists

        // 2) Read table columns (ordered)
        var cols = await sql.QueryAsync($@"
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {Id(database)}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{EscapeSql(schema)}'
              AND TABLE_NAME   = '{EscapeSql(table)}'
            ORDER BY ORDINAL_POSITION;", ct);

        if (cols.Count == 0)
            throw new InvalidOperationException($"Table {database}.{schema}.{table} not found.");

        // 3) Build SELECT list: $1:"COL"::<TYPE> AS COL
        var selectList = string.Join(",\n    ",
            cols.Select(c =>
            {
                var col = (string)c["COLUMN_NAME"]!;
                var dataType = ((string)c["DATA_TYPE"]!).ToUpperInvariant();
                var cast = CastFor(dataType);
                return $"""$1:"{col}"::{cast} AS {Id(col)}""";
            }));

        // 4) CREATE PIPE IF NOT EXISTS … (STREAMING)
        var sqlText = $@"
CREATE PIPE IF NOT EXISTS {Id(database)}.{Id(schema)}.{Id(pipe)} AS
COPY INTO {Id(database)}.{Id(schema)}.{Id(table)}
FROM (
  SELECT
    {selectList}
  FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);";
        logger.LogInformation("create pipe if not exists {database}.{schema}.{pipe}", database, schema, pipe);

        await sql.ExecuteCommandAsync(sqlText, ct).ConfigureAwait(false);
    }

    private static string Id(string ident) => $"\"{ident.Replace("\"", "\"\"")}\"";
    private static string EscapeSql(string s) => s.Replace("'", "''");

    private static string CastFor(string dataTypeUpper)
        => dataTypeUpper switch
        {
            // numeric family
            var t when t.StartsWith("NUMBER") || t.StartsWith("DECIMAL") || t.StartsWith("NUMERIC") => "NUMBER",
            "INTEGER" or "INT" or "BIGINT" or "SMALLINT" or "TINYINT" => "NUMBER",
            "FLOAT" or "DOUBLE" or "DOUBLE PRECISION" or "REAL" => "FLOAT",

            // strings & binary
            var t when t.StartsWith("VARCHAR") || t.StartsWith("STRING") || t.StartsWith("TEXT") => "STRING",
            var t when t.StartsWith("BINARY") => "BINARY",

            // booleans
            "BOOLEAN" or "BOOL" => "BOOLEAN",

            // semi-structured
            "VARIANT" => "VARIANT",

            // dates/times
            "DATE" => "DATE",
            _ when dataTypeUpper.StartsWith("TIME") => "TIME",
            _ when dataTypeUpper.StartsWith("TIMESTAMP_TZ") => "TIMESTAMP_TZ",
            _ when dataTypeUpper.StartsWith("TIMESTAMP_LTZ") => "TIMESTAMP_LTZ",
            _ when dataTypeUpper.StartsWith("TIMESTAMP_NTZ") => "TIMESTAMP_NTZ",

            _ => "STRING" // safe fallback
        };
}