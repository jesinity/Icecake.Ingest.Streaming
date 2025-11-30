namespace Icecake.Ingest.Streaming.Connect;

/// <summary>
/// Defines methods for executing SQL queries and commands asynchronously
/// against a Snowflake database.
/// </summary>
public interface ISnowflakeSqlExecutor
{
    /// <summary>
    /// Executes a non-query SQL command asynchronously against a Snowflake database.
    /// Non-query commands include operations such as INSERT, UPDATE, DELETE, and DDL commands (e.g., CREATE TABLE).
    /// </summary>
    /// <param name="sql">The SQL command to execute.</param>
    /// <param name="ct">An optional CancellationToken to observe while waiting for the task to complete.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The task result contains the number of rows affected
    /// by the SQL command or -1 for commands where the number of rows affected is not applicable.
    /// </returns>
    Task<int> ExecuteCommandAsync(string sql, CancellationToken ct = default);

    /// <summary>
    /// Executes a SQL query asynchronously against a Snowflake database and returns the query results.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="ct">An optional CancellationToken to observe while waiting for the task to complete.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The task result contains a read-only list of rows,
    /// where each row is represented as a dictionary mapping column names to their corresponding values.
    /// </returns>
    Task<IReadOnlyList<Dictionary<string, object?>>> QueryAsync(string sql, CancellationToken ct = default);
}