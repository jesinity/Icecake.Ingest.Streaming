using System.Data.Common;
using Icecake.Ingest.Streaming.Auth;
using Icecake.Ingest.Streaming.Core;
using Microsoft.Extensions.Logging;
using Snowflake.Data.Client;

namespace Icecake.Ingest.Streaming.Connect;

/// <summary>
/// Provides functionality for executing SQL commands and queries against a Snowflake database using an ADO.NET-based connection.
/// </summary>
public sealed class AdoSnowflakeSqlExecutor : ISnowflakeSqlExecutor, IDisposable
{
    private readonly SnowflakeDbConnection _conn;
    private readonly ILogger<SnowflakeHelper> _logger;

    public AdoSnowflakeSqlExecutor(AccountOptions account,
        CredentialsOptions credentials,
        ConnectOptions connect,
        ILogger<SnowflakeHelper> logger)
    {
        var connectionString = connect.AdoConnectionString(account, credentials);
        _conn = new SnowflakeDbConnection { ConnectionString = connectionString };
        _conn.Open();
        _logger = logger;
    }

    ///<inheritdoc cref="ISnowflakeSqlExecutor"/>
    public async Task<int> ExecuteCommandAsync(string sql, CancellationToken ct = default)
    {
        _logger.LogInformation("Executing command {sql}",sql);
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    ///<inheritdoc cref="ISnowflakeSqlExecutor"/>
    public async Task<IReadOnlyList<Dictionary<string, object?>>> QueryAsync(string sql, CancellationToken ct = default)
    {
        _logger.LogInformation("Executing query {sql}",sql);
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        using var rdr = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        var rows = new List<Dictionary<string, object?>>();
        var schema = rdr.GetColumnSchema();
        while (await rdr.ReadAsync(ct).ConfigureAwait(false))
        {
            var row = new Dictionary<string, object?>(schema.Count, StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < schema.Count; i++)
                row[schema[i].ColumnName] = rdr.IsDBNull(i) ? null : rdr.GetValue(i);
            rows.Add(row);
        }
        return rows;
    }

    public void Dispose() => _conn.Dispose();
}