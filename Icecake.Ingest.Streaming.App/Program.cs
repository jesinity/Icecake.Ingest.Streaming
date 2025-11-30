using Icecake.Ingest.Streaming;
using Icecake.Ingest.Streaming.Connect;
using Icecake.Ingest.Streaming.Core;
using Icecake.Ingest.Streaming.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration
    .AddJsonFile("appsettings.jsonc", optional: false, reloadOnChange: true)
    .AddEnvironmentVariables()
    .AddUserSecrets<Program>();

builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Debug);

// Register the snowflake streaming client.
builder.Services.ConfigureRegisterSnowpipeIngestClient(builder.Configuration);

builder.Services.AddSingleton<ISnowflakeSqlExecutor, AdoSnowflakeSqlExecutor>();
builder.Services.AddSingleton<SnowflakeHelper>();

using var host = builder.Build();
await host.StartAsync();

using (var scope = host.Services.CreateScope())
{
    var sp = scope.ServiceProvider;

    var client = sp.GetRequiredService<SnowpipeIngestClient>();

    var logger = sp.GetRequiredService<ILogger<Program>>();
    var chLogger = sp.GetRequiredService<ILogger<SnowpipeIngestChannel>>();

    var table = new SchemaObjectCoords
    {
        Database = "DEMO_PIPELINE_SNOWPARK",
        Schema = "PUBLIC",
        Name = "ORDERS",
    };

    var schema = new TableSchema
    {
        SchemaObject = table,
        ColumnsByName = new Dictionary<string, ColumnSpec>
        {
            ["ID"] = new()    { Name = "ID",    Type = SnowflakeType.NUMBER },
            ["VALUE"] = new() { Name = "VALUE", Type = SnowflakeType.VARCHAR }
        }
    };
    
    var random = new Random();
    
    logger.LogInformation("Creating pipe for table {Table}", table);

    var pipeName = $"{table.Name}_PIPE";
    
    var channelName = $"{table.Name}_ch_{Environment.MachineName}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";

    var helper = sp.GetRequiredService<SnowflakeHelper>();

    await helper.EnsureStreamingPipeForTableAsync(
        table, pipeName, CancellationToken.None
    );
    
    var channel = new SnowpipeIngestChannel(
        channelName,
        pipeName,
        schema,
        client,
        new FlushPolicy(),
        chLogger
    );
    
    logger.LogInformation("Created the channel {ChannelName}", channelName);

    await channel.OpenAsync();
    
    logger.LogInformation("Opened the channel {ChannelName}", channelName);

    var rows = new List<Dictionary<string, object?>>();

    
    for (var i = 0; i < 10; i++) 
    {
        var row = new Dictionary<string, object?>
        {
            ["ID"] = random.Next(1, 1_000_001),
            ["VALUE"] = Guid.NewGuid().ToString()[..10] // truncate to 10 chars to make the row smaller
        };
        rows.Add(row);
    }
    
    channel.InsertRows(rows);
    
    // that's not really necessary, the offset token is an extra tool
    // on the client side to track the ingestion.
    var nextOffsetToken = DateTime.UtcNow.Ticks.ToString();
    channel.SetOffsetTokenForNextFlush(nextOffsetToken);
    
    await channel.FlushAsync(offsetToken: nextOffsetToken);

    var committedOffset = await channel.FetchLatestCommittedOffsetAsync(timeOutSeconds:20, pollMilliseconds:250);
        
    logger.LogInformation("Committed offset: {Offset}", committedOffset ?? "<still null.... nevermind!>");

    await channel.DisposeAsync();
    logger.LogInformation("Closed the channel {ChannelName}", channelName);
    
    logger.LogInformation("Open it again (that was never disposed indeed bhy the client!) {ChannelName}", channelName);
    await channel.OpenAsync();
    
    var committedOffsetBackAgain = await channel.FetchLatestCommittedOffsetAsync();
    logger.LogInformation("Committed offset: {Offset}", committedOffsetBackAgain ?? "<still null.... that should not happen!>");
    
    var moreRows = new List<Dictionary<string, object?>>();
    
    for (var i = 0; i < 10; i++) 
    {
        var row = new Dictionary<string, object?>
        {
            ["ID"] = random.Next(1, 1_000_001),
            ["VALUE"] = Guid.NewGuid().ToString()[..10]
        };
        moreRows.Add(row);
    }
    
    channel.InsertRows(moreRows);
    await channel.FlushAsync();
    await channel.DisposeAsync();
    
    // The channel will still exist on the Snowpipe side, if we don't drop it.
    // Actually it could not be dropped for long running apps that want to re-attach to the same
    // channel to resume ingestion process.
    // Here we kill it and bye bye.
    await channel.DropAsync();
    logger.LogInformation("Channel was deleted {ChannelName}", channelName);
}
await host.StopAsync();