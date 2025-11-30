using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Icecake.Ingest.Streaming.Auth;
using Icecake.Ingest.Streaming.Shims;

namespace Icecake.Ingest.Streaming.Core;

/// <summary>
/// The <see cref="PayloadBuilder"/> class is responsible for constructing payloads from rows of data
/// based on a specified table schema. It serializes the data into a format suitable for ingestion
/// into downstream systems, ensuring proper conversion and validation of schema columns.
/// </summary>
public sealed class PayloadBuilder(TableSchema schema, int targetBytes = 1_000_000)
{
    private readonly JsonSerializerOptions _json = JsonHelper.DefaultIgnore;

    /// <summary>
    /// Constructs a blob chunk from the provided data, encoding it
    /// as a JSON payload and applying metadata.
    /// </summary>
    /// <param name="channel">The name of the channel to which the payload belongs.</param>
    /// <param name="rows">The data rows to include in the payload, where each row is represented as a dictionary with string keys and nullable object values.</param>
    /// <param name="offsetToken">An optional offset token representing the position of the data in the stream.</param>
    /// <returns>A <see cref="Payload"/> object representing the constructed chunk, including its payload data and metadata.</returns>
    /// <exception cref="KeyNotFoundException">Thrown if a required key is missing when processing rows.</exception>
    public Payload Build(string channel, List<Dictionary<string, object?>> rows, string? offsetToken)
    {
        using var buffer = new MemoryStream(Math.Max(targetBytes, 1024));

        var writer = new StreamWriter(buffer, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), 1024);

        foreach (var r in rows)
        {
            var normalized = new Dictionary<string, object?>();
            foreach (var kv in r)
            {
                if (!schema.ColumnsByName.TryGetValue(kv.Key, out var spec))
                    throw new KeyNotFoundException($"Column not in schema: {kv.Key}");

                normalized[kv.Key] = ColumnValueConverter.ConvertInternal(spec, kv.Value);
            }

            writer.Write(JsonSerializer.Serialize(normalized, _json));
            writer.Write('\n');
        }

        // Make sure everything is written into the MemoryStream
        writer.Flush();

        var bytes = buffer.ToArray();
        var md5 = ComputeMd5Base64(bytes);
        var chunkId = $"{channel}-{DateTimeOffset.UtcNow:yyyyMMddTHHmmssfff}-{Guid.NewGuid():N}";

        var meta = new ChunkMetadata
        {
            ChunkId = chunkId,
            RowCount = rows.Count,
            SizeBytes = bytes.LongLength,
            Checksum = md5,
            OffsetToken = offsetToken
        };

        return new Payload
        {
            ChunkId = chunkId,
            Data = bytes,
            Metadata = meta
        };
    }
    
    private static string ComputeMd5Base64(byte[] bytes)
    {
        using var md5 = System.Security.Cryptography.MD5.Create();
        var hash = md5.ComputeHash(bytes);
        return Convert.ToBase64String(hash);
    }
}