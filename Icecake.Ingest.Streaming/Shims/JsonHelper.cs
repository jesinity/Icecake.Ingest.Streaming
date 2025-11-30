using System.Text.Json;
using System.Text.Json.Serialization;

namespace Icecake.Ingest.Streaming.Shims;

/// <summary>
/// Provides utility methods and pre-configured options for JSON serialization using the System.Text.Json library.
/// </summary>
internal static class JsonHelper
{
    public static JsonSerializerOptions DefaultCamelCase { get; } =
#if NETSTANDARD2_0
        new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            IgnoreNullValues = true
        };
#else
        // modern .NET: use the proper recommended API
        new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };
#endif
    
    public static JsonSerializerOptions DefaultIgnore { get; } =
#if NETSTANDARD2_0
        new JsonSerializerOptions
        {
            IgnoreNullValues = true
        };
#else
        // modern .NET: use the proper recommended API
        new JsonSerializerOptions
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };
#endif
}