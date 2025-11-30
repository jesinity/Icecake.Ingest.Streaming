using System.IO.Compression;

namespace Icecake.Ingest.Streaming.Shims;

/// <summary>
/// Provides utility methods for compressing data using the GZIP compression algorithm.
/// </summary>
internal static class CompressionHelper
{
    public static byte[] Gzip(ReadOnlyMemory<byte> input, CompressionLevel level)
    {
        using var ms = new MemoryStream();

#if NETSTANDARD2_0
        using (var gz = new GZipStream(ms, level, leaveOpen: true))
        {
            // netstandard2.0 has no Write(ReadOnlySpan<byte>)
            var array = input.ToArray();
            gz.Write(array, 0, array.Length);
        }
#else
        using (var gz = new GZipStream(ms, level, leaveOpen: true))
        {
            gz.Write(input.Span); // fast overload available
        }
#endif

        return ms.ToArray();
    }
}
