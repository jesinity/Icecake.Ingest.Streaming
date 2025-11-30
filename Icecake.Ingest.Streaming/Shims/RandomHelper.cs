namespace Icecake.Ingest.Streaming.Shims;

/// <summary>
/// Provides utility methods for generating random values.
/// This is a static helper class used to handle random number generation.
/// </summary>
internal static class RandomHelper
{
#if NETSTANDARD2_0
    // .NET Standard 2.0 has no Random.Shared, so we create one static RNG.
    // Random is thread-safe for Next* operations as long as it's not reseeded.
    private static readonly Random Rng = new Random();

    public static double NextDouble() => Rng.NextDouble();

    public static int Next(int maxValue) => Rng.Next(maxValue);

    public static int Next(int minValue, int maxValue) => Rng.Next(minValue, maxValue);

    public static void NextBytes(byte[] buffer) => Rng.NextBytes(buffer);

#else
    // .NET 6+ (including .NET 8) has Random.Shared
    public static double NextDouble() => Random.Shared.NextDouble();

    public static int Next(int maxValue) => Random.Shared.Next(maxValue);

    public static int Next(int minValue, int maxValue) => Random.Shared.Next(minValue, maxValue);

    public static void NextBytes(Span<byte> buffer) => Random.Shared.NextBytes(buffer);

    public static void NextBytes(byte[] buffer) => Random.Shared.NextBytes(buffer);
#endif
}